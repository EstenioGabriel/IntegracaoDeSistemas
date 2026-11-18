const crypto = require('crypto');
const supabase = require('../config/database');
const { criarPublicador } = require('../config/rabbitmq');

// =====================================================================
// Publicador: conexão com o RabbitMQ criada UMA vez (index.js guarda em
// global.publicador na subida). Se um publish chegar antes, conecta aí.
// =====================================================================
async function publicar(evento) {
    if (!global.publicador) {
        global.publicador = await criarPublicador();
    }
    await global.publicador.publicar(evento);
}

const hello = (req, res) => {
    res.send('API de estoque (produtor de eventos)');
};

// ------------------------------ CRUD --------------------------------
const produtos = async (req, res) => {
    try {
        const { data, error } = await supabase.from('produtos').select('*');
        if (error) return res.status(500).json({ erro: error.message });
        res.json(data);
    } catch (error) {
        res.status(500).json({ erro: error.message });
    }
};

const produtoStore = async (req, res) => {
    const { nome, preco, estoque, estoque_minimo } = req.body;

    try {
        const { data, error } = await supabase
            .from('produtos')
            .insert({ nome, preco, estoque, estoque_minimo })
            .select();

        if (error) return res.status(400).json({ erro: error.message });
        return res.status(201).json(data[0]);
    } catch (error) {
        return res.status(500).json({ erro: error.message });
    }
};

// =====================================================================
// ROTA CRÍTICA: movimentação de estoque (produtor do evento)
// Só faz trabalho SÍNCRONO essencial: valida, atualiza o saldo e
// publica o evento. O kardex, os alertas e a vitrine são processados
// por workers independentes — nada aqui espera por eles.
// =====================================================================
const movimentar = async (req, res) => {
    const { id } = req.params;
    const { tipo, quantidade } = req.body;

    // 1) Validações de entrada
    if (!['ENTRADA', 'SAIDA'].includes(tipo)) {
        return res.status(400).json({ erro: "tipo deve ser 'ENTRADA' ou 'SAIDA'" });
    }
    const qtd = Number(quantidade);
    if (!Number.isInteger(qtd) || qtd <= 0) {
        return res.status(400).json({ erro: 'quantidade deve ser um inteiro positivo' });
    }

    // 2) Valida produto e saldo (precisamos do saldo anterior p/ o evento)
    const { data: produto, error: erroLeitura } = await supabase
        .from('produtos').select('*').eq('id', id).single();
    if (erroLeitura || !produto) {
        return res.status(404).json({ erro: 'Produto não encontrado' });
    }

    const saldoAnterior = produto.estoque;
    if (tipo === 'SAIDA' && saldoAnterior < qtd) {
        return res.status(409).json({ erro: 'Saldo insuficiente', saldo: saldoAnterior });
    }

    // 3) Atualiza o saldo.
    //    .gte('estoque', qtd) é uma guarda atômica: se outra venda
    //    concorrente baixou o saldo primeiro, o UPDATE não casa linha nenhuma.
    const saldoAtual = tipo === 'SAIDA' ? saldoAnterior - qtd : saldoAnterior + qtd;
    let consulta = supabase.from('produtos').update({ estoque: saldoAtual }).eq('id', id);
    if (tipo === 'SAIDA') consulta = consulta.gte('estoque', qtd);

    const { data: atualizado, error: erroUpdate } = await consulta.select().single();
    if (erroUpdate || !atualizado) {
        return res.status(409).json({ erro: 'Saldo insuficiente' });
    }

    // 4) Monta o evento no contrato definido e publica
    const evento = {
        eventoId: crypto.randomUUID(),
        produtoId: Number(id),
        nomeProduto: produto.nome,
        tipo,
        quantidadeMovimentada: qtd,
        saldoAnterior,
        saldoAtual,
        estoqueMinimo: produto.estoque_minimo,
        timestamp: new Date().toISOString(),
    };

    try {
        await publicar(evento);
    } catch (erro) {
        // Broker fora do ar: o saldo já foi atualizado, mas avisamos o cliente
        return res.status(503).json({ erro: 'RabbitMQ indisponível: ' + erro.message });
    }

    // 5) Responde imediatamente — os 3 workers processam por conta própria
    return res.status(200).json({ ok: true, saldoAtual, evento });
};

// ----------------- Consultas de apoio (p/ demonstração) -------------
const movimentacoes = async (req, res) => {
    try {
        const { data, error } = await supabase
            .from('movimentacoes_estoque')
            .select('*')
            .order('criado_em', { ascending: false })
            .limit(50);
        if (error) return res.status(500).json({ erro: error.message });
        res.json(data);
    } catch (error) {
        res.status(500).json({ erro: error.message });
    }
};

const alertas = async (req, res) => {
    try {
        const { data, error } = await supabase
            .from('alertas_reposicao')
            .select('*')
            .order('criado_em', { ascending: false })
            .limit(50);
        if (error) return res.status(500).json({ erro: error.message });
        res.json(data);
    } catch (error) {
        res.status(500).json({ erro: error.message });
    }
};

module.exports = { hello, produtos, produtoStore, movimentar, movimentacoes, alertas };
