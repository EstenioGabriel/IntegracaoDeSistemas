// =====================================================================
// WORKER 2 — ALERTAS DE COMPRAS
// Fila: fila.compras (ligada à exchange estoque.eventos)
// Função: se saldoAtual <= estoqueMinimo, insere alerta em
// alertas_reposicao e dispara (simulado) a notificação ao setor.
// =====================================================================
require('dotenv').config();
const supabase = require('../config/database');
const { FILAS, conectar, prepararFila } = require('../config/rabbitmq');

const FILA = FILAS.COMPRAS;

async function processar(evento) {
    const { saldoAtual, estoqueMinimo, produtoId } = evento;

    // Bônus: ENTRADA que repõe o saldo resolve alertas PENDENTES do produto
    if (evento.tipo === 'ENTRADA' && saldoAtual > estoqueMinimo) {
        await supabase
            .from('alertas_reposicao')
            .update({ status: 'RESOLVIDO' })
            .eq('produto_id', produtoId)
            .eq('status', 'PENDENTE');
    }

    if (saldoAtual > estoqueMinimo) {
        console.log(JSON.stringify({
            worker: 'compras',
            produto: evento.nomeProduto,
            saldoAtual,
            estoqueMinimo,
            msg: 'saldo acima do mínimo — nenhuma ação',
        }));
        return;
    }

    const { error } = await supabase.from('alertas_reposicao').insert({
        produto_id: produtoId,
        saldo_atual: saldoAtual,
        estoque_minimo: estoqueMinimo,
    });
    if (error) throw error;

    // Log estruturado simulando o disparo da notificação ao setor de compras
    console.log(JSON.stringify({
        worker: 'compras',
        nivel: 'ALERTA',
        acao: 'NOTIFICAR_SETOR_COMPRAS',
        produto: evento.nomeProduto,
        produtoId,
        saldoAtual,
        estoqueMinimo,
        evento: evento.eventoId,
        mensagem: `⚠️  Estoque crítico: ${evento.nomeProduto} (saldo ${saldoAtual}, mínimo ${estoqueMinimo})`,
    }));
}

(async () => {
    const conexao = await conectar();
    const canal = await conexao.createChannel();
    await prepararFila(canal, FILA);
    canal.prefetch(1);

    console.log(`[compras] aguardando eventos em "${FILA}"...`);

    canal.consume(FILA, async (msg) => {
        if (!msg) return;
        try {
            await processar(JSON.parse(msg.content.toString()));
            canal.ack(msg);
        } catch (erro) {
            console.error('[compras] erro ao processar:', erro.message);
            canal.nack(msg, false, !msg.fields.redelivered);
        }
    }, { noAck: false });
})();
