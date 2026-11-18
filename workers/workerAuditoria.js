// =====================================================================
// WORKER 1 — AUDITORIA (Kardex)
// Fila: fila.auditoria (ligada à exchange estoque.eventos)
// Função: registrar TODAS as movimentações em movimentacoes_estoque.
// IDEMPOTÊNCIA: evento_id é UNIQUE no Supabase + upsert com
// ignoreDuplicates => se a mensagem for reentregue, nada é duplicado.
// =====================================================================
require('dotenv').config();
const supabase = require('../config/database');
const { FILAS, conectar, prepararFila } = require('../config/rabbitmq');

const FILA = FILAS.AUDITORIA;

async function processar(evento) {
    const { error } = await supabase
        .from('movimentacoes_estoque')
        .upsert({
            evento_id: evento.eventoId,
            produto_id: evento.produtoId,
            tipo: evento.tipo,
            quantidade: evento.quantidadeMovimentada,
            saldo_anterior: evento.saldoAnterior,
            saldo_atual: evento.saldoAtual,
        }, { onConflict: 'evento_id', ignoreDuplicates: true });

    if (error) throw error;

    console.log(JSON.stringify({
        worker: 'auditoria',
        evento: evento.eventoId,
        produto: evento.nomeProduto,
        tipo: evento.tipo,
        quantidade: evento.quantidadeMovimentada,
        saldo: `${evento.saldoAnterior} -> ${evento.saldoAtual}`,
        msg: 'movimentação registrada no kardex',
    }));
}

(async () => {
    const conexao = await conectar();
    const canal = await conexao.createChannel();
    await prepararFila(canal, FILA);
    canal.prefetch(1); // processa uma mensagem por vez

    console.log(`[auditoria] aguardando eventos em "${FILA}"...`);

    canal.consume(FILA, async (msg) => {
        if (!msg) return;
        try {
            await processar(JSON.parse(msg.content.toString()));
            canal.ack(msg); // ack SOMENTE após o Supabase concluir com sucesso
        } catch (erro) {
            console.error('[auditoria] erro ao processar:', erro.message);
            // 1ª tentativa: reenfileira | já reentregue: descarta (evita loop infinito)
            canal.nack(msg, false, !msg.fields.redelivered);
        }
    }, { noAck: false }); // confirmação MANUAL (exigência da atividade)
})();
