// =====================================================================
// WORKER 3 — SINCRONIZAÇÃO DE VITRINE
// Fila: fila.vitrine (ligada à exchange estoque.eventos)
// Função: manter produtos.disponivel coerente com o saldo:
//   - saldoAtual === 0            -> disponivel = false (esgotado)
//   - ENTRADA com saldoAtual > 0  -> disponivel = true (voltou)
// =====================================================================
require('dotenv').config();
const supabase = require('../config/database');
const { FILAS, conectar, prepararFila } = require('../config/rabbitmq');

const FILA = FILAS.VITRINE;

async function processar(evento) {
    const { tipo, saldoAtual, produtoId, nomeProduto } = evento;

    let disponivel = null;
    if (saldoAtual === 0) disponivel = false;                          // esgotou
    else if (tipo === 'ENTRADA' && saldoAtual > 0) disponivel = true;  // reabasteceu
    if (disponivel === null) return; // nada a mudar na vitrine

    const { error } = await supabase
        .from('produtos')
        .update({ disponivel })
        .eq('id', produtoId);
    if (error) throw error;

    console.log(JSON.stringify({
        worker: 'vitrine',
        produto: nomeProduto,
        produtoId,
        disponivel,
        msg: disponivel ? 'produto disponível na vitrine' : 'produto marcado como ESGOTADO na vitrine',
    }));
}

(async () => {
    const conexao = await conectar();
    const canal = await conexao.createChannel();
    await prepararFila(canal, FILA);
    canal.prefetch(1);

    console.log(`[vitrine] aguardando eventos em "${FILA}"...`);

    canal.consume(FILA, async (msg) => {
        if (!msg) return;
        try {
            await processar(JSON.parse(msg.content.toString()));
            canal.ack(msg);
        } catch (erro) {
            console.error('[vitrine] erro ao processar:', erro.message);
            canal.nack(msg, false, !msg.fields.redelivered);
        }
    }, { noAck: false });
})();
