const amqp = require('amqplib');

const EXCHANGE = 'estoque.eventos';

const FILAS = {
    AUDITORIA: 'fila.auditoria',
    COMPRAS: 'fila.compras',
    VITRINE: 'fila.vitrine',
};

async function conectar(url = process.env.RABBITMQ_URL){
    const conexao = await amqp.connect(url);
    conexao.on('error', (err) => console.error('[rabbitmq] conexao perdida:', err.message));
    return conexao;
}

async function prepararFila(canal, nomeFila) {
    await canal.assertExchange(EXCHANGE, 'fanout', { durable: true });
    await canal.assertQueue(nomeFila, { durable: true });
    await canal.bindQueue(nomeFila, EXCHANGE, '');
}


async function criarPublicador() {
    const conexao = await conectar();
    const canal = await conexao.createConfirmChannel();
    await canal.assertExchange(EXCHANGE, 'fanout', { durable: true });

    return {
        async publicar(evento) {
            canal.publish(EXCHANGE, '', Buffer.from(JSON.stringify(evento)), {
                persistent: true,
                contentType: 'application/json',
                messageId: evento.eventoId,
            });
            await canal.waitForConfirms(); 
        },
    };
}

module.exports = { EXCHANGE, FILAS, conectar, prepararFila, criarPublicador };