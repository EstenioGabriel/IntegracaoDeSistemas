const express = require('express');
require('dotenv').config();
const rotasEstoque = require('./rotas/rotasEstoque');
const { criarPublicador } = require('./config/rabbitmq');

const porta = Number(process.env.PORTA) || 3000;

async function iniciar() {
    const app = express();
    app.use(express.json());
    app.use(rotasEstoque);

    try {
        // Falha rápido com mensagem clara se o broker não estiver de pé
        global.publicador = await criarPublicador();
        console.log('[api] conectado ao RabbitMQ — exchange estoque.eventos pronta');
    } catch (erro) {
        console.error('[api] RabbitMQ indisponível. Rode "docker compose up -d" primeiro.');
        console.error('[api] detalhe:', erro.message);
        process.exit(1);
    }

    app.listen(porta, () => {
        console.log(`[api] produtor rodando em http://localhost:${porta}`);
    });
}

iniciar();
