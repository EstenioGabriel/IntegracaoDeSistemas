const express = require('express');
const controller = require('../controllers/estoqueControl');

const router = express.Router();

router.get('/', controller.hello);
router.get('/produtos', controller.produtos);
router.post('/produtos', controller.produtoStore);
router.post('/produtos/:id/movimentar', controller.movimentar);

// Consultas de apoio para a demonstração/apresentação
router.get('/movimentacoes', controller.movimentacoes);
router.get('/alertas', controller.alertas);

module.exports = router;
