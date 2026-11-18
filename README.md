# Estoque orientado a eventos (Node.js + Express + Supabase + RabbitMQ)

Evolução da API de produtos (CRUD) para um modelo **orientado a eventos (Pub/Sub)**:
a rota de venda/movimentação atualiza o saldo, **publica um evento** e responde
imediatamente — sem esperar os processos secundários (auditoria, alertas de
compras e vitrine), que rodam em **workers independentes** via RabbitMQ.

## Arquitetura

```
                        ┌──────────────────────────┐
   API Express          │  Exchange (fanout)       │
   (Produtor)           │    estoque.eventos       │
        │               └────┬─────────┬─────────┬─┘
   valida saldo,            │ cópia   │ cópia   │ cópia
   atualiza saldo e    ┌────▼─────┐ ┌──▼──────┐ ┌▼──────────┐
   publica 1 evento    │fila.     │ │fila.    │ │fila.      │
                       │auditoria │ │compras  │ │vitrine    │
                       └────┬─────┘ └──┬──────┘ └┬──────────┘
                        Worker 1     Worker 2     Worker 3
                        (Kardex)     (Compras)    (Vitrine)
                            │            │            │
                    movimentacoes_   alertas_     produtos.
                    estoque          reposicao    disponivel
```

Por que FANOUT: um único evento de movimentação interessa aos 3 setores —
a exchange entrega **uma cópia independente** para cada fila ligada a ela.
Se um worker cai, os outros continuam e as mensagens dele ficam **retidas
na própria fila** até ele voltar.

## Estrutura

```
├── index.js                    # API Express (produtor)
├── controllers/estoqueControl.js   # CRUD + POST /produtos/:id/movimentar
├── rotas/rotasEstoque.js
├── config/database.js          # cliente Supabase
├── config/rabbitmq.js          # topologia (exchange/filas) + publicador
├── workers/
│   ├── workerAuditoria.js      # worker 1 — kardex (idempotente)
│   ├── workerCompras.js        # worker 2 — alertas de reposição
│   └── workerVitrine.js        # worker 3 — disponível/esgotado
├── sql/schema.sql              # DDL completo (Supabase)
├── docker-compose.yml          # RabbitMQ + painel de gerenciamento
└── .env.example
```

## Como rodar

### 1. Pré-requisitos
- Node.js 18+
- Docker (para o RabbitMQ)
- Projeto no Supabase com as tabelas criadas (ver abaixo)

### 2. Configuração

```bash
npm install
cp .env.example .env   # e preencha SUPABASE_URL / SUPABASE_KEY
```

Execute o DDL `sql/schema.sql` no **SQL Editor** do Supabase
(cria `movimentacoes_estoque` e `alertas_reposicao` e adiciona as colunas
`estoque_minimo` e `disponivel` em `produtos`).

### 3. Subir o RabbitMQ

```bash
docker compose up -d
```

Painel de gerenciamento: **http://localhost:15672** (usuário `admin`, senha `admin`).

### 4. Iniciar API e workers (terminais separados)

```bash
npm start                  # API (produtor) na porta 3000
npm run worker:auditoria   # worker 1 — kardex
npm run worker:compras     # worker 2 — alertas de reposição
npm run worker:vitrine     # worker 3 — vitrine
```

## Contrato do evento (Exchange `estoque.eventos`)

```json
{
  "eventoId": "uuid-v4-gerado-na-api",
  "produtoId": 1,
  "nomeProduto": "Teclado Mecânico RGB",
  "tipo": "SAIDA",
  "quantidadeMovimentada": 2,
  "saldoAnterior": 6,
  "saldoAtual": 4,
  "estoqueMinimo": 5,
  "timestamp": "2026-11-16T22:00:00.000Z"
}
```

## Endpoints

| Método | Rota | Descrição |
|---|---|---|
| GET | `/produtos` | lista produtos |
| POST | `/produtos` | cria produto (`nome`, `preco`, `estoque`, `estoque_minimo`) |
| POST | `/produtos/:id/movimentar` | movimenta estoque (`tipo`: ENTRADA/SAIDA, `quantidade`) |
| GET | `/movimentacoes` | kardex (auditoria) |
| GET | `/alertas` | alertas de reposição |

### Exemplos

```bash
# criar produto
curl -X POST http://localhost:3000/produtos \
  -H 'Content-Type: application/json' \
  -d '{"nome":"Teclado Mecânico RGB","preco":250,"estoque":8,"estoque_minimo":5}'

# venda (8 -> 6)
curl -X POST http://localhost:3000/produtos/1/movimentar \
  -H 'Content-Type: application/json' -d '{"tipo":"SAIDA","quantidade":2}'

# entrada (reabastece)
curl -X POST http://localhost:3000/produtos/1/movimentar \
  -H 'Content-Type: application/json' -d '{"tipo":"ENTRADA","quantidade":10}'
```

## Decisões de resiliência

| Requisito | Implementação |
|---|---|
| Exchange e filas sobrevivem a restart | `durable: true` (exchange + filas) |
| Mensagens sobrevivem a restart | `persistent: true` no publish |
| Nada é perdido se o worker cair | `noAck: false`; `channel.ack(msg)` só **após** o Supabase concluir; sem ack, o broker **reentrega** |
| Reentrega não duplica registro | Idempotência: `evento_id UNIQUE` + upsert `ignoreDuplicates` (worker de auditoria) |
| API nunca fica presa nos secundários | Publish com **publisher confirms** (ms) e resposta 200 imediata |
| Erro no worker não trava a fila | 1ª falha reenfileira (`nack requeue`); falha em reentrega descarta com log |
| Venda concorrente não estoura o estoque | `UPDATE` com guarda `.gte('estoque', quantidade)` |
