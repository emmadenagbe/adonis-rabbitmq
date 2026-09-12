# @emmadenagbe/adonis-rabbitmq

[![npm version](https://img.shields.io/npm/v/@emmadenagbe/adonis-rabbitmq.svg)](https://www.npmjs.com/package/@emmadenagbe/adonis-rabbitmq)
[![checks](https://github.com/emmadenagbe/adonis-rabbitmq/actions/workflows/checks.yml/badge.svg)](https://github.com/emmadenagbe/adonis-rabbitmq/actions/workflows/checks.yml)
[![license](https://img.shields.io/npm/l/@emmadenagbe/adonis-rabbitmq.svg)](LICENSE.md)

A RabbitMQ integration for AdonisJS 6. It wraps [amqp-connection-manager](https://github.com/jwalton/node-amqp-connection-manager) so connections reconnect on their own, and plugs into AdonisJS's container so you can publish messages and write consumers the same way you'd write anything else in an Adonis app.

## What you get

- Publish messages and send them straight to a queue
- Write consumers as classes with a `@consumer` decorator, resolved through the container (so `@inject` works inside them)
- Failed messages are retried a few times, then moved to a dead-letter exchange automatically
- Request/reply (RPC) over RabbitMQ, if you need it
- Multiple named connections, if you talk to more than one broker
- Priority queues and header-based routing, for when topic/direct routing isn't enough

## Install

```sh
node ace add @emmadenagbe/adonis-rabbitmq
```

This does three things:

- Creates `config/rabbitmq.ts`
- Adds `RABBITMQ_URL` to `.env` and validates it in `env.ts`
- Registers the provider in `adonisrc.ts`

By default it connects to `amqp://guest:guest@localhost:5672`. Change `RABBITMQ_URL` in `.env` to point at your broker.

## Publishing a message

```ts
import rabbitmq from '@emmadenagbe/adonis-rabbitmq/services/main'

await rabbitmq.publish('orders_exchange', 'order.created', { id: order.id })
```

`publish` sends to an exchange with a routing key. If you don't need an exchange, send straight to a queue instead:

```ts
await rabbitmq.sendToQueue('orders', { id: order.id })
```

Both return a promise that resolves once RabbitMQ has actually confirmed the message, not just when it's been sent.

## Writing a consumer

Consumers are classes. Put them wherever you like (`app/consumers/` is a reasonable default) and decorate them with `@consumer`:

```ts
// app/consumers/orders_consumer.ts
import { consumer, BaseConsumer } from '@emmadenagbe/adonis-rabbitmq'

@consumer({
  queue: 'orders',
  exchange: 'orders_exchange',
  routingKey: 'order.created',
})
export default class OrdersConsumer extends BaseConsumer<{ id: number }> {
  async handle(payload: { id: number }) {
    // do something with the message
  }
}
```

A consumer only starts listening once its file has actually been imported somewhere. The easiest way to make that happen is a preload file:

```ts
// start/rabbitmq.ts
import '#app/consumers/orders_consumer'
// import every other consumer here too
```

```ts
// adonisrc.ts
{
  preloads: ['./start/rabbitmq.js']
}
```

### If handling a message fails

If `handle` throws, the message is retried automatically (3 times by default, after a short delay), and if it keeps failing it's routed to a dead-letter exchange named `<queue>.dlx` so you don't lose it. Change the retry count per consumer:

```ts
@consumer({ queue: 'orders', maxRetries: 5 })
```

## Multiple connections

If you need more than one broker (or more than one vhost), name your connections in the config file:

```ts
// config/rabbitmq.ts
const rabbitmqConfig = defineConfig({
  connection: 'main',
  connections: {
    main: { url: env.get('RABBITMQ_URL') },
    events: { url: env.get('EVENTS_RABBITMQ_URL') },
  },
})
```

Then point a publish call or a consumer at the one you want:

```ts
await rabbitmq.publish('exchange', 'key', payload, { connection: 'events' })

@consumer({ queue: 'audit', connection: 'events' })
export default class AuditConsumer extends BaseConsumer {}
```

Leave it out and everything uses `main`.

## Request/reply (RPC)

Sometimes you want to send a message and wait for an answer, instead of just firing it off. `request` does that:

```ts
const response = await rabbitmq.request('math.double', { n: 21 })
// response is whatever the consumer replied with
```

On the other end, a consumer replies using the raw message it was handed:

```ts
@consumer({ queue: 'math.double' })
export default class DoubleConsumer extends BaseConsumer<{ n: number }> {
  async handle(payload: { n: number }, raw) {
    await rabbitmq.reply(raw, { result: payload.n * 2 })
  }
}
```

If nothing replies, `request` rejects after 30 seconds by default. Change that with `{ timeoutMs: 5000 }`.

## Priority queues

If some messages should jump ahead of others in the same queue, set `maxPriority` on the consumer and a `priority` on the message:

```ts
@consumer({ queue: 'jobs', maxPriority: 10 })
export default class JobsConsumer extends BaseConsumer {}
```

```ts
await rabbitmq.sendToQueue('jobs', payload, { priority: 9 })
```

## Routing by headers instead of a routing key

For cases where a topic/direct routing key doesn't fit, you can bind a queue based on message headers instead:

```ts
@consumer({
  queue: 'eu-orders',
  exchange: 'orders_exchange',
  exchangeType: 'headers',
  headers: { region: 'eu' },
  headersMatch: 'all', // or 'any'
})
export default class EuOrdersConsumer extends BaseConsumer {}
```

```ts
await rabbitmq.publish('orders_exchange', '', payload, { headers: { region: 'eu' } })
```

## Testing locally

A `compose.yml` is included, so you can start a broker with:

```sh
docker compose up -d
```

The management UI is at http://localhost:15672 (user/pass: `guest`/`guest`).

## License

MIT
