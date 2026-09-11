import { test } from '@japa/runner'
import amqplib from 'amqplib'

import RabbitMQManager from '../src/rabbitmq_manager.js'
import { consumer } from '../src/consumer.js'
import { BaseConsumer } from '../src/types.js'
import type { ConsumerResolver } from '../src/rabbitmq_manager.js'

const RABBITMQ_URL = 'amqp://guest:guest@localhost:5672'

const fakeLogger = {
  error: () => {},
} as any

function uniqueName(prefix: string) {
  return `${prefix}.${process.pid}.${Date.now()}.${Math.random().toString(36).slice(2)}`
}

function makeManager() {
  return new RabbitMQManager(
    { connection: 'main', connections: { main: { url: RABBITMQ_URL } } },
    fakeLogger
  )
}

/**
 * Polls until `check` returns true or the timeout elapses.
 */
async function waitUntil(check: () => boolean, timeoutMs = 5000) {
  const start = Date.now()
  while (!check()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error('waitUntil timed out')
    }
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
}

test.group('RabbitMQManager | publish + sendToQueue', (group) => {
  let manager: RabbitMQManager<any>

  group.each.teardown(async () => {
    await manager.closeAll()
  })

  test('sendToQueue delivers a JSON payload to the queue', async ({ assert }) => {
    manager = makeManager()
    const queue = uniqueName('test.send')

    const setupConn = await amqplib.connect(RABBITMQ_URL)
    const setupChannel = await setupConn.createChannel()
    await setupChannel.assertQueue(queue, { durable: false, autoDelete: true })

    await manager.sendToQueue(queue, { hello: 'world' })

    const received: any[] = []
    await setupChannel.consume(queue, (message) => {
      if (!message) return
      received.push(JSON.parse(message.content.toString()))
      setupChannel.ack(message)
    })

    await waitUntil(() => received.length > 0)
    assert.deepEqual(received[0], { hello: 'world' })

    await setupChannel.close()
    await setupConn.close()
  }).timeout(10000)

  test('publish delivers a JSON payload via an exchange + routing key', async ({ assert }) => {
    manager = makeManager()
    const exchange = uniqueName('test.exchange')
    const queue = uniqueName('test.publish.queue')
    const routingKey = 'my.key'

    const setupConn = await amqplib.connect(RABBITMQ_URL)
    const setupChannel = await setupConn.createChannel()
    await setupChannel.assertExchange(exchange, 'topic', { durable: false, autoDelete: true })
    await setupChannel.assertQueue(queue, { durable: false, autoDelete: true })
    await setupChannel.bindQueue(queue, exchange, routingKey)

    await manager.publish(exchange, routingKey, { id: 42 })

    const received: any[] = []
    await setupChannel.consume(queue, (message) => {
      if (!message) return
      received.push(JSON.parse(message.content.toString()))
      setupChannel.ack(message)
    })

    await waitUntil(() => received.length > 0)
    assert.deepEqual(received[0], { id: 42 })

    await setupChannel.close()
    await setupConn.close()
  }).timeout(10000)
})

test.group('RabbitMQManager | consumers', (group) => {
  let manager: RabbitMQManager<any>

  group.each.teardown(async () => {
    await manager.closeAll()
  })

  test('a successful consumer acks the message and receives the decoded payload', async ({
    assert,
  }) => {
    manager = makeManager()
    const queue = uniqueName('test.consumer.ok')
    const received: any[] = []

    @consumer({ queue })
    class OkConsumer extends BaseConsumer<{ id: number }> {
      async handle(payload: { id: number }) {
        received.push(payload)
      }
    }

    const resolve: ConsumerResolver = async (ctor) => new (ctor as any)()
    await manager.startConsumers(resolve)
    await manager.sendToQueue(queue, { id: 1 })

    await waitUntil(() => received.length > 0)
    assert.deepEqual(received[0], { id: 1 })
  }).timeout(10000)

  test('a failing consumer retries then routes the message to the dead-letter exchange', async ({
    assert,
  }) => {
    manager = makeManager()
    const queue = uniqueName('test.consumer.fail')
    const deadLetterExchange = `${queue}.dlx`
    let attempts = 0

    @consumer({ queue, maxRetries: 2 })
    class FailingConsumer extends BaseConsumer {
      async handle() {
        attempts++
        throw new Error('boom')
      }
    }

    const resolve: ConsumerResolver = async (ctor) => new (ctor as any)()
    await manager.startConsumers(resolve)

    // Bind our own queue to the dead-letter exchange to observe the outcome.
    const setupConn = await amqplib.connect(RABBITMQ_URL)
    const setupChannel = await setupConn.createChannel()
    await setupChannel.assertExchange(deadLetterExchange, 'fanout', { durable: true })
    const dlq = uniqueName('test.consumer.fail.observer')
    await setupChannel.assertQueue(dlq, { durable: false, autoDelete: true })
    await setupChannel.bindQueue(dlq, deadLetterExchange, '')

    const deadLettered: any[] = []
    await setupChannel.consume(dlq, (message) => {
      if (!message) return
      deadLettered.push(JSON.parse(message.content.toString()))
      setupChannel.ack(message)
    })

    await manager.sendToQueue(queue, { id: 99 })

    await waitUntil(() => deadLettered.length > 0, 15000)
    assert.deepEqual(deadLettered[0], { id: 99 })
    assert.isAbove(attempts, 2)

    await setupChannel.close()
    await setupConn.close()
  }).timeout(20000)
})

test.group('RabbitMQManager | RPC (request/reply)', (group) => {
  let manager: RabbitMQManager<any>

  group.each.teardown(async () => {
    await manager.closeAll()
  })

  test('request resolves with the payload sent back via reply', async ({ assert }) => {
    manager = makeManager()
    const queue = uniqueName('test.rpc.echo')

    @consumer({ queue })
    class EchoConsumer extends BaseConsumer<{ n: number }> {
      async handle(payload: { n: number }, raw: any) {
        await manager.reply(raw, { doubled: payload.n * 2 })
      }
    }

    const resolve: ConsumerResolver = async (ctor) => new (ctor as any)()
    await manager.startConsumers(resolve)

    const response = await manager.request<{ doubled: number }>(queue, { n: 21 })
    assert.deepEqual(response, { doubled: 42 })
  }).timeout(10000)

  test('request rejects when nothing replies before the timeout', async ({ assert }) => {
    manager = makeManager()
    const queue = uniqueName('test.rpc.silent')

    // A consumer that never calls reply().
    @consumer({ queue })
    class SilentConsumer extends BaseConsumer {
      async handle() {}
    }

    const resolve: ConsumerResolver = async (ctor) => new (ctor as any)()
    await manager.startConsumers(resolve)

    await assert.rejects(() => manager.request(queue, {}, { timeoutMs: 300 }))
  }).timeout(10000)
})

test.group('RabbitMQManager | headers exchange binding', (group) => {
  let manager: RabbitMQManager<any>

  group.each.teardown(async () => {
    await manager.closeAll()
  })

  test('a queue bound with matching headers receives the message', async ({ assert }) => {
    manager = makeManager()
    const exchange = uniqueName('test.headers.exchange')
    const queue = uniqueName('test.headers.queue')
    const received: any[] = []

    @consumer({
      queue,
      exchange,
      exchangeType: 'headers',
      headers: { region: 'eu' },
      headersMatch: 'all',
    })
    class RegionConsumer extends BaseConsumer<{ id: number }> {
      async handle(payload: { id: number }) {
        received.push(payload)
      }
    }

    const resolve: ConsumerResolver = async (ctor) => new (ctor as any)()
    await manager.startConsumers(resolve)

    // Non-matching headers: should NOT be delivered.
    await manager.publish(exchange, '', { id: 1 }, { headers: { region: 'us' } })
    // Matching headers: should be delivered.
    await manager.publish(exchange, '', { id: 2 }, { headers: { region: 'eu' } })

    await waitUntil(() => received.length > 0)
    // Give the non-matching publish a chance to (wrongly) arrive too.
    await new Promise((r) => setTimeout(r, 300))

    assert.deepEqual(received, [{ id: 2 }])
  }).timeout(10000)
})

test.group('RabbitMQManager | priority queues', (group) => {
  let manager: RabbitMQManager<any>

  group.each.teardown(async () => {
    await manager.closeAll()
  })

  test('higher priority messages are delivered before lower priority ones', async ({ assert }) => {
    manager = makeManager()
    const queue = uniqueName('test.priority')
    const order: number[] = []
    let resolveDone: () => void
    const done = new Promise<void>((resolve) => {
      resolveDone = resolve
    })

    @consumer({ queue, maxPriority: 10 })
    class PriorityConsumer extends BaseConsumer<{ priority: number }> {
      async handle(payload: { priority: number }) {
        order.push(payload.priority)
        if (order.length === 3) resolveDone()
      }
    }

    // Publish low-to-high while nothing is consuming yet, so the
    // broker has all three queued up before ordering by priority.
    const setupConn = await amqplib.connect(RABBITMQ_URL)
    const setupChannel = await setupConn.createChannel()
    await setupChannel.assertQueue(queue, {
      durable: true,
      arguments: { 'x-max-priority': 10 },
    })
    await setupChannel.sendToQueue(queue, Buffer.from(JSON.stringify({ priority: 1 })), {
      priority: 1,
    })
    await setupChannel.sendToQueue(queue, Buffer.from(JSON.stringify({ priority: 5 })), {
      priority: 5,
    })
    await setupChannel.sendToQueue(queue, Buffer.from(JSON.stringify({ priority: 9 })), {
      priority: 9,
    })
    await setupChannel.close()
    await setupConn.close()

    const resolve: ConsumerResolver = async (ctor) => new (ctor as any)()
    await manager.startConsumers(resolve)

    await done
    assert.deepEqual(order, [9, 5, 1])
  }).timeout(10000)
})
