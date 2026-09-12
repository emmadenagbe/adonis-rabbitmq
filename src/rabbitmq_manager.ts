/*
 * @emmadenagbe/adonis-rabbitmq
 *
 * RabbitMQ provider for AdonisJS 6
 */

import type { Logger } from '@adonisjs/core/logger'
import type { ConfirmChannel, ConsumeMessage } from 'amqplib'

import debug from './debug.js'
import RabbitMQConnection from './connection.js'
import { consumersRegistry } from './consumer.js'
import { E_RABBITMQ_CONNECTION_NOT_FOUND } from './errors.js'
import type {
  BaseConsumer,
  ConsumerConstructor,
  ConsumerOptions,
  PublishOptions,
  RabbitMQConnectionsList,
} from './types.js'

/**
 * A consumer class resolver, backed by the IoC container. Kept as an
 * injected function (rather than importing the container directly)
 * so the manager stays testable in isolation.
 */
export type ConsumerResolver = (ctor: ConsumerConstructor) => Promise<BaseConsumer>

export default class RabbitMQManager<ConnectionsList extends RabbitMQConnectionsList> {
  #logger: Logger
  #activeConnections: Map<string, RabbitMQConnection> = new Map()

  constructor(
    public managerConfig: { connection: keyof ConnectionsList; connections: ConnectionsList },
    logger: Logger
  ) {
    this.#logger = logger
  }

  /**
   * Returns (creating if needed) the named connection, or the
   * default one when no name is given.
   */
  connection(connectionName?: keyof ConnectionsList): RabbitMQConnection {
    const name = (connectionName ?? this.managerConfig.connection) as string
    const existing = this.#activeConnections.get(name)
    if (existing) {
      return existing
    }

    const config = this.managerConfig.connections[name]
    if (!config) {
      throw new E_RABBITMQ_CONNECTION_NOT_FOUND([name])
    }

    const connection = new RabbitMQConnection(name, config)
    this.#activeConnections.set(name, connection)
    return connection
  }

  /**
   * Publish a JSON payload to an exchange using a routing key. The
   * publish channel uses confirms by default, so the returned promise
   * only resolves once the broker has acknowledged the message.
   * `options.priority` requires the target queue to have been
   * declared with a `maxPriority`.
   */
  publish(
    exchange: string,
    routingKey: string,
    payload: unknown,
    options?: PublishOptions<ConnectionsList>
  ) {
    const { connection, ...publishOptions } = options ?? {}
    return this.connection(connection).publish(exchange, routingKey, payload, publishOptions)
  }

  /**
   * Send a JSON payload straight to a queue. See `publish` for notes
   * on confirms and priority.
   */
  sendToQueue(queue: string, payload: unknown, options?: PublishOptions<ConnectionsList>) {
    const { connection, ...publishOptions } = options ?? {}
    return this.connection(connection).sendToQueue(queue, payload, publishOptions)
  }

  /**
   * Sends a JSON payload to a queue and waits for a reply, correlated
   * automatically. The consumer on the other end must call `reply()`
   * with the same message it received. Rejects if no reply arrives
   * within `timeoutMs` (defaults to 30s).
   */
  request<Response = unknown>(
    queue: string,
    payload: unknown,
    options?: { connection?: keyof ConnectionsList; timeoutMs?: number }
  ): Promise<Response> {
    return this.connection(options?.connection).request<Response>(queue, payload, {
      timeoutMs: options?.timeoutMs,
    })
  }

  /**
   * Replies to an RPC request received by a consumer. Pass the raw
   * `ConsumeMessage` a consumer's `handle(payload, raw)` received.
   */
  reply(raw: ConsumeMessage, payload: unknown, options?: { connection?: keyof ConnectionsList }) {
    return this.connection(options?.connection).reply(raw, payload)
  }

  /**
   * Starts every consumer registered via the `@consumer` decorator.
   * Each consumer gets its own channel: its queue/exchange are
   * asserted and bound, and incoming messages are resolved through
   * `resolve` (normally the IoC container) before being handed to
   * `handle`. On failure the message is retried up to `maxRetries`
   * times before being routed to a dead-letter exchange.
   */
  async startConsumers(resolve: ConsumerResolver) {
    for (const [ctor, options] of consumersRegistry.entries()) {
      await this.#startConsumer(ctor, options, resolve)
    }
  }

  async #startConsumer(
    ctor: ConsumerConstructor,
    options: ConsumerOptions,
    resolve: ConsumerResolver
  ) {
    const connection = this.connection(options.connection)
    const maxRetries = options.maxRetries ?? 3
    const deadLetterExchange = options.deadLetterExchange ?? `${options.queue}.dlx`
    const retryQueue = `${options.queue}.retry`

    const channelWrapper = connection.createChannel({
      setup: async (channel: ConfirmChannel) => {
        await channel.assertExchange(deadLetterExchange, 'fanout', { durable: true })

        if (options.assertQueue !== false) {
          await channel.assertQueue(options.queue, {
            durable: true,
            ...(options.maxPriority
              ? { arguments: { 'x-max-priority': options.maxPriority } }
              : {}),
            ...options.queueOptions,
          })
        }

        /**
         * The retry queue dead-letters straight back into the main
         * queue once its per-message TTL expires, giving us a cheap
         * delayed-retry mechanism without a plugin.
         */
        await channel.assertQueue(retryQueue, {
          durable: true,
          deadLetterExchange: '',
          deadLetterRoutingKey: options.queue,
          messageTtl: 5000,
        })

        if (options.exchange) {
          await channel.assertExchange(options.exchange, options.exchangeType ?? 'topic', {
            durable: true,
            ...options.exchangeOptions,
          })

          if (options.exchangeType === 'headers') {
            await channel.bindQueue(options.queue, options.exchange, '', {
              ...options.headers,
              'x-match': options.headersMatch ?? 'all',
            })
          } else {
            const routingKeys = options.routingKey
              ? Array.isArray(options.routingKey)
                ? options.routingKey
                : [options.routingKey]
              : ['']
            for (const key of routingKeys) {
              await channel.bindQueue(options.queue, options.exchange, key)
            }
          }
        }

        await channel.consume(options.queue, async (message: ConsumeMessage | null) => {
          if (!message) return

          try {
            const instance = await resolve(ctor)
            const payload = JSON.parse(message.content.toString())
            await instance.handle(payload, message)
            channel.ack(message)
          } catch (error) {
            await this.#handleFailure(channel, message, {
              retryQueue,
              deadLetterExchange,
              maxRetries,
              error,
              consumerName: ctor.name,
            })
          }
        })

        debug('consumer "%s" listening on queue "%s"', ctor.name, options.queue)
      },
    })

    /**
     * Wait for the channel's setup (queue/exchange assertions and
     * bindings) to actually finish before returning, so a publish
     * that happens right after `startConsumers()` resolves can't
     * race ahead of the consumer's topology setup.
     */
    await channelWrapper.waitForConnect()
  }

  async #handleFailure(
    channel: ConfirmChannel,
    message: ConsumeMessage,
    context: {
      retryQueue: string
      deadLetterExchange: string
      maxRetries: number
      error: unknown
      consumerName: string
    }
  ) {
    const deaths = (message.properties.headers?.['x-death'] as Array<{ count: number }>) ?? []
    const attempts = deaths.reduce((total, death) => total + death.count, 0)

    this.#logger.error(
      { err: context.error, consumer: context.consumerName, attempts },
      'RabbitMQ consumer failed to process message'
    )

    if (attempts < context.maxRetries) {
      channel.sendToQueue(context.retryQueue, message.content, {
        persistent: true,
        headers: message.properties.headers,
      })
      channel.ack(message)
      return
    }

    channel.publish(context.deadLetterExchange, '', message.content, {
      persistent: true,
      headers: message.properties.headers,
    })
    channel.ack(message)
  }

  /**
   * Close a named connection, or the default one when no name is
   * given.
   */
  async close(connectionName?: keyof ConnectionsList) {
    const name = (connectionName ?? this.managerConfig.connection) as string
    const connection = this.#activeConnections.get(name)
    if (!connection) return
    await connection.close()
    this.#activeConnections.delete(name)
  }

  /**
   * Close every open connection. Called by the provider on app
   * shutdown.
   */
  async closeAll() {
    await Promise.all([...this.#activeConnections.keys()].map((name) => this.close(name)))
  }
}
