/*
 * @emmadenagbe/rabbitmq
 *
 * RabbitMQ provider for AdonisJS 6
 */

import { randomUUID } from 'node:crypto'
import amqp, { type AmqpConnectionManager, type ChannelWrapper } from 'amqp-connection-manager'
import type { ConfirmChannel, ConsumeMessage } from 'amqplib'

import debug from './debug.js'
import { E_RABBITMQ_RPC_TIMEOUT } from './errors.js'
import type { RabbitMQConnectionConfig } from './types.js'

type PendingRpcRequest = {
  resolve: (value: unknown) => void
  reject: (error: unknown) => void
  timer: NodeJS.Timeout
}

/**
 * Wraps a single amqp-connection-manager connection plus a shared
 * "default" channel used for publishing. Consumers each get their
 * own dedicated channel (see `createChannel`).
 */
export default class RabbitMQConnection {
  #connectionManager: AmqpConnectionManager
  #publishChannel: ChannelWrapper
  #rpcChannel?: ChannelWrapper
  #rpcReplyQueue?: string
  #pendingRpcRequests = new Map<string, PendingRpcRequest>()

  constructor(
    public connectionName: string,
    public config: RabbitMQConnectionConfig
  ) {
    const urls = Array.isArray(config.url) ? config.url : [config.url]

    debug('creating rabbitmq connection %s', connectionName)
    this.#connectionManager = amqp.connect(urls, config.connectionOptions)
    this.#connectionManager.on('connect', () =>
      debug('connection "%s" established', connectionName)
    )
    this.#connectionManager.on('disconnect', ({ err }) =>
      debug('connection "%s" dropped: %s', connectionName, err?.message)
    )

    this.#publishChannel = this.#connectionManager.createChannel({
      json: false,
      setup: async (channel: ConfirmChannel) => {
        if (config.prefetch) {
          await channel.prefetch(config.prefetch)
        }
      },
    })
  }

  get connectionManager() {
    return this.#connectionManager
  }

  /**
   * Publish a JSON-serializable payload to an exchange with a
   * routing key. Uses the connection's shared, auto-reconnecting
   * publish channel.
   */
  async publish(
    exchange: string,
    routingKey: string,
    payload: unknown,
    options?: Parameters<ChannelWrapper['publish']>[3]
  ) {
    const content = Buffer.from(JSON.stringify(payload))
    return this.#publishChannel.publish(exchange, routingKey, content, {
      contentType: 'application/json',
      persistent: true,
      ...options,
    })
  }

  /**
   * Send a JSON-serializable payload directly to a queue (bypassing
   * any exchange).
   */
  async sendToQueue(
    queue: string,
    payload: unknown,
    options?: Parameters<ChannelWrapper['sendToQueue']>[2]
  ) {
    const content = Buffer.from(JSON.stringify(payload))
    return this.#publishChannel.sendToQueue(queue, content, {
      contentType: 'application/json',
      persistent: true,
      ...options,
    })
  }

  /**
   * Create a dedicated channel with its own setup/consume function.
   * Used by the manager to set up one channel per consumer so a
   * slow/failing consumer cannot block publishing or other consumers.
   */
  createChannel(options: {
    json?: boolean
    setup: (channel: ConfirmChannel) => Promise<void>
  }): ChannelWrapper {
    return this.#connectionManager.createChannel(options)
  }

  /**
   * Lazily creates the RPC reply channel: an exclusive, auto-delete,
   * broker-named queue that every `request()` call on this connection
   * shares, with replies correlated back to their caller by
   * `correlationId`.
   */
  async #ensureRpcChannel() {
    if (this.#rpcChannel) {
      await this.#rpcChannel.waitForConnect()
      return
    }

    this.#rpcChannel = this.#connectionManager.createChannel({
      setup: async (channel: ConfirmChannel) => {
        const { queue } = await channel.assertQueue('', { exclusive: true, autoDelete: true })
        this.#rpcReplyQueue = queue

        await channel.consume(
          queue,
          (message: ConsumeMessage | null) => {
            if (!message) return

            const correlationId = message.properties.correlationId
            const pending = this.#pendingRpcRequests.get(correlationId)
            if (!pending) return

            clearTimeout(pending.timer)
            this.#pendingRpcRequests.delete(correlationId)

            try {
              pending.resolve(JSON.parse(message.content.toString()))
            } catch (error) {
              pending.reject(error)
            }
          },
          { noAck: true }
        )
      },
    })

    await this.#rpcChannel.waitForConnect()
  }

  /**
   * Sends a JSON-serializable payload to `queue` and waits for a
   * reply sent back via `reply()`, correlated by a generated
   * `correlationId`. Rejects if no reply arrives within `timeoutMs`
   * (defaults to 30s).
   */
  async request<Response = unknown>(
    queue: string,
    payload: unknown,
    options?: { timeoutMs?: number }
  ): Promise<Response> {
    await this.#ensureRpcChannel()

    const correlationId = randomUUID()
    const timeoutMs = options?.timeoutMs ?? 30_000

    return new Promise<Response>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#pendingRpcRequests.delete(correlationId)
        reject(new E_RABBITMQ_RPC_TIMEOUT([queue, timeoutMs]))
      }, timeoutMs)

      this.#pendingRpcRequests.set(correlationId, {
        resolve: resolve as (value: unknown) => void,
        reject,
        timer,
      })

      this.sendToQueue(queue, payload, {
        correlationId,
        replyTo: this.#rpcReplyQueue,
      }).catch((error) => {
        clearTimeout(timer)
        this.#pendingRpcRequests.delete(correlationId)
        reject(error)
      })
    })
  }

  /**
   * Replies to an RPC request received by a consumer. A no-op if the
   * incoming message has no `replyTo` (i.e. it wasn't sent via
   * `request()`).
   */
  async reply(raw: ConsumeMessage, payload: unknown) {
    if (!raw.properties.replyTo) return
    await this.sendToQueue(raw.properties.replyTo, payload, {
      correlationId: raw.properties.correlationId,
    })
  }

  async close() {
    debug('closing rabbitmq connection %s', this.connectionName)
    for (const pending of this.#pendingRpcRequests.values()) {
      clearTimeout(pending.timer)
    }
    this.#pendingRpcRequests.clear()
    await this.#rpcChannel?.close()
    await this.#publishChannel.close()
    await this.#connectionManager.close()
  }
}

export type { ConsumeMessage }
