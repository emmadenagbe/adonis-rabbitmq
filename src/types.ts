/*
 * @emmadenagbe/adonis-rabbitmq
 *
 * RabbitMQ provider for AdonisJS 6
 */

import type { Options } from 'amqplib'
import type RabbitMQManager from './rabbitmq_manager.js'

/**
 * Config for a single RabbitMQ connection, resolved by
 * amqp-connection-manager.
 */
export type RabbitMQConnectionConfig = {
  /**
   * One or more broker URLs. Multiple URLs are tried in order and
   * amqp-connection-manager will fail over between them.
   */
  url: string | string[]

  /**
   * Options forwarded to amqp-connection-manager's `connect()`
   */
  connectionOptions?: {
    heartbeatIntervalInSeconds?: number
    reconnectTimeInSeconds?: number
  }

  /**
   * Prefetch count applied to every channel created on this connection
   */
  prefetch?: number
}

export type RabbitMQConnectionsList = Record<string, RabbitMQConnectionConfig>

/**
 * List of connections inferred from the user's config file. Consumers
 * augment this via declaration merging, the same way @adonisjs/redis
 * does for RedisConnections.
 */
export interface RabbitMQConnections {}

export type InferConnections<T extends { connections: RabbitMQConnectionsList }> = T['connections']

/**
 * Describes where a consumer binds: the queue it reads from, and the
 * optional exchange/routing key used to bind that queue.
 */
export type ConsumerOptions = {
  /**
   * Name of the connection to use. Defaults to the manager's default
   * connection when omitted.
   */
  connection?: string

  /**
   * Queue to consume from. Declared automatically (durable by default)
   * unless `assertQueue: false` is set.
   */
  queue: string
  assertQueue?: boolean
  queueOptions?: Options.AssertQueue

  /**
   * Optional exchange to declare and bind the queue to.
   */
  exchange?: string
  exchangeType?: 'direct' | 'topic' | 'fanout' | 'headers'
  exchangeOptions?: Options.AssertExchange
  routingKey?: string | string[]

  /**
   * Used instead of `routingKey` when `exchangeType` is `'headers'`:
   * the queue is bound based on matching message headers rather than
   * a routing key.
   */
  headers?: Record<string, unknown>

  /**
   * Whether a `'headers'` exchange binding requires all header
   * key/value pairs to match, or just one of them. Defaults to `'all'`.
   */
  headersMatch?: 'all' | 'any'

  /**
   * How many times a failed message is redelivered before being
   * routed to the dead-letter queue. Defaults to 3.
   */
  maxRetries?: number

  /**
   * Name of the dead-letter exchange messages are routed to once
   * `maxRetries` is exceeded. Defaults to "<queue>.dlx".
   */
  deadLetterExchange?: string

  /**
   * Enables RabbitMQ's priority queue feature: sets `x-max-priority`
   * on the queue, so messages published with a `priority` option are
   * delivered before lower-priority ones.
   */
  maxPriority?: number
}

/**
 * Options accepted by `publish` and `sendToQueue`, forwarded to
 * amqplib's publish options (e.g. `priority`, `expiration`, custom
 * `headers`) alongside which connection to use.
 */
export type PublishOptions<ConnectionsList extends RabbitMQConnectionsList> = {
  connection?: keyof ConnectionsList
} & Omit<Options.Publish, 'contentType' | 'persistent'>

/**
 * Base class every consumer must extend. `handle` receives the
 * decoded JSON payload and the raw amqplib message for cases where
 * headers/redelivery info are needed.
 */
export abstract class BaseConsumer<Payload = unknown> {
  abstract handle(payload: Payload, raw: import('amqplib').ConsumeMessage): Promise<void> | void
}

export type ConsumerConstructor = new (...args: any[]) => BaseConsumer

/**
 * RabbitMQ service is a singleton manager instance registered with
 * the container based on the user's config.
 */
export interface RabbitMQService extends RabbitMQManager<
  RabbitMQConnections extends RabbitMQConnectionsList ? RabbitMQConnections : never
> {}
