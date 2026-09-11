/*
 * @emmadenagbe/rabbitmq
 *
 * RabbitMQ provider for AdonisJS 6
 */

import type { ConsumerConstructor, ConsumerOptions } from './types.js'

/**
 * Registry mapping decorated consumer classes to their options. The
 * manager reads this when it starts consuming, resolving each class
 * through the IoC container so consumers can use `@inject`.
 */
export const consumersRegistry = new Map<ConsumerConstructor, ConsumerOptions>()

/**
 * Marks a class as a RabbitMQ consumer bound to the given queue
 * (and, optionally, an exchange/routing key it should be bound to).
 *
 * ```ts
 * @consumer({ queue: 'orders', exchange: 'orders_exchange', routingKey: 'order.created' })
 * export default class OrdersConsumer extends BaseConsumer<OrderCreated> {
 *   async handle(payload: OrderCreated) {
 *     // ...
 *   }
 * }
 * ```
 */
export function consumer(options: ConsumerOptions) {
  return function decorate<T extends ConsumerConstructor>(target: T) {
    consumersRegistry.set(target, options)
    return target
  }
}
