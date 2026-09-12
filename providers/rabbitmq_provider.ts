/*
 * @emmadenagbe/adonis-rabbitmq
 *
 * RabbitMQ provider for AdonisJS 6
 */

import type { ApplicationService } from '@adonisjs/core/types'
import type { ConsumerConstructor, RabbitMQService } from '../src/types.js'

declare module '@adonisjs/core/types' {
  export interface ContainerBindings {
    rabbitmq: RabbitMQService
  }
}

/**
 * Registers the RabbitMQ manager as a singleton on the container and
 * starts declared consumers once the app has booted.
 */
export default class RabbitMQProvider {
  constructor(protected app: ApplicationService) {}

  register() {
    this.app.container.singleton('rabbitmq', async () => {
      const { default: RabbitMQManager } = await import('../src/rabbitmq_manager.js')

      const config = this.app.config.get<any>('rabbitmq', {})
      const logger = await this.app.container.make('logger')
      return new RabbitMQManager(config, logger)
    })
  }

  /**
   * Start every `@consumer`-decorated class once the app is ready.
   * Consumers are resolved through the container so they can use
   * `@inject` like controllers and commands do.
   */
  async start() {
    const rabbitmq = await this.app.container.make('rabbitmq')
    await rabbitmq.startConsumers((ctor: ConsumerConstructor) => this.app.container.make(ctor))
  }

  /**
   * Gracefully close every connection when the app shuts down so
   * in-flight messages aren't dropped mid-ack.
   */
  async shutdown() {
    const rabbitmq = await this.app.container.make('rabbitmq')
    await rabbitmq.closeAll()
  }
}
