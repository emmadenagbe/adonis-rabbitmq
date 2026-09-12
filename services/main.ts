/*
 * @emmadenagbe/adonis-rabbitmq
 *
 * RabbitMQ provider for AdonisJS 6
 */

import app from '@adonisjs/core/services/app'
import type { RabbitMQService } from '../src/types.js'

let rabbitmq: RabbitMQService

/**
 * Returns a singleton instance of the RabbitMQ manager from the
 * container.
 */
await app.booted(async () => {
  rabbitmq = await app.container.make('rabbitmq')
})

export { rabbitmq as default }
