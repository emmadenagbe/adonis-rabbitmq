/*
 * @emmadenagbe/adonis-rabbitmq
 *
 * RabbitMQ provider for AdonisJS 6
 */

import { RuntimeException } from '@poppinss/utils'
import type { RabbitMQConnectionsList } from './types.js'

/**
 * Define config for the RabbitMQ provider. Validates the shape at
 * boot time and infers connection names for the `RabbitMQConnections`
 * interface via declaration merging in the user's config file.
 */
export function defineConfig<Connections extends RabbitMQConnectionsList>(config: {
  connection: keyof Connections
  connections: Connections
}): {
  connection: keyof Connections
  connections: Connections
} {
  if (!config) {
    throw new RuntimeException('Invalid config. It must be an object')
  }

  if (!config.connections) {
    throw new RuntimeException('Missing "connections" property in the rabbitmq config file')
  }

  if (!config.connection) {
    throw new RuntimeException(
      'Missing "connection" property in rabbitmq config. Specify a default connection to use'
    )
  }

  if (!config.connections[config.connection]) {
    throw new RuntimeException(
      `Missing "connections.${String(
        config.connection
      )}". It is referenced by the "connection" property`
    )
  }

  return config
}
