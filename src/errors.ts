/*
 * @emmadenagbe/adonis-rabbitmq
 *
 * RabbitMQ provider for AdonisJS 6
 */

import { createError } from '@poppinss/utils'

export const E_RABBITMQ_CONNECTION_NOT_FOUND = createError<[string]>(
  'RabbitMQ connection "%s" is not defined',
  'E_RABBITMQ_CONNECTION_NOT_FOUND'
)

export const E_RABBITMQ_CONSUMER_RESOLUTION_FAILED = createError<[string]>(
  'Unable to resolve RabbitMQ consumer "%s" from the container',
  'E_RABBITMQ_CONSUMER_RESOLUTION_FAILED'
)

export const E_RABBITMQ_RPC_TIMEOUT = createError<[string, number]>(
  'RPC request to queue "%s" timed out after %dms',
  'E_RABBITMQ_RPC_TIMEOUT'
)
