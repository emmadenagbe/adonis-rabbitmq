/*
 * @emmadenagbe/rabbitmq
 *
 * RabbitMQ provider for AdonisJS 6
 */

import type Configure from '@adonisjs/core/commands/configure'
import { stubsRoot } from './stubs/main.js'

/**
 * Configures the package
 */
export async function configure(command: Configure) {
  const codemods = await command.createCodemods()

  /**
   * Publish config file
   */
  await codemods.makeUsingStub(stubsRoot, 'config/rabbitmq.stub', {})

  /**
   * Add environment variables
   */
  await codemods.defineEnvVariables({
    RABBITMQ_URL: 'amqp://guest:guest@localhost:5672',
  })

  /**
   * Validate environment variables
   */
  await codemods.defineEnvValidations({
    variables: {
      RABBITMQ_URL: 'Env.schema.string()',
    },
  })

  /**
   * Add provider to rc file
   */
  await codemods.updateRcFile((rcFile: any) => {
    rcFile.addProvider('@emmadenagbe/rabbitmq/rabbitmq_provider')
  })
}
