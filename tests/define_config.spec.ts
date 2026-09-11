import { test } from '@japa/runner'
import { defineConfig } from '../src/define_config.js'

test.group('defineConfig', () => {
  test('throws when connections is missing', ({ assert }) => {
    assert.throws(() => defineConfig({} as any), /Missing "connections"/)
  })

  test('throws when the default connection is missing', ({ assert }) => {
    assert.throws(
      () => defineConfig({ connections: { main: { url: 'amqp://x' } } } as any),
      /Missing "connection"/
    )
  })

  test('throws when the default connection is not defined in connections', ({ assert }) => {
    assert.throws(
      () =>
        defineConfig({
          connection: 'main',
          connections: { other: { url: 'amqp://x' } },
        } as any),
      /Missing "connections.main"/
    )
  })

  test('returns the config as-is when valid', ({ assert }) => {
    const config = defineConfig({
      connection: 'main',
      connections: { main: { url: 'amqp://x' } },
    })
    assert.equal(config.connection, 'main')
  })
})
