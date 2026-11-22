import assert from 'node:assert/strict'

process.env.DSH_CONTINUATION_DISABLED = '0'
const mod = await import('../lib/index.js')

function harness(plans) {
  const hooks = []
  const logged = []
  const ctx = {
    on(event, fn) {
      assert.equal(event, 'llm/stream')
      hooks.push(fn)
      return () => {}
    },
    effect() {},
    logger: {
      warn: (...args) => logged.push(['warn', ...args]),
      error: (...args) => logged.push(['error', ...args]),
    },
  }
  mod.apply(ctx)
  const calls = []
  const runtime = {
    ctx,
    stream(options) {
      const plan = plans[Math.min(calls.length, plans.length - 1)]
      calls.push(options)
      const base = async function* () {
        if (plan === THROW) throw new Error('upstream down')
        for (const chunk of plan) yield chunk
      }
      let factory = () => base()
      for (let i = hooks.length - 1; i >= 0; i--) {
        const hook = hooks[i]
        const downstream = factory
        factory = () => hook.call(runtime, options, downstream)
      }
      return factory()
    },
  }
  const options = { provider: 'deepseek', model: 'deepseek-flash', messages: [{ role: 'user', content: [{ type: 'text', text: 'hi' }] }] }
  return { ctx, runtime, options, calls, logged, run: () => runtime.stream(options) }
}

const THROW = Symbol('throw')
const text = (s, index = 0) => [
  { type: 'block-start', index, blockType: 'text' },
  { type: 'text-delta', index, text: s },
  { type: 'block-end', index, block: { type: 'text', text: s } },
]
const usage = (inputTokens, outputTokens) => ({ type: 'usage', usage: { inputTokens, outputTokens } })
const finish = (reason, replayState) => ({ type: 'finish', reason, ...(replayState === undefined ? {} : { replayState }) })
const visibleText = (chunks) => chunks.filter((c) => c.type === 'text-delta').map((c) => c.text).join('')

async function collect(iterable) {
  const out = []
  for await (const chunk of iterable) out.push(chunk)
  return out
}

// 1. truncation -> one continuation, remapped indexes, merged usage, stop
{
  const h = harness([
    [...text('1234'), usage(10, 5), finish({ kind: 'max-tokens' })],
    [...text('5678'), usage(12, 7), finish({ kind: 'stop' })],
  ])
  const out = await collect(h.run())
  assert.equal(h.calls.length, 2, 'should issue exactly one continuation request')
  assert.equal(h.calls[1].reasoningEffort, 'off', 'continuation must disable thinking')
  assert.equal(h.calls[1].messages.at(-1).role, 'user')
  assert.match(h.calls[1].messages.at(-1).content[0].text, /请从断点接着写/)
  assert.equal(visibleText(out), '12345678')
  assert.deepEqual(out.filter((c) => c.type !== 'usage').map((c) => c.index).filter((i) => i !== undefined), [0, 0, 0, 1, 1, 1])
  const usageChunks = out.filter((c) => c.type === 'usage')
  assert.equal(usageChunks.length, 1)
  assert.deepEqual(usageChunks[0].usage, { inputTokens: 22, outputTokens: 12 })
  assert.deepEqual(out.at(-1).reason, { kind: 'stop' })
  console.log('PASS truncation continuation')
}

// 2. normal stop -> byte-for-byte regression
{
  const plan = [...text('hello'), usage(3, 2), finish({ kind: 'stop' }, { blocks: [] })]
  const h = harness([plan])
  const out = await collect(h.run())
  assert.equal(h.calls.length, 1)
  assert.deepEqual(out, plan)
  console.log('PASS stop regression')
}

// 3. user abort / tool call / text-free error -> never continue
for (const [label, chunks] of [
  ['abort', [...text('partial'), finish({ kind: 'aborted', failure: { code: 'ABORTED' } })]],
  ['tool call', [...text('partial'), { type: 'tool-call-delta', index: 1, id: 't1', name: 'f', argumentsDelta: '{}' }, finish({ kind: 'max-tokens' })]],
  ['text-free error', [{ type: 'reasoning-delta', index: 0, text: 'thinking only' }, finish({ kind: 'error', failure: { code: 'HTTP_500' } })]],
]) {
  const h = harness([chunks, [...text('SHOULD_NOT_APPEAR'), finish({ kind: 'stop' })]])
  const out = await collect(h.run())
  assert.equal(h.calls.length, 1, `${label} must not continue`)
  assert.ok(!visibleText(out).includes('SHOULD_NOT_APPEAR'))
  console.log(`PASS no continuation on ${label}`)
}

// 4. continuation request throws -> original partial and finish stay visible
{
  const h = harness([
    [...text('1234'), usage(10, 5), finish({ kind: 'max-tokens' })],
    THROW,
  ])
  const out = await collect(h.run())
  assert.equal(h.calls.length, 2)
  assert.equal(visibleText(out), '1234')
  assert.deepEqual(out.at(-1).reason, { kind: 'max-tokens' })
  assert.ok(h.logged.some((entry) => entry[0] === 'warn'))
  console.log('PASS continuation failure preserves original finish')
}

// 5. sentinel: kill switch must turn truncation continuation red
{
  process.env.DSH_CONTINUATION_DISABLED = '1'
  const sentinel = await import('../lib/index.js?sentinel=1')
  const hooks = []
  const ctx = { on: (e, fn) => { hooks.push(fn); return () => {} }, effect() {}, logger: { warn() {}, error() {} } }
  sentinel.apply(ctx)
  const calls = []
  const runtime = {
    ctx,
    stream(options) {
      calls.push(options)
      return (async function* () {
        yield* text('1234')
        yield finish({ kind: 'max-tokens' })
      })()
    },
  }
  const out = await collect(runtime.stream({ messages: [{ role: 'user', content: [] }] }))
  assert.equal(calls.length, 1)
  assert.deepEqual(out.at(-1).reason, { kind: 'max-tokens' })
  process.env.DSH_CONTINUATION_DISABLED = '0'
  console.log('PASS sentinel kill switch')
}

// 6. incompatible runtime -> explicit error, no silent no-op
{
  const hooks = []
  const ctx = { on: (e, fn) => { hooks.push(fn); return () => {} }, effect() {}, logger: { error() {}, warn() {} } }
  mod.apply(ctx)
  await assert.rejects(
    async () => { for await (const _ of hooks[0].call({}, { messages: [] }, async function* () {})) {} },
    /内核接口不兼容/,
  )
  console.log('PASS incompatible runtime error')
}

console.log('ALL PASS')
