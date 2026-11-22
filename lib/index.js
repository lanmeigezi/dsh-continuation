/**
 * dsh-continuation
 *
 * Wrap the `llm/stream` waterfall. When a response ends with `max-tokens` or an
 * upstream error AFTER visible text was produced, send one continuation request
 * and append its text chunks to the same stream so the agent loop still sees a
 * single `finish`.
 *
 * No version check: this uses the runtime hook shape only. If the kernel does
 * not provide that shape, apply()/the hook throws an explicit error.
 */

export const name = 'dsh-continuation'
export const inject = ['llm']

const MAX_ROUNDS = 3
const MIN_GROWTH = 20
const TAIL_CHARS = 1500
const DISABLED = process.env.DSH_CONTINUATION_DISABLED === '1'

const CONTINUE_PROMPT = (partial) => `你上一轮的输出被中断了。下面是你已经写出的内容末尾（只用于定位断点，不要重复、不要复述）：

<<<
${partial}
>>>

请从断点接着写：不复述已写内容、不重新开头、不解释、不加任何前言；代码保持缩进与语法连续。`

function report(ctx, level, message, ...args) {
  try {
    ctx?.logger?.[level]?.('[dsh-continuation] ' + message, ...args)
  } catch {
    // logging must never break the stream
  }
}

function incompatible(ctx, message) {
  const error = new Error('[dsh-continuation] ' + message)
  report(ctx, 'error', '%s', error.message)
  throw error
}

function tail(text, limit) {
  return text.length <= limit ? text : text.slice(-limit)
}

function mergeUsage(usage, next) {
  if (!next || typeof next !== 'object') return usage
  const out = usage ? { ...usage } : {}
  for (const [key, value] of Object.entries(next)) {
    if (typeof value === 'number' && Number.isFinite(value)) {
      out[key] = (typeof out[key] === 'number' && Number.isFinite(out[key]) ? out[key] : 0) + value
    } else if (!(key in out)) {
      out[key] = value
    }
  }
  return out
}

function remap(chunk, base) {
  if (!chunk || typeof chunk !== 'object' || typeof chunk.index !== 'number') return chunk
  return { ...chunk, index: chunk.index + base }
}

function shouldContinue(finish, text, sawToolCall, signal) {
  if (DISABLED) return false
  if (!finish || signal?.aborted || sawToolCall) return false
  if (finish.kind !== 'max-tokens' && finish.kind !== 'error') return false
  if (finish.failure?.code === 'ABORTED') return false
  return text.trim().length > 0
}

export function apply(ctx) {
  if (!ctx || typeof ctx.on !== 'function') {
    incompatible(ctx, '当前内核不支持插件事件注册（ctx.on 缺失），插件无法挂载')
  }

  // A continuation request re-enters this waterfall through runtime.stream().
  // Mark its exact options object so this hook only forwards it once.
  const inflight = new WeakSet()

  const dispose = ctx.on('llm/stream', async function* (options, next) {
    if (typeof next !== 'function') {
      incompatible(ctx, 'llm/stream 钩子未收到 next()，内核接口不兼容')
    }
    const runtime = this
    if (!runtime || typeof runtime.stream !== 'function') {
      incompatible(ctx, 'llm/stream 的 this 不是 LlmRuntime（缺少 stream()），内核接口不兼容')
    }
    if (inflight.has(options)) {
      yield* next()
      return
    }
    if (!options || !Array.isArray(options.messages)) {
      report(ctx, 'warn', 'options.messages 缺失，跳过续写包装')
      yield* next()
      return
    }

    let acc = ''
    let sawToolCall = false
    let maxIndex = -1
    let usage = null
    let originalFinishChunk = null
    const usageChunks = []

    for await (const chunk of next()) {
      if (!chunk || typeof chunk !== 'object') {
        yield chunk
        continue
      }
      switch (chunk.type) {
        case 'text-delta':
          if (typeof chunk.text === 'string') acc += chunk.text
          yield chunk
          break
        case 'tool-call-delta':
          sawToolCall = true
          yield chunk
          break
        case 'usage':
          usageChunks.push(chunk)
          usage = mergeUsage(usage, chunk.usage)
          break
        case 'finish':
          originalFinishChunk = chunk
          break
        default:
          yield chunk
      }
      if (typeof chunk.index === 'number') maxIndex = Math.max(maxIndex, chunk.index)
    }

    const originalFinish = originalFinishChunk?.reason
    const replayState = originalFinishChunk?.replayState

    // Regression path: leave an ordinary response byte-for-byte alone.
    if (!shouldContinue(originalFinish, acc, sawToolCall, options.signal)) {
      for (const chunk of usageChunks) yield chunk
      if (originalFinishChunk) yield originalFinishChunk
      return
    }

    let complete = false
    let finalFinish = originalFinish
    let abortFinish = null

    for (let round = 0; round < MAX_ROUNDS; round++) {
      if (options.signal?.aborted) break
      if (!shouldContinue(finalFinish, acc, sawToolCall, options.signal)) break

      const base = maxIndex + 1
      const continued = {
        ...options,
        // 0.2.x deepseek adapter maps "off" to thinking:{type:"disabled"}.
        // Omitting it falls back to the configured effort and can return 400
        // when tools are present and a partial assistant turn is replayed.
        reasoningEffort: 'off',
        messages: [
          ...options.messages,
          { role: 'user', content: [{ type: 'text', text: CONTINUE_PROMPT(tail(acc, TAIL_CHARS)) }] },
        ],
      }

      inflight.add(continued)
      const buffered = []
      let roundText = ''
      let roundFinish = null
      let roundSawTool = false
      try {
        for await (const chunk of runtime.stream(continued)) {
          if (!chunk || typeof chunk !== 'object') continue
          switch (chunk.type) {
            case 'text-delta': {
              const text = typeof chunk.text === 'string' ? chunk.text : ''
              roundText += text
              buffered.push(remap(chunk, base))
              break
            }
            case 'tool-call-delta':
              roundSawTool = true
              buffered.push(remap(chunk, base))
              break
            case 'reasoning-delta':
              // Continuation runs with thinking disabled; nothing to append.
              break
            case 'usage':
              usage = mergeUsage(usage, chunk.usage)
              break
            case 'finish':
              roundFinish = chunk.reason
              break
            default:
              buffered.push(remap(chunk, base))
          }
        }
      } catch (error) {
        report(ctx, 'warn', '续写请求失败，保留原响应：%o', error)
        break
      } finally {
        inflight.delete(continued)
      }

      if (roundSawTool) {
        report(ctx, 'warn', '续写段出现工具调用，已丢弃该段')
        finalFinish = originalFinish
        break
      }

      for (const chunk of buffered) {
        yield chunk
        if (typeof chunk.index === 'number') maxIndex = Math.max(maxIndex, chunk.index)
      }
      acc += roundText
      finalFinish = roundFinish ?? finalFinish

      if (roundFinish?.kind === 'aborted') {
        abortFinish = roundFinish
        complete = false
        break
      }

      if (roundText.length === 0) {
        finalFinish = originalFinish
        break
      }

      if (roundFinish?.kind === 'stop' || roundFinish?.kind === 'tool-calls') {
        complete = true
        break
      }

      if (roundText.length < MIN_GROWTH) {
        finalFinish = originalFinish
        break
      }
    }

    if (usage) yield { type: 'usage', usage }

    if (abortFinish) {
      yield { type: 'finish', reason: abortFinish }
    } else if (complete) {
      yield { type: 'finish', reason: { kind: 'stop' }, ...(replayState === undefined ? {} : { replayState }) }
    } else if (originalFinishChunk) {
      yield originalFinishChunk
    } else {
      yield { type: 'finish', reason: finalFinish ?? { kind: 'stop' } }
    }
  })

  ctx.effect?.(() => () => dispose(), 'dsh-continuation: dispose llm/stream hook')
}
