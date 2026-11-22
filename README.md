# dsh-continuation

A DeepSeek Harness (DSH) plugin that **keeps a long answer going when the model gets cut off**.

When a response ends with `max-tokens` — or an upstream error after the model has already produced visible text — this plugin sends one continuation request and appends the new chunks to the same stream. The agent loop still sees a single `finish`, so nothing downstream has to change.

## What it actually does

```
llm/stream (waterfall)
   ├─ original stream → text-delta / tool-call-delta / usage / finish
   ├─ finish.kind === 'max-tokens' or 'error'  (and no tool call, and not aborted)
   ├─ build a continuation request:
   │     messages + a user turn carrying the last 1500 chars as a "resume from here" anchor
   │     reasoningEffort: 'off'   ← required, see below
   └─ stream the continuation and merge its deltas with remapped indices
```

Up to **3 rounds**. Each round must add at least **20 characters**, otherwise the original finish is kept and the response is left as-is.

## Why `reasoningEffort: 'off'`

A continuation request replays a partial assistant turn. With thinking enabled and tools present, the DeepSeek adapter rejects that with `content[].thinking must be passed back`. Turning reasoning off for the continuation round is what makes the retry legal — this was found by testing, not by reading the docs.

## Install

```bash
dsh plugin --profile web add "<absolute path to this repo>"
```

Then restart dsh once (bundle list change is not hot-reloaded).

The bundle patch is included, so the plugin mounts itself:

```yaml
- insert:
    - id: dsh-continuation
      name: '@local/dsh-continuation'
```

## Kill switch

```bash
DSH_CONTINUATION_DISABLED=1
```

Set it before starting dsh and the hook becomes a pass-through. The self-test asserts this.

## Tests

```bash
node test/self-test.mjs
```

Eight offline cases, no network, no real model calls — they drive the hook with synthetic streams:

| case | what it pins down |
| --- | --- |
| truncation continuation | `max-tokens` → continuation text is appended, finish becomes `stop` |
| stop regression | a normal `stop` response is forwarded byte-for-byte |
| no continuation on abort | `signal.aborted` disables the hook |
| no continuation on tool call | once a tool call appears, do not continue |
| no continuation on text-free error | error without any text → nothing to continue |
| continuation failure preserves original finish | upstream throws mid-continuation → original response survives |
| sentinel kill switch | `DSH_CONTINUATION_DISABLED=1` turns the hook into a pass-through |
| incompatible runtime error | a kernel without `ctx.on` / `runtime.stream` fails loudly |

Last run: `ALL PASS`.

## Design notes

- **Fail loudly, never silently.** If the kernel does not provide `ctx.on` or `runtime.stream`, the plugin throws an explicit error instead of degrading to "no continuation".
- **Never rewrite a good response.** Any response that does not qualify is replayed exactly as received, including its original `finish` chunk and `replayState`.
- **Discard continuation segments that emit tool calls.** A half-answer resumed into a tool call would corrupt the turn; the segment is dropped and the original finish wins.
- **WeakSet guard against re-entry.** A continuation re-enters the same waterfall through `runtime.stream()`; the exact options object is marked so the hook only forwards it once.

## Limitations

- Continuation text is appended in the same message; consumers see one longer response, not two messages.
- If the cut happens inside a thinking block, the turn cannot be continued — thinking carries a signature that is not portable.
- `MAX_ROUNDS`, `MIN_GROWTH` and `TAIL_CHARS` are constants in `lib/index.js`; there is no config surface yet.

## License

MIT
