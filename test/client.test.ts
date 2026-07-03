/**
 * Unit tests for the PipeRadar BullMQ client's delivery machinery.
 *
 * These lock the behavior that makes the backend's exactly-once dedupe work and
 * keeps monitoring from breaking the host app under an outage: stable
 * idempotency keys across retries, poison-batch dropping, transient retry, and
 * the bounded retry buffer. They poke private state (`buffer`/`pending`) via an
 * `any` cast and stub global `fetch` — no Redis or real network involved.
 *
 * Run with: npm test
 */

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { PipeRadar } from '../src/index.ts'

interface FetchCall {
  url: string
  headers: Record<string, string>
  body: { events: unknown[] }
}

type FetchResult = { ok?: boolean; status?: number } | 'throw'

/** Build a client with the flush timer disabled so flushes are fully manual. */
function makeClient(opts: Record<string, unknown> = {}): any {
  return PipeRadar({ apiKey: 'pr_test_key', enabled: false, ...opts }) as any
}

function fakeEvent(i: number) {
  return {
    queue_name: 'q',
    adapter_type: 'bullmq',
    job_id: `job-${i}`,
    job_name: 'task',
    status: 'completed',
    attempt: 1,
    max_attempts: 1,
    occurred_at: new Date().toISOString(),
  }
}

/** Stub global fetch with a per-call handler; returns the captured calls. */
function installFetch(handler: (call: FetchCall, n: number) => FetchResult): FetchCall[] {
  const calls: FetchCall[] = []
  globalThis.fetch = (async (url: string, init: any) => {
    const call: FetchCall = { url, headers: init.headers, body: JSON.parse(init.body) }
    calls.push(call)
    const r = handler(call, calls.length)
    if (r === 'throw') throw new Error('network down')
    return { ok: r.ok ?? false, status: r.status ?? (r.ok ? 200 : 500) } as Response
  }) as any
  return calls
}

test('reuses the idempotency key when retrying the same batch', async () => {
  const calls = installFetch((_c, n) => (n === 1 ? 'throw' : { ok: true, status: 200 }))
  const c = makeClient()

  c.buffer.push(fakeEvent(1))
  await c.flush() // seals batch, first attempt throws → kept for retry
  assert.equal(c.pending.length, 1)

  await c.flush() // buffer empty → retries the same batch, succeeds
  assert.equal(c.pending.length, 0)
  assert.equal(calls.length, 2)
  assert.equal(
    calls[0].headers['Idempotency-Key'],
    calls[1].headers['Idempotency-Key'],
    'retry must carry the same key so the backend dedupes it',
  )
})

test('mints a distinct idempotency key per batch', async () => {
  const calls = installFetch(() => ({ ok: true, status: 200 }))
  const c = makeClient()

  c.buffer.push(fakeEvent(1))
  await c.flush()
  c.buffer.push(fakeEvent(2))
  await c.flush()

  assert.equal(calls.length, 2)
  assert.notEqual(calls[0].headers['Idempotency-Key'], calls[1].headers['Idempotency-Key'])
})

test('drops a poison 4xx batch instead of retrying forever', async () => {
  const calls = installFetch(() => ({ ok: false, status: 400 }))
  const c = makeClient()

  c.buffer.push(fakeEvent(1))
  await c.flush()

  assert.equal(c.pending.length, 0, '4xx is permanent → batch must leave the buffer')
  assert.equal(calls.length, 1)
})

test('keeps a batch for retry on transient failures (5xx and 429)', async () => {
  for (const status of [500, 503, 429]) {
    installFetch(() => ({ ok: false, status }))
    const c = makeClient()
    c.buffer.push(fakeEvent(1))
    await c.flush()
    assert.equal(c.pending.length, 1, `status ${status} should be retried, not dropped`)
  }
})

test('bounds the retry buffer by dropping the oldest batches', async () => {
  installFetch(() => 'throw') // every delivery fails → batches accumulate
  const c = makeClient({ maxBufferedEvents: 2 })

  for (let i = 0; i < 4; i++) {
    c.buffer.push(fakeEvent(i))
    await c.flush()
  }

  const total = c.pending.reduce((n: number, b: any) => n + b.events.length, 0)
  assert.ok(total <= 2, `expected <= 2 buffered events, got ${total}`)
  assert.ok(c.pending.length >= 1, 'at least one batch is always kept')

  const newest = c.pending[c.pending.length - 1]
  assert.equal(newest.events[0].job_id, 'job-3', 'the freshest batch must survive')
})

// --- error-message policy (formatError) -------------------------------------
// These lock the "safe by default" promise: PII/secrets never leave the box
// unless the operator explicitly opts in.

test('scrubs PII/secrets from error messages by default', () => {
  const c = makeClient() // no errorMessages → default 'scrub'
  const cases: Array<[string, RegExp]> = [
    ['User john@example.com failed payment', /\[redacted-email\]/],
    ['token eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.abc123 rejected', /\[redacted-jwt\]/],
    ['bad key sk_live_abcdef123456 for charge', /\[redacted-key\]/],
    ['auth ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 denied', /\[redacted-key\]/],
    ['job 550e8400-e29b-41d4-a716-446655440000 not found', /\[redacted-uuid\]/],
    ['connection refused at 10.0.0.12:5432', /\[redacted-ip\]/],
    ['call customer +1 415-555-0132 back', /\[redacted-phone\]/],
    ['card 4242424242424242 declined', /\[redacted-number\]/],
  ]
  for (const [input, expected] of cases) {
    const out = c.formatError(input) as string
    assert.match(out, expected, `should redact in: ${input}`)
    assert.doesNotMatch(out, /john@example\.com|sk_live_abcdef|4242424242424242/, `leaked in: ${input}`)
  }
})

test('keeps the non-sensitive shape so failures still group', () => {
  const c = makeClient()
  assert.equal(
    c.formatError('Stripe API error: card_declined for customer'),
    'Stripe API error: card_declined for customer',
    'a clean message passes through untouched for fingerprinting',
  )
})

test("errorMessages:'raw' sends the message verbatim", () => {
  const c = makeClient({ errorMessages: 'raw' })
  assert.equal(c.formatError('User john@example.com failed'), 'User john@example.com failed')
})

test("errorMessages:'off' omits the message entirely", () => {
  const c = makeClient({ errorMessages: 'off' })
  assert.equal(c.formatError('anything'), undefined)
})

test('errorTransformer overrides the mode and a throwing one drops safely', () => {
  const c = makeClient({ errorMessages: 'raw', errorTransformer: () => 'FIXED' })
  assert.equal(c.formatError('whatever'), 'FIXED')

  const bad = makeClient({ errorTransformer: () => { throw new Error('boom') } })
  assert.equal(bad.formatError('x'), undefined, 'a throwing transformer must not break monitoring')

  const drop = makeClient({ errorTransformer: () => undefined })
  assert.equal(drop.formatError('x'), undefined)
})

test('truncates very long error messages', () => {
  const c = makeClient({ errorMessages: 'raw' })
  const out = c.formatError('x'.repeat(2000)) as string
  assert.ok(out.length <= 501, `expected <=501 chars, got ${out.length}`)
  assert.ok(out.endsWith('…'))
})

// --- event metadata (sdk_version / environment / service) -------------------
// These lock that every event carries identifying metadata on the wire, and that
// `service` is only present when the caller sets one.

test('stamps sdk_version and environment on every event', async () => {
  const calls = installFetch(() => ({ ok: true, status: 200 }))
  const c = makeClient({ environment: 'production', service: 'billing-worker' })

  c.push({
    queue_name: 'q', job_id: 'j1', job_name: 'task', status: 'completed',
    attempt: 1, max_attempts: 1, occurred_at: new Date().toISOString(),
  })
  await c.flush()

  const ev = calls[0].body.events[0] as Record<string, unknown>
  assert.match(String(ev.sdk_version), /^\d+\.\d+\.\d+/, 'sdk_version is stamped')
  assert.equal(ev.environment, 'production')
  assert.equal(ev.service, 'billing-worker')
  assert.equal(ev.adapter_type, 'bullmq')
})

test('defaults environment to NODE_ENV and omits service when unset', async () => {
  const prev = process.env.NODE_ENV
  process.env.NODE_ENV = 'staging'
  try {
    const calls = installFetch(() => ({ ok: true, status: 200 }))
    const c = makeClient() // no environment / service given
    c.push({
      queue_name: 'q', job_id: 'j1', job_name: 'task', status: 'completed',
      attempt: 1, max_attempts: 1, occurred_at: new Date().toISOString(),
    })
    await c.flush()

    const ev = calls[0].body.events[0] as Record<string, unknown>
    assert.equal(ev.environment, 'staging', 'falls back to NODE_ENV')
    assert.ok(!('service' in ev), 'service is omitted from the wire when unset')
  } finally {
    process.env.NODE_ENV = prev
  }
})

// --- apiUrl resolution (advanced.apiUrl with legacy fallback) ---------------

test('advanced.apiUrl takes precedence over the deprecated top-level apiUrl', () => {
  const c = makeClient({ apiUrl: 'https://legacy.example', advanced: { apiUrl: 'https://new.example/' } })
  assert.equal(c.apiUrl, 'https://new.example', 'advanced.apiUrl wins and trailing slash is trimmed')

  const legacy = makeClient({ apiUrl: 'https://legacy.example' })
  assert.equal(legacy.apiUrl, 'https://legacy.example', 'top-level apiUrl still honored for back-compat')

  const def = makeClient()
  assert.equal(def.apiUrl, 'https://piperadar.dev', 'defaults to production')
})
