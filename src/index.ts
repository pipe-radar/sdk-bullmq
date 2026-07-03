/**
 * @piperadar/bullmq
 *
 * Drop-in monitoring for BullMQ queues. One line in your worker process:
 *
 *   import { PipeRadar } from '@piperadar/bullmq'
 *   const pr = PipeRadar({ apiKey: 'pr_live_...' })
 *   pr.watch(myQueue)
 *
 * It hooks BullMQ's native QueueEvents, computes latency, batches events, and
 * ships them to PipeRadar. Monitoring must never break the host app, so every
 * failure path here is swallowed.
 */

import { randomUUID } from 'node:crypto'
import { Queue, QueueEvents } from 'bullmq'

export interface PipeRadarOptions {
  apiKey: string
  /** Ingestion API base URL. Defaults to production. */
  apiUrl?: string
  /** Max events to buffer before flushing. Default: 25 */
  batchSize?: number
  /** Flush interval in ms. Default: 5000 */
  flushInterval?: number
  /**
   * Max events to hold in the in-memory retry buffer while delivery is failing.
   * Bounds memory during an outage; once exceeded, the oldest batches are
   * dropped first. Default: 1000
   */
  maxBufferedEvents?: number
  /** Set false to disable entirely (e.g. in tests). Default: true */
  enabled?: boolean
  /**
   * How job error messages are handled before they leave your infrastructure.
   * Failures are always grouped by queue + job; this only governs the free-text
   * message. Default: 'scrub'.
   * - 'scrub': strip emails, tokens, JWTs, UUIDs, IPs, long numbers, and phone
   *   numbers from the message before sending — safe by default.
   * - 'raw':   send the message verbatim (only if you're sure it carries no PII).
   * - 'off':   never send error text at all (failures still group by queue/job).
   */
  errorMessages?: 'scrub' | 'raw' | 'off'
  /**
   * Escape hatch for full control: given the raw error message, return exactly
   * the string to send (or undefined to omit it). Overrides `errorMessages`.
   * A throwing transformer is treated as "omit" — it can never break monitoring.
   */
  errorTransformer?: (rawMessage: string) => string | undefined
}

/** The wire shape sent to /v1/ingest. workspace_id is stamped server-side. */
interface JobEvent {
  queue_name: string
  adapter_type: 'bullmq'
  job_id: string
  job_name: string
  status: 'completed' | 'failed' | 'retrying'
  latency_ms?: number
  attempt: number
  max_attempts: number
  error_msg?: string
  occurred_at: string
}

const DEFAULT_API_URL = 'https://api.piperadar.com'
const DEFAULT_MAX_BUFFERED_EVENTS = 1000
const MAX_ERROR_MSG_LEN = 500
const FETCH_TIMEOUT_MS = 5000

/**
 * `DEBUG=piperadar` (or `DEBUG=*`) turns on transparency logging: the exact JSON
 * body of every ingest request is printed to stderr, so you can verify for
 * yourself that job payloads never leave your infrastructure.
 */
const DEBUG_ENABLED = (() => {
  const d = process.env.DEBUG ?? ''
  return d === '*' || d.split(/[\s,]+/).includes('piperadar')
})()

function truncate(s: string): string {
  return s.length > MAX_ERROR_MSG_LEN ? s.slice(0, MAX_ERROR_MSG_LEN) + '…' : s
}

/**
 * Redact the usual PII/secret shapes from a free-text error message so it's safe
 * to send off-box by default, while keeping enough of the message intact for the
 * backend to group identical failures. Best-effort — it catches the common cases
 * (emails, tokens, JWTs, UUIDs, IPs, long numbers, phone numbers), not every
 * possible leak. Developers who need certainty use `errorMessages: 'off'` or an
 * `errorTransformer`.
 */
function scrubErrorMessage(msg: string): string {
  return msg
    // JWTs first (three base64url segments) — before the generic key rule.
    .replace(/\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, '[redacted-jwt]')
    // Emails.
    .replace(/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/g, '[redacted-email]')
    // `Authorization: Bearer <token>` style headers.
    .replace(/\bBearer\s+[A-Za-z0-9._-]{8,}/gi, 'Bearer [redacted]')
    // Prefixed secret keys — Stripe/PipeRadar-style (sk_live_…, pr_test_…) and
    // GitHub tokens (ghp_…, gho_…).
    .replace(/\b[A-Za-z]{2,}_(?:live|test)_[A-Za-z0-9]{6,}/g, '[redacted-key]')
    .replace(/\bgh[pousr]_[A-Za-z0-9]{20,}/g, '[redacted-key]')
    // UUIDs.
    .replace(/\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/gi, '[redacted-uuid]')
    // IPv4 addresses.
    .replace(/\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b/g, '[redacted-ip]')
    // Phone numbers with separators (e.g. +1 415-555-0132).
    .replace(/\b(?:\+?\d{1,3}[\s.-]?)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}\b/g, '[redacted-phone]')
    // Any remaining long digit run (card numbers, long numeric ids).
    .replace(/\b\d{7,}\b/g, '[redacted-number]')
}

/**
 * A batch of events awaiting (re)delivery, paired with the idempotency key it
 * must carry on *every* attempt. Reusing the key is what lets the backend's
 * exactly-once machinery dedupe a retried batch instead of double-counting it.
 */
interface PendingBatch {
  key: string
  events: JobEvent[]
}

class PipeRadarClient {
  private apiKey: string
  private apiUrl: string
  private batchSize: number
  private flushInterval: number
  private maxBufferedEvents: number
  private enabled: boolean
  private errorMode: 'scrub' | 'raw' | 'off'
  private errorTransformer?: (rawMessage: string) => string | undefined
  private buffer: JobEvent[] = []
  // Batches taken from `buffer` that are sent or awaiting retry, oldest first.
  private pending: PendingBatch[] = []
  // Guards against two flushes (timer + a batch-full push) draining `pending`
  // concurrently and sending the same batch twice.
  private flushing = false
  private timer: NodeJS.Timeout | null = null
  private startTimes = new Map<string, number>()
  private watched = new Set<string>()
  private events: QueueEvents[] = []

  constructor(opts: PipeRadarOptions) {
    this.apiKey = opts.apiKey
    this.apiUrl = (opts.apiUrl ?? DEFAULT_API_URL).replace(/\/$/, '')
    this.batchSize = opts.batchSize ?? 25
    this.flushInterval = opts.flushInterval ?? 5000
    this.maxBufferedEvents = opts.maxBufferedEvents ?? DEFAULT_MAX_BUFFERED_EVENTS
    this.enabled = opts.enabled ?? true
    this.errorMode = opts.errorMessages ?? 'scrub'
    this.errorTransformer = opts.errorTransformer
    if (this.enabled) this.scheduleFlush()
  }

  /**
   * Attach PipeRadar to a BullMQ Queue. Listens to native queue events, so
   * there is zero overhead inside your job processing path.
   */
  watch(queue: Queue): void {
    if (!this.enabled) return
    const queueName = queue.name
    if (this.watched.has(queueName)) return // idempotent
    this.watched.add(queueName)

    const queueEvents = new QueueEvents(queueName, {
      connection: (queue as any).opts?.connection,
    })
    this.events.push(queueEvents)

    // Mark latency start. We never SEND "started" events — the backend ignores
    // them — we only use them to compute latency, which also keeps the
    // customer's event quota low.
    queueEvents.on('active', ({ jobId }) => {
      this.startTimes.set(jobId, Date.now())
    })

    queueEvents.on('completed', async ({ jobId }) => {
      const job = await this.safeGetJob(queue, jobId)
      this.push({
        queue_name: queueName,
        job_id: jobId,
        job_name: job?.name ?? 'unknown',
        status: 'completed',
        latency_ms: this.popLatency(jobId),
        attempt: job?.attemptsMade ?? 1,
        max_attempts: job?.opts?.attempts ?? 1,
        occurred_at: new Date().toISOString(),
      })
    })

    queueEvents.on('failed', async ({ jobId, failedReason }) => {
      const job = await this.safeGetJob(queue, jobId)
      const attemptsMade = job?.attemptsMade ?? 1
      const maxAttempts = job?.opts?.attempts ?? 1
      // A failure that will be retried is reported as "retrying"; only the
      // final attempt is a terminal "failed" (which the backend stores as a row).
      const isRetrying = attemptsMade < maxAttempts
      this.push({
        queue_name: queueName,
        job_id: jobId,
        job_name: job?.name ?? 'unknown',
        status: isRetrying ? 'retrying' : 'failed',
        latency_ms: this.popLatency(jobId),
        attempt: attemptsMade,
        max_attempts: maxAttempts,
        error_msg: this.formatError(failedReason),
        occurred_at: new Date().toISOString(),
      })
    })
  }

  /**
   * Apply the configured error-message policy before a message leaves the box.
   * Scrubs PII/secrets by default; honors an `errorTransformer` if given. Never
   * throws — a bad transformer must not break monitoring, so it drops instead.
   */
  private formatError(raw: string | undefined | null): string | undefined {
    if (!raw) return undefined
    if (this.errorTransformer) {
      try {
        const out = this.errorTransformer(raw)
        return out ? truncate(out) : undefined
      } catch {
        return undefined
      }
    }
    if (this.errorMode === 'off') return undefined
    return truncate(this.errorMode === 'raw' ? raw : scrubErrorMessage(raw))
  }

  private async safeGetJob(queue: Queue, jobId: string): Promise<any | null> {
    try {
      return await queue.getJob(jobId)
    } catch {
      return null
    }
  }

  private push(event: Omit<JobEvent, 'adapter_type'>): void {
    this.buffer.push({ ...event, adapter_type: 'bullmq' })
    if (this.buffer.length >= this.batchSize) {
      void this.flush()
    }
  }

  private popLatency(jobId: string): number | undefined {
    const start = this.startTimes.get(jobId)
    if (start === undefined) return undefined
    this.startTimes.delete(jobId)
    return Date.now() - start
  }

  async flush(): Promise<void> {
    // Seal whatever's currently buffered into a batch with a stable idempotency
    // key. The key is minted once, here, and reused on every retry of this exact
    // set of events — so a retried POST is deduped by the backend rather than
    // double-counted in the rollups and monthly quota.
    if (this.buffer.length > 0) {
      const events = this.buffer.splice(0, this.buffer.length)
      this.pending.push({ key: randomUUID(), events })
      this.enforceBufferBound()
    }
    if (this.pending.length === 0 || this.flushing) return

    this.flushing = true
    try {
      // Send oldest-first; stop at the first batch that needs retrying so we
      // preserve order and don't hammer a down endpoint. The rest stay buffered
      // for the next flush (timer-driven or batch-full).
      while (this.pending.length > 0) {
        const settled = await this.send(this.pending[0])
        if (!settled) break
        this.pending.shift()
      }
    } finally {
      this.flushing = false
    }
  }

  /**
   * Attempt one batch. Returns true when it's settled and should leave the
   * buffer — either delivered (2xx) or permanently rejected (a non-429 4xx,
   * which retrying the same payload can't fix). Returns false on a transient
   * failure (network error, 5xx, or 429) so the caller retries it later.
   * Never throws: monitoring must not break the host app.
   */
  private async send(batch: PendingBatch): Promise<boolean> {
    try {
      if (DEBUG_ENABLED) {
        // Transparency mode: show the operator the exact bytes leaving the box.
        console.error(
          `[piperadar] POST ${this.apiUrl}/v1/ingest — ${batch.events.length} event(s):\n` +
            JSON.stringify({ events: batch.events }, null, 2),
        )
      }
      const res = await fetch(`${this.apiUrl}/v1/ingest`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-API-Key': this.apiKey,
          'Idempotency-Key': batch.key,
        },
        body: JSON.stringify({ events: batch.events }),
        // Bound each attempt so a hung connection can't block shutdown; a timeout
        // aborts → throws → caught below → the batch is retried on the next flush.
        signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
      })
      if (res.ok) return true
      // A 4xx (other than rate-limit) is a permanent client error — drop the
      // poison batch so it can't wedge the buffer behind retries forever.
      return res.status >= 400 && res.status < 500 && res.status !== 429
    } catch {
      return false // network error → keep for the next flush
    }
  }

  /**
   * Cap the retry buffer's memory by dropping the oldest batches once the total
   * queued events exceed the bound. The stalest monitoring data is sacrificed
   * first; at least one batch is always kept.
   */
  private enforceBufferBound(): void {
    let total = this.pending.reduce((n, b) => n + b.events.length, 0)
    while (total > this.maxBufferedEvents && this.pending.length > 1) {
      total -= this.pending.shift()!.events.length
    }
  }

  private scheduleFlush(): void {
    this.timer = setInterval(() => void this.flush(), this.flushInterval)
    // Don't keep the Node process alive just for the flush timer.
    if (this.timer.unref) this.timer.unref()
  }

  /** Flush remaining events and stop listeners. Call on graceful shutdown. */
  async destroy(): Promise<void> {
    if (this.timer) clearInterval(this.timer)
    await Promise.all(this.events.map((e) => e.close().catch(() => {})))
    await this.flush()
  }
}

/**
 * Create a PipeRadar monitoring client.
 *
 * @example
 *   const pr = PipeRadar({ apiKey: 'pr_live_...' })
 *   pr.watch(emailQueue)
 *   pr.watch(paymentQueue)
 */
export function PipeRadar(opts: PipeRadarOptions): PipeRadarClient {
  return new PipeRadarClient(opts)
}

export type { PipeRadarClient }
