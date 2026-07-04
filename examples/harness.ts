/**
 * Live BullMQ integration example for PipeRadar.
 *
 * This drives the *real* SDK end-to-end: it spins up real BullMQ queues and
 * workers against a local Redis, attaches the PipeRadar SDK to each, and
 * continuously produces a realistic mix of successes, retried failures, and
 * terminal failures. The SDK's native QueueEvents hooks pick those up, compute
 * latency, batch, and ship them to PipeRadar. Watch them land in your dashboard
 * (queue health, failure groups, KPIs) in real time.
 *
 * You need two things: a running Redis, and an ingest key from your PipeRadar
 * dashboard.
 *
 *   PIPERADAR_API_KEY=pr_... npm run example:harness  # from sdk-bullmq/
 *
 * Heads up: this generates continuous synthetic traffic against your PipeRadar
 * project. Stop with Ctrl-C — it flushes buffered events and closes cleanly.
 *
 * Env:
 *   PIPERADAR_API_KEY  (required)  ingest key from your PipeRadar dashboard
 *   PIPERADAR_API_URL  default https://api.piperadar.dev
 *   REDIS_URL          default redis://localhost:6379
 *   RATE_MS            default 700    ms between job submissions
 *   SPIKE              set "1" to force a high failure rate (demo an incident)
 */

import { Queue, Worker } from 'bullmq'
import { PipeRadar } from '../src/index'

const API_KEY = process.env.PIPERADAR_API_KEY
if (!API_KEY) {
  console.error('PIPERADAR_API_KEY is required (get an ingest key from your PipeRadar dashboard)')
  process.exit(1)
}

const API_URL = process.env.PIPERADAR_API_URL ?? 'https://api.piperadar.dev'
const RATE_MS = Number(process.env.RATE_MS ?? 700)
const SPIKE = process.env.SPIKE === '1'

// Minimal redis:// URL parse — host:port is all BullMQ needs locally.
const redisUrl = new URL(process.env.REDIS_URL ?? 'redis://localhost:6379')
const connection = {
  host: redisUrl.hostname,
  port: Number(redisUrl.port || 6379),
  // Required by BullMQ Workers (blocking commands must not time out).
  maxRetriesPerRequest: null as null,
}

interface QueueSpec {
  name: string
  jobName: string
  failPct: number // probability a job attempt throws
  avgLatency: number // mean ms of work before resolving/throwing
  errors: string[] // realistic failure messages — exercise fingerprinting
}

const specs: QueueSpec[] = [
  {
    name: 'payment-processor',
    jobName: 'process_payment',
    failPct: 0.05,
    avgLatency: 300,
    errors: [
      'Stripe API error: card_declined for customer cus_8Kd92Ksl21',
      'connection refused at 10.0.0.12:5432',
      'timeout waiting for payment gateway after 30000ms',
    ],
  },
  {
    name: 'email-sender',
    jobName: 'send_email',
    failPct: 0.03,
    avgLatency: 180,
    errors: [
      'SMTP 421 service not available from mx.provider.net:587',
      "Cannot read property 'address' of undefined at /app/jobs/email.js:42",
    ],
  },
  {
    name: 'webhook-dispatch',
    jobName: 'dispatch_webhook',
    failPct: 0.08,
    avgLatency: 240,
    errors: [
      'POST https://hooks.customer.com/abc123 returned 503',
      'connection refused at 10.0.0.51:443',
    ],
  },
]

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
const pick = <T>(xs: T[]): T => xs[Math.floor(Math.random() * xs.length)]

const pr = PipeRadar({
  apiKey: API_KEY,
  service: 'harness',
  // Small batch + fast flush so you see data quickly while watching the UI.
  batchSize: 10,
  flushInterval: 1500,
  advanced: { apiUrl: API_URL },
})

const queues: Queue[] = []
const workers: Worker[] = []

for (const spec of specs) {
  const queue = new Queue(spec.name, { connection })
  queues.push(queue)
  pr.watch(queue)

  // The worker does real (simulated) work so the SDK measures real latency,
  // then throws on some attempts. attempts:3 means non-terminal failures are
  // retried — the SDK reports those as "retrying" and only the final attempt
  // as "failed", matching the backend's aggregation model (see TESTING.md).
  const worker = new Worker(
    spec.name,
    async () => {
      const latency = spec.avgLatency * (0.3 + Math.random() * 1.6)
      await sleep(latency)
      const failPct = SPIKE && spec.name === 'payment-processor' ? 0.6 : spec.failPct
      if (Math.random() < failPct) {
        throw new Error(pick(spec.errors))
      }
      return { ok: true }
    },
    { connection },
  )
  worker.on('error', () => {}) // never let worker errors crash the harness
  workers.push(worker)
}

let produced = 0
const producer = setInterval(async () => {
  const spec = pick(specs)
  const queue = queues[specs.indexOf(spec)]
  try {
    await queue.add(
      spec.jobName,
      { at: new Date().toISOString() },
      { attempts: 3, backoff: { type: 'fixed', delay: 500 }, removeOnComplete: 1000, removeOnFail: 1000 },
    )
    produced++
    if (produced % 25 === 0) console.log(`  …produced ${produced} jobs`)
  } catch (err) {
    console.error('enqueue failed (is Redis up?):', (err as Error).message)
  }
}, RATE_MS)

console.log('────────────────────────────────────────────────────────')
console.log('  PipeRadar BullMQ harness running')
console.log(`  api      : ${API_URL}`)
console.log(`  redis    : ${connection.host}:${connection.port}`)
console.log(`  queues   : ${specs.map((s) => s.name).join(', ')}`)
console.log(`  rate     : 1 job every ${RATE_MS}ms${SPIKE ? '   (SPIKE mode)' : ''}`)
console.log('  Ctrl-C to stop. Watch the dashboard for live queue health.')
console.log('────────────────────────────────────────────────────────')

let shuttingDown = false
async function shutdown() {
  if (shuttingDown) return
  shuttingDown = true
  console.log('\nshutting down — flushing events…')
  clearInterval(producer)
  await Promise.all(workers.map((w) => w.close().catch(() => {})))
  await pr.destroy() // closes QueueEvents listeners and flushes the buffer
  await Promise.all(queues.map((q) => q.close().catch(() => {})))
  console.log(`done. produced ${produced} jobs.`)
  process.exit(0)
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)
