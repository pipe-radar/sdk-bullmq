/**
 * Minimal runnable PipeRadar + BullMQ example — the README quick-start, wired up.
 *
 * This is the smallest thing that sends real events: two queues, a worker, and
 * PipeRadar watching. It needs a running Redis and an ingest key from your
 * PipeRadar dashboard.
 *
 *   PIPERADAR_API_KEY=pr_... npm run example:basic     # from sdk-bullmq/
 *
 * Stop with Ctrl-C — destroy() flushes buffered events and closes listeners.
 */

import { Queue, Worker } from 'bullmq'
import { PipeRadar } from '../src/index'

const apiKey = process.env.PIPERADAR_API_KEY
if (!apiKey) {
  console.error('PIPERADAR_API_KEY is required (get an ingest key from your PipeRadar dashboard)')
  process.exit(1)
}

const connection = { host: 'localhost', port: 6379, maxRetriesPerRequest: null as null }

const emailQueue = new Queue('email', { connection })
const paymentQueue = new Queue('payments', { connection })

// One line to start monitoring. watch() is idempotent per queue.
// The base URL lives under `advanced` and is unset by default (SDK falls back to
// prod); PIPERADAR_API_URL points it at a local backend for this example.
const pr = PipeRadar({
  apiKey,
  service: 'basic-example',
  advanced: { apiUrl: process.env.PIPERADAR_API_URL },
})
pr.watch(emailQueue)
pr.watch(paymentQueue)

// A worker so there is something to observe — half the payments fail on purpose.
const emailWorker = new Worker('email', async () => ({ ok: true }), { connection })
const paymentWorker = new Worker(
  'payments',
  async () => {
    if (Math.random() < 0.5) throw new Error('card_declined')
    return { ok: true }
  },
  { connection },
)

async function main() {
  await emailQueue.add('welcome', { at: new Date().toISOString() })
  await paymentQueue.add('charge', { at: new Date().toISOString() }, { attempts: 2 })

  console.log('sent 2 jobs — watch them land in your PipeRadar dashboard. Ctrl-C to stop.')
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})

// On graceful shutdown, flush buffered events and stop listeners.
async function shutdown() {
  await Promise.all([emailWorker.close(), paymentWorker.close()])
  await pr.destroy()
  await Promise.all([emailQueue.close(), paymentQueue.close()])
  process.exit(0)
}
process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)
