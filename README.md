# @piperadar/bullmq

Drop-in pipeline failure & latency monitoring for [BullMQ](https://docs.bullmq.io/)
queues. Add one line to your worker process and PipeRadar tracks job failures,
retries, and latency — surfacing incidents, failure groups, and alerts in your
dashboard.

It hooks BullMQ's **native `QueueEvents`**, so there is zero overhead inside your
job-processing path. Monitoring must never break the host app, so every failure
path in the SDK is swallowed.

## Install

```bash
npm install @piperadar/bullmq
```

`bullmq` is a peer dependency (`>=3.0.0`) — you already have it.

## What PipeRadar collects

> **PipeRadar never sends your job payloads.** It observes BullMQ's native queue
> events — it never reads `job.data`, return values, your Redis, or your
> environment. Turn on transparency mode (`DEBUG=piperadar`) to watch the exact
> bytes leave your process.

| ✅ Sent to PipeRadar | ❌ Never sent |
|---|---|
| Queue name | Job data / payload / arguments |
| Job name & job ID | Return values / results |
| Status (`completed` / `failed` / `retrying`) | Redis contents or credentials |
| Latency (ms) | Environment variables |
| Attempt / retry count | Secrets, tokens, API keys |
| Timestamp | Customer objects / PII |
| `environment` (e.g. `production`) & optional `service` name | Raw error text (unless you opt in) |
| SDK version (`sdk_version`) | |
| Error message — **scrubbed by default** (see below) | |

**On job IDs and queue names:** job IDs *are* sent (they let you find a specific
failure in the dashboard). Queue and job **names** are sent verbatim, so don't
encode PII in them — name a queue `send-invoice`, not `invoice-john@acme.com`.

## Quick start

```ts
import { Queue } from 'bullmq'
import { PipeRadar } from '@piperadar/bullmq'

const emailQueue = new Queue('email')
const paymentQueue = new Queue('payments')

const pr = PipeRadar({ apiKey: process.env.PIPERADAR_API_KEY! })
pr.watch(emailQueue)
pr.watch(paymentQueue)

// On graceful shutdown, flush buffered events and stop listeners:
process.on('SIGTERM', () => pr.destroy())
```

That's it. `watch()` is idempotent per queue, so calling it twice is harmless.

## Options

```ts
PipeRadar({
  apiKey: 'pr_live_...',     // required
  environment,               // tags every event (default: process.env.NODE_ENV)
  service,                   // service/app name, e.g. 'billing-worker' (optional)
  batchSize,                 // events buffered before a flush (default: 25)
  flushInterval,             // flush cadence in ms (default: 5000)
  maxBufferedEvents,         // retry-buffer cap during an outage (default: 1000)
  enabled,                   // set false to disable, e.g. in tests (default: true)
  errorMessages,             // 'scrub' (default) | 'raw' | 'off'  — see below
  errorTransformer,          // (raw) => string | undefined        — full control
  advanced: { apiUrl },      // ingestion base URL (default: https://piperadar.dev)
})
```

`environment` and `service` travel with every event so you can tell a
`production` failure from a `staging` one, or attribute events when several
services share a queue. Each event also carries the SDK version (`sdk_version`)
for compatibility diagnostics — you don't set it.

> **`apiUrl` moved under `advanced`.** 99% of workers never point the SDK anywhere
> but production, so the base URL now lives in `advanced.apiUrl` to keep setup to a
> single `apiKey`. The old top-level `apiUrl` still works (deprecated) — pass
> `advanced.apiUrl` in new code.

## Flushing

Events are batched and flushed automatically, but in short-lived processes
(scripts, serverless handlers, tests) call `await pr.flush()` before you exit so
nothing buffered is lost. `flush()` never throws.

```ts
await pr.flush()   // force-send everything buffered, now
```

On a long-running worker, prefer `await pr.destroy()` on shutdown — it flushes
*and* closes the queue-event listeners.

## Error messages — safe by default

Developers throw errors like `` new Error(`User john@example.com failed payment`) ``
or `throw new Error(JSON.stringify(req.body))`, which can carry PII or secrets.
So PipeRadar **scrubs error text by default** before it ever leaves your process,
while keeping enough of the message intact for the backend to group identical
failures.

- **`errorMessages: 'scrub'`** _(default)_ — strips emails, JWTs, `Bearer`
  tokens, `sk_live_…`/`pr_test_…`-style keys, GitHub tokens, UUIDs, IPs, phone
  numbers, and long digit runs (card numbers, long IDs), replacing each with a
  `[redacted-…]` marker. `User john@example.com failed` → `User [redacted-email] failed`.
- **`errorMessages: 'raw'`** — send the message verbatim. Only if you're certain
  your errors never contain sensitive data.
- **`errorMessages: 'off'`** — never send error text at all. Failures still group
  by queue + job.
- **`errorTransformer: (raw) => string | undefined`** — full control. Return
  exactly what should be sent, or `undefined` to omit it. Overrides
  `errorMessages`; a throwing transformer is treated as "omit" and never breaks
  monitoring.

Messages are also capped at 500 characters.

## Transparency mode

Run your worker with `DEBUG=piperadar` (or `DEBUG=*`) and the SDK prints the exact
JSON body of every ingest request to stderr — so you can see for yourself that job
payloads never leave your infrastructure.

```bash
DEBUG=piperadar node worker.js
```

## How it behaves

- **Latency** is computed from BullMQ's `active` → `completed`/`failed`
  transitions. "Started" events are never sent — only terminal outcomes — which
  keeps your event quota low.
- **Retries vs. failures.** A failure that BullMQ will retry is reported as
  `retrying`; only the final attempt is a terminal `failed`.
- **Batching & at-least-once delivery.** Events are batched and flushed on an
  interval (or when a batch fills). Each batch carries a stable idempotency key
  that is **reused across retries**, so the backend dedupes a retried batch
  instead of double-counting it.
- **Resilient under outages.** Failed batches stay in a bounded in-memory retry
  buffer (`maxBufferedEvents`; oldest dropped when over cap). Permanent `4xx`
  rejections are dropped so a poison batch can't wedge the queue. (On-disk
  buffering across process restarts is future work.)
- **Graceful shutdown.** `destroy()` closes listeners and flushes what's buffered.

## Development

```bash
npm run build          # tsc → dist/
npm test               # node --test via tsx (no jest)
npm run example:basic  # smallest runnable example (examples/basic.ts)
npm run example:harness  # runnable integration example — flood/spike demo (examples/harness.ts)
```

The examples need a running Redis and a `PIPERADAR_API_KEY` from your dashboard:

```bash
PIPERADAR_API_KEY=pr_... npm run example:basic
```

## License

MIT
