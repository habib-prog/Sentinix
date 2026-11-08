<div align="center">

<p align="center">
  <img
    src="./public/Sentinix.png"
    alt="Sentinix - Monitor Trace Stay Ahead"
    width="100%"
  />
</p>


### High-Throughput Distributed Tracing & Centralized Log Aggregator for Modern Microservices

Find the slow span. Read the exact log line. Fix it in minutes, not hours.

![Node.js](https://img.shields.io/badge/Node.js-20%2B-339933?logo=nodedotjs&logoColor=white)
![Express](https://img.shields.io/badge/Express-4.x-000000?logo=express&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Prisma](https://img.shields.io/badge/Prisma-ORM-2D3748?logo=prisma&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-7%20Streams-DC382D?logo=redis&logoColor=white)
![React](https://img.shields.io/badge/React-Vite%20%2B%20Tailwind-61DAFB?logo=react&logoColor=black)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![W3C Trace Context](https://img.shields.io/badge/W3C-Trace%20Context-005A9C)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

</div>

---

## Table of Contents

1. [The Problem It Solves](#1-the-problem-it-solves)
2. [How Sentinix Works](#2-how-sentinix-works)
3. [System Architecture](#3-system-architecture)
4. [Database Schema (ERD)](#4-database-schema-erd)
5. [Getting Started / Local Setup](#5-getting-started--local-setup)
6. [Express SDK Usage](#6-express-sdk-usage)
7. [Roadmap & RAG Integration](#7-roadmap--rag-integration)
8. [Interview-Ready Talking Points](#8-interview-ready-talking-points)
9. [Repository Layout](#9-repository-layout)
10. [Contributing & License](#10-contributing--license)

---

## 1. The Problem It Solves

Modern backends are no longer one process. A single `POST /checkout` can touch an auth service, PostgreSQL, Redis, and a third-party payment API. When that request becomes slow or fails, the evidence is scattered:

- Logs live in **PM2 files, container stdout, or `docker logs`**, one stream per service, with no shared request identity.
- Latency is reported as one number (`842 ms`) with **no breakdown of where the time went**.
- Engineers `grep` timestamps across machines and guess at causality, which is slow and error-prone.

This inflates **MTTD** (Mean Time To Detect) and **MTTR** (Mean Time To Resolve).

### Before vs. After (illustrative scenario)

| Step | ❌ Without Sentinix | ✅ With Sentinix |
|---|---|---|
| **Detect** | A customer complains "checkout is slow" | Dashboard filter: `status = ERROR` or `duration > 500 ms` surfaces the trace |
| **Locate** | SSH into 4 hosts, `pm2 logs`, `grep` by approximate timestamp | Open one trace, see the full **latency waterfall** across all services |
| **Correlate** | Guess which log line belongs to which request | Every log carries `trace_id` + `span_id`, so the trace view shows exact logs per span |
| **Diagnose** | Reproduce locally, add `console.log`, redeploy | The `stripe-api` span took 610 ms and the log shows `ETIMEDOUT` |
| **Time spent** | ~45–90 minutes | ~2–5 minutes |

> The timings above illustrate the class of improvement a trace-first workflow gives; they are not benchmark results.

---

## 2. How Sentinix Works

Sentinix follows the [W3C Trace Context](https://www.w3.org/TR/trace-context/) standard. Every request gets a **trace** (the whole journey) made of **spans** (individual units of work: an HTTP handler, a DB query, a cache call, a third-party request). Spans form a tree via parent/child IDs.

### The `traceparent` header

```text
traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
             ││ └────────── trace_id (32 hex) ──┘ └ span_id (16 hex) ┘ └ flags (sampled)
             └ version
```

### Request lifecycle

```text
 Client / Browser
       │  POST /api/checkout            (no traceparent -> SDK generates trace_id + root span_id)
       ▼
 ┌─────────────────────────────────────────────────────────────────────┐
 │ checkout-api  (Sentinix SDK middleware)                             │
 │  1. extract or generate traceparent                                 │
 │  2. start root SERVER span, start timer                             │
 │  3. propagate traceparent to downstream calls ────────────┐         │
 │  4. res.on('finish') -> end span, record status + duration│         │
 │  5. enqueue span in memory buffer (non-blocking)          │         │
 └───────────────────────────────────────────────────────────┼─────────┘
                                                             ▼
                          auth-service ──► postgres ──► redis ──► stripe-api
                          (child spans share the same trace_id; each has its own span_id)

 Background flush (every N ms or M items, gzip, async):
 SDK buffer ──► POST /api/v1/telemetry/ingest ──► 202 Accepted
```

### Latency waterfall (what you see in the dashboard)

```text
Trace 4bf92f3577b34da6a3ce929d0e0e4736   POST /api/checkout   total 842 ms   HTTP 500
                                    0 ms      200       400       600       800
 checkout-api  POST /api/checkout   |████████████████████████████████████████| 842 ms
  ├─ auth-service  GET /verify      |██                                      |  38 ms
  ├─ postgres      SELECT orders    |  ██████                                | 120 ms
  ├─ redis         GET cart:8812    |       ▌                                |   4 ms
  └─ stripe-api    POST /v1/charges |        █████████████████████████████   | 610 ms  ◄ bottleneck
                                                 └─ ERROR log: "ETIMEDOUT connecting to api.stripe.com"
```

Span types captured: **HTTP server**, **database** (PostgreSQL/Prisma), **cache** (Redis), **third-party HTTP calls**, and **custom internal spans**.

---

## 3. System Architecture

### 3.1 High-level diagram

```text
 ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐
 │ Service A          │  │ Service B          │  │ Service N          │
 │ Express + SDK      │  │ Express + SDK      │  │ Express + SDK      │
 │ (in-memory buffer) │  │ (in-memory buffer) │  │ (in-memory buffer) │
 └─────────┬──────────┘  └─────────┬──────────┘  └─────────┬──────────┘
           │   batched, gzipped    │                       │
           └───────────────────────┼───────────────────────┘
                                   │  POST /api/v1/telemetry/ingest   (x-api-key)
                                   ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │ INGESTION GATEWAY (Express, stateless, horizontally scalable)        │
 │  1. Validate payload schema + size limit                             │
 │  2. Authenticate API key  ──► Redis cache (miss ──► PostgreSQL)      │
 │  3. Token-bucket rate limit (atomic Lua script, per API key)         │
 │  4. XADD envelope to Redis Stream                                    │
 │  5. Return 202 Accepted immediately                                  │
 └───────────────────────────────────┬──────────────────────────────────┘
                                     ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │ REDIS 7                                                              │
 │  • sentinix:ingest        (Stream + consumer group "workers")        │
 │  • sentinix:ingest:dlq    (dead-letter stream for poison messages)   │
 │  • sentinix:apikey:<hash> (API key cache, TTL)                       │
 │  • sentinix:rl:<keyId>    (token bucket state)                       │
 └───────────────────────────────────┬──────────────────────────────────┘
                                     │ XREADGROUP (batch), XACK after commit
                                     ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │ QUEUE WORKER (N replicas, same consumer group)                       │
 │  • Pull batch (e.g. 500 entries / 2 s block)                         │
 │  • Normalize, enrich, chunk (e.g. 1,000 rows)                        │
 │  • Single transaction: createMany({ skipDuplicates: true })          │
 │  • XACK on commit · retry w/ backoff · XAUTOCLAIM stale · DLQ        │
 └───────────────────────────────────┬──────────────────────────────────┘
                                     ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │ POSTGRESQL 16  (via Prisma ORM)                                      │
 │  Organization → Project → Service → Trace → Span → Log               │
 │  B-Tree (FKs, timestamps) · GIN on JSONB · time-partitioned tables   │
 └───────────────────────────────────┬──────────────────────────────────┘
                                     ▲
 ┌───────────────────────────────────┴──────────────────────────────────┐
 │ QUERY API (Express, read-only paths, tenant-scoped, cursor-paginated)│
 └───────────────────────────────────┬──────────────────────────────────┘
                                     ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │ REACT DASHBOARD (Vite + Tailwind): trace search, waterfall, log view │
 └──────────────────────────────────────────────────────────────────────┘
```

### 3.2 System design decisions

#### A. Asynchronous ingestion with `202 Accepted`
The gateway does the minimum work needed to *safely accept* data (validate, authenticate, rate-limit, enqueue) and returns `202`. Database latency, lock contention, and vacuum pauses never reach the instrumented application. This decouples **ingest availability** from **storage availability**: PostgreSQL can be briefly unavailable while the stream buffers data.

#### B. Redis Streams as the buffer (not a plain list)
| Requirement | Why Streams |
|---|---|
| Don't lose data when a worker crashes mid-batch | Consumer groups keep a **Pending Entries List (PEL)**; unacked entries are reclaimed with `XAUTOCLAIM` |
| Scale workers horizontally | Multiple workers join one consumer group; each entry goes to exactly one consumer |
| Bounded memory | `XADD ... MAXLEN ~ N` caps the stream; overflow triggers backpressure (see below) |
| Poison messages | Entries exceeding a delivery-count threshold are moved to `sentinix:ingest:dlq` |

#### C. Batched writes into PostgreSQL
Row-at-a-time inserts are the dominant cost in write-heavy telemetry. The worker reads a batch, chunks it, and issues **multi-row `createMany`** statements inside one transaction, which amortizes round trips, WAL flushes, and index maintenance. Batch size and block time are env-tunable (`WORKER_BATCH_SIZE`, `WORKER_BLOCK_MS`).

#### D. At-least-once delivery + idempotent writes
The worker `XACK`s **only after the transaction commits**. A crash between commit and ack causes redelivery, so writes are idempotent: spans are unique on `(traceId, spanId, startTime)` and inserted with `skipDuplicates: true`. At-least-once delivery plus idempotent writes gives **effectively-once** persistence.

#### E. Token-bucket rate limiting (per API key)
A Lua script runs atomically in Redis: refill tokens by elapsed time, subtract the request cost (number of spans + logs), and return allow/deny with `Retry-After`. Properties:
- **Burst-friendly**: a service that was idle can flush a large batch.
- **Atomic and distributed**: correct across many gateway replicas with no extra locking.
- **Cost-based**: a 500-span batch costs more than a 5-span batch.

#### F. API key authentication via Redis cache
Keys are stored as **SHA-256 hashes** (plaintext shown once at creation). The hot path resolves `hash → { orgId, projectId, rateLimit, revoked }` from Redis (TTL-bound, with short negative caching for invalid keys). PostgreSQL is only consulted on cache miss, and revocation deletes the cache entry.

#### G. Backpressure and failure behavior
| Condition | Gateway response | SDK behavior |
|---|---|---|
| Invalid / revoked key | `401` | Disable flushing, log once |
| Rate limit exceeded | `429` + `Retry-After` | Back off, keep buffering (bounded) |
| Payload too large / invalid | `413` / `400` | Drop batch, log once |
| Redis unavailable or stream full | `503` + `Retry-After` | Retry with exponential backoff + jitter |
| Gateway unreachable | n/a | Bounded in-memory buffer, **drop oldest** when full; never block the app |

#### H. JSONB + GIN indexing
Free-form attributes (`http.route`, `db.statement`, `user.tier`, ...) and log metadata are stored as **JSONB** so new attributes need no migrations. GIN indexes (`jsonb_path_ops`) make containment queries such as `metadata @> '{"userId":"42"}'` fast without a column per attribute.

#### I. Time-based table partitioning
Spans and logs are append-only and time-queried. They are **range-partitioned by `startTime` / `timestamp` (daily)**:
- Queries with a time window get **partition pruning**.
- Indexes stay small per partition, which keeps inserts cheap.
- **Retention is `DROP PARTITION`** (instant, no bloat), not `DELETE` plus vacuum.

PostgreSQL requires the partition key to be part of every primary/unique key, so spans use `(id, startTime)` and the idempotency key includes `startTime`. Prisma does not manage partitions natively, so they are defined in a **raw SQL migration**; the Prisma models remain the typed access layer.

### 3.3 Ingestion API reference

**`POST /api/v1/telemetry/ingest`**

Headers: `x-api-key: <key>`, `content-type: application/json`, optional `content-encoding: gzip`.

```json
{
  "service": { "name": "checkout-api", "version": "1.14.2", "environment": "production" },
  "spans": [
    {
      "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
      "spanId": "00f067aa0ba902b7",
      "parentSpanId": null,
      "name": "POST /api/checkout",
      "kind": "SERVER",
      "startTime": "2026-11-08T10:15:00.120Z",
      "durationMs": 842,
      "status": "ERROR",
      "attributes": { "http.method": "POST", "http.route": "/api/checkout", "http.status_code": 500 }
    }
  ],
  "logs": [
    {
      "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
      "spanId": "00f067aa0ba902b7",
      "level": "ERROR",
      "message": "ETIMEDOUT connecting to api.stripe.com",
      "timestamp": "2026-11-08T10:15:00.940Z",
      "metadata": { "userId": "42", "stack": "Error: ETIMEDOUT\n    at ..." }
    }
  ]
}
```

| Status | Meaning |
|---|---|
| `202 Accepted` | Enqueued: `{ "accepted": { "spans": 1, "logs": 1 }, "requestId": "..." }` |
| `400 Bad Request` | Schema validation failed (field-level errors returned) |
| `401 Unauthorized` | Missing, unknown, or revoked API key |
| `413 Payload Too Large` | Exceeds batch/body limit |
| `429 Too Many Requests` | Token bucket empty; `Retry-After` header set |
| `503 Service Unavailable` | Queue unavailable or full; `Retry-After` header set |

**Query API (read side, tenant-scoped, cursor-paginated)**

| Method & Path | Purpose |
|---|---|
| `GET /api/v1/traces` | Search by service, status, time range, `minDurationMs` |
| `GET /api/v1/traces/:traceId` | Full span tree + logs for one trace |
| `GET /api/v1/logs` | Filter by level, text, and JSONB metadata containment |
| `GET /api/v1/services` | Services with throughput, error rate, p50/p95/p99 |

---

## 4. Database Schema (ERD)

### 4.1 Entity-relationship model

```text
 ┌──────────────┐ 1      N ┌──────────────┐ 1      N ┌──────────────┐
 │ Organization │──────────│   Project    │──────────│    ApiKey    │
 │ id, name     │          │ id, orgId    │          │ id, projectId│
 │ slug         │          │ name, slug   │          │ keyHash, rate│
 └──────────────┘          └──────┬───────┘          │ revokedAt    │
                                   │ 1                └──────────────┘
                                   │
                                   │ N
                            ┌──────▼───────┐
                            │   Service    │  unique(projectId, name, environment)
                            │ id, projectId│
                            │ name, env    │
                            └──────┬───────┘
                                   │ 1
                                   │ N
                            ┌──────▼───────┐
                            │    Trace     │  unique(projectId, traceId)
                            │ id, traceId  │  startTime, durationMs, status
                            │ serviceId    │
                            └──────┬───────┘
                                   │ 1
                                   │ N
                  ┌────────────────▼───────────────┐
                  │             Span               │◄──┐
                  │ id, traceId, spanId            │   │ parentSpanId
                  │ parentSpanId, name, kind       │───┘ (self-reference:
                  │ startTime, durationMs, status  │      parent → children)
                  │ attributes JSONB  (GIN)        │
                  └────────────────┬───────────────┘
                                   │ 1
                                   │ N
                            ┌──────▼───────┐
                            │     Log      │  level, message, timestamp
                            │ spanId?      │  metadata JSONB (GIN)
                            │ traceId?     │
                            └──────────────┘
```

Cardinalities: `Organization 1—N Project`, `Project 1—N ApiKey`, `Project 1—N Service`, `Service 1—N Trace`, `Trace 1—N Span`, `Span 1—N Log` (logs may also attach to a trace without a span).

### 4.2 Technical rationale

**Multi-tenancy.** Every row is reachable from an `Organization` through `Project`. `projectId` is also **denormalized onto `Trace`, `Span`, and `Log`**, so tenant-scoped queries and partition pruning never need a multi-table join. All Query API handlers derive `projectId` from the authenticated principal, never from request input.

**Cascading rules.** `Organization → Project → (ApiKey, Service) → Trace → (Span, Log)` use `onDelete: Cascade`, so deleting a project atomically removes its data without orphaned rows.

**Parent-child span tree.** Spans store `parentSpanId`, and the tree is reconstructed in memory (`O(n)` over spans of one trace, typically tens to hundreds). `parentSpanId` is **not** a hard foreign key: spans arrive out of order and from different services, so a child often lands before its parent. Referential integrity is enforced logically, and orphans are rendered as roots.

**Index strategy**

| Index | Type | Serves |
|---|---|---|
| FK columns (`projectId`, `serviceId`, `traceId`) | B-Tree | Tenant scoping, joins, trace lookups |
| `(projectId, startTime DESC)` on `Trace` | B-Tree | "Latest traces" and time-range search |
| `(projectId, status, startTime DESC)` on `Trace` | B-Tree | "Recent errors" |
| `(traceId, startTime)` on `Span` | B-Tree | Fetch all spans of a trace |
| `(projectId, level, timestamp DESC)` on `Log` | B-Tree | Log explorer filters |
| `attributes`, `metadata` | **GIN (`jsonb_path_ops`)** | JSONB containment (`@>`) filters |
| `keyHash` on `ApiKey` | Unique B-Tree | Auth cache-miss lookup |

Indexes are kept deliberately few on the hot write tables, because every extra index taxes ingest throughput.

---

## 5. Getting Started / Local Setup

### Prerequisites

- **Node.js 20+** and npm 10+
- **Docker** and Docker Compose v2 (runs PostgreSQL 16 and Redis 7)
- Git

### Step-by-step

```bash
# 1. Clone
git clone https://github.com/jamesxavier/sentinix.git
cd sentinix

# 2. Start infrastructure (PostgreSQL + Redis)
docker compose up -d postgres redis

# 3. Configure environment
cp server/.env.example server/.env

# 4. Install dependencies (npm workspaces: server, sdk, client)
npm install

# 5. Apply database migrations (includes the partitioning SQL) and generate the Prisma client
cd server
npx prisma migrate dev
npx prisma generate

# 6. Seed a demo Organization, Project and API key (key is printed ONCE)
npm run seed

# 7. Run the processes (three terminals, or `npm run dev` at the repo root)
npm run dev:gateway    # Ingestion Gateway + Query API -> http://localhost:4000
npm run dev:worker     # Queue worker
npm run dev:client     # React dashboard (from /client) -> http://localhost:5173
```

### `.env` sample (`server/.env`)

```dotenv
# --- Runtime ---
NODE_ENV=development
PORT=4000
LOG_LEVEL=info

# --- PostgreSQL ---
DATABASE_URL=postgresql://sentinix:sentinix@localhost:5432/sentinix?schema=public

# --- Redis ---
REDIS_URL=redis://localhost:6379
INGEST_STREAM_KEY=sentinix:ingest
INGEST_DLQ_KEY=sentinix:ingest:dlq
INGEST_STREAM_MAXLEN=1000000

# --- Ingestion limits ---
INGEST_MAX_BODY_BYTES=1048576
INGEST_MAX_ITEMS_PER_REQUEST=1000
API_KEY_CACHE_TTL_SECONDS=300

# --- Token bucket defaults (per API key) ---
RATE_LIMIT_CAPACITY=5000
RATE_LIMIT_REFILL_PER_SECOND=1000

# --- Worker ---
WORKER_CONSUMER_GROUP=workers
WORKER_BATCH_SIZE=500
WORKER_BLOCK_MS=2000
WORKER_INSERT_CHUNK_SIZE=1000
WORKER_MAX_DELIVERIES=5
WORKER_CLAIM_IDLE_MS=60000

# --- Query API auth ---
JWT_SECRET=change-me-to-a-long-random-string
```

### Run everything in Docker

```bash
docker compose up --build
# gateway:   http://localhost:4000/healthz
# dashboard: http://localhost:5173
```

### Verify the pipeline end-to-end

```bash
curl -i -X POST http://localhost:4000/api/v1/telemetry/ingest \
  -H "content-type: application/json" \
  -H "x-api-key: $SENTINIX_API_KEY" \
  -d '{
    "service": { "name": "demo-api", "version": "0.0.1", "environment": "development" },
    "spans": [{
      "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
      "spanId": "00f067aa0ba902b7",
      "parentSpanId": null,
      "name": "GET /health",
      "kind": "SERVER",
      "startTime": "2026-11-08T10:15:00.000Z",
      "durationMs": 12,
      "status": "OK",
      "attributes": { "http.method": "GET" }
    }],
    "logs": []
  }'
# Expected: HTTP/1.1 202 Accepted
```

---

## 6. Express SDK Usage

```bash
npm install @sentinix/node-sdk
```

```js
import express from 'express';
import { sentinix } from '@sentinix/node-sdk';

const app = express();

app.use(sentinix({
  apiKey: process.env.SENTINIX_API_KEY,
  serviceName: 'checkout-api',
  endpoint: 'http://localhost:4000',
}));

app.get('/api/checkout', async (req, res) => {
  // Optional: wrap downstream work in a child span
  const order = await req.sentinix.span('postgres.query', () => db.order.findFirst());
  res.json(order);
});

app.use(sentinix.errorHandler()); // register AFTER routes to capture unhandled errors
app.listen(3000);
```

**What the middleware does automatically**
- Reads an incoming `traceparent` header or generates a new `trace_id` / `span_id`
- Measures request duration with a monotonic clock (`process.hrtime.bigint()`)
- Captures unhandled errors (message, stack, status) and attaches them to the span
- Propagates `traceparent` to downstream calls
- Buffers in memory and **flushes asynchronously** (size/time triggers, gzip, retry with backoff + jitter)

**Safety guarantees**
- Never blocks or delays the user's response
- Bounded buffer: **drops oldest data** instead of growing memory
- Network failures and `4xx/5xx` from the collector are swallowed and logged once
- Flush timers are `unref()`'d and a final flush runs on `SIGTERM`

---

## 7. Roadmap & RAG Integration

### ✅ MVP (current scope)

- [x] Multi-tenant model: Organization → Project → Service → Trace → Span → Log
- [x] `POST /api/v1/telemetry/ingest` with schema validation, `202 Accepted`
- [x] API key auth (hashed keys, Redis cache)
- [x] Token-bucket rate limiting (atomic Lua, per key)
- [x] Redis Streams buffer with consumer groups, retries, DLQ
- [x] Batched, idempotent bulk inserts via Prisma
- [x] JSONB + GIN indexing for attributes and log metadata
- [x] Express SDK: W3C `traceparent`, duration capture, error capture, async batched flush
- [x] React dashboard: trace search, latency waterfall, correlated logs
- [x] Docker Compose local environment

### 🚧 Next (platform hardening)

- [ ] Automated partition creation and retention (drop-partition job)
- [ ] Head-based and tail-based sampling
- [ ] Latency percentiles (p50/p95/p99) via rollup tables
- [ ] Alert rules (error rate, latency SLO burn) with webhook/Slack delivery
- [ ] OpenTelemetry (OTLP) ingestion endpoint

### 🤖 AI Incident Diagnostic Engine (RAG, `pgvector`)

```text
 New ERROR trace
      │
      ▼
 Normalize stack trace ──► fingerprint (strip paths, line numbers, IDs)
      │
      ▼
 Embed (stack + message + top spans) ──► vector(1536) in PostgreSQL (pgvector, HNSW)
      │
      ▼
 Retrieve: nearest past incidents + their resolutions + related trace context
      │
      ▼
 LLM: summarize probable root cause, cite evidence (span IDs, log lines), suggest next step
      │
      ▼
 Conversational debugging in the dashboard ("Why did p99 spike after the 14:05 deploy?")
```

- [ ] `ErrorFingerprint` + `Embedding` tables with `vector` columns and HNSW index
- [ ] Embedding worker consuming new error traces from a second Redis Stream
- [ ] Similar-incident search (cosine distance, filtered by project)
- [ ] LLM root-cause summaries with **mandatory citations** to spans/logs (grounded, no free-form guessing)
- [ ] Conversational debugging with tool-calling over the Query API
- [ ] Secret/PII redaction before any text leaves the system or is embedded
- [ ] Human feedback loop ("was this helpful?") for evaluation and re-ranking

---

## 8. Interview-Ready Talking Points

### Why PostgreSQL instead of a NoSQL or columnar store?
- The domain is **inherently relational** (tenant → project → service → trace → span → log); constraints and cascades give integrity for free.
- **JSONB + GIN** provides schema flexibility for attributes without leaving SQL.
- **One datastore** covers OLTP metadata, time-series-like telemetry, and (later) vector search via `pgvector`, so operational complexity stays low.
- **Honest trade-off:** at very high volume, a columnar engine (ClickHouse) is better for analytical scans. The worker is the only writer, so storage can be swapped behind it. Postgres is the right choice up to a large scale and for a single-binary operational story.

### How is write-heavy load optimized?
1. Async `202` ingestion keeps client latency independent of the database.
2. Redis Streams absorb bursts and smooth them into steady batch writes.
3. Multi-row `createMany` in one transaction amortizes round trips and WAL flushes.
4. Few indexes on hot tables; GIN with `jsonb_path_ops` (smaller, faster to maintain).
5. Daily time partitions keep indexes small and make retention a metadata operation.
6. Horizontal scaling: gateways are stateless; workers scale via the consumer group.

### How is delivery guaranteed?
- **At-least-once:** `XACK` only after commit; unacked entries are reclaimed with `XAUTOCLAIM`.
- **Idempotent writes:** unique key `(traceId, spanId, startTime)` + `skipDuplicates` → effectively-once persistence.
- **Poison messages:** delivery-count cap, then DLQ, so one bad batch cannot stall a partition of work.
- **Known gap:** data buffered only in SDK memory is lost if the host process is killed. This is a deliberate trade-off against app availability, mitigated by `SIGTERM` flush.

### What happens under overload?
Explicit **backpressure, not silent loss**: `429` (per-key limit) and `503` (queue full) with `Retry-After`; the SDK backs off with jitter and drops oldest data from a bounded buffer rather than growing memory or blocking requests.

### Why not make `parentSpanId` a foreign key?
Spans from different services arrive out of order, so a child frequently arrives before its parent. A hard FK would reject valid data or force ordering. Trade-off: integrity is logical, and the UI treats orphans as roots.

### Why Redis Streams instead of Kafka/RabbitMQ?
Redis is already required for caching and rate limiting. Streams give consumer groups, acknowledgments, and replay semantics with far less operational weight. Trade-off: durability and retention are weaker than Kafka; the ingest layer is isolated so a Kafka-backed implementation could replace it.

### How is tenant isolation enforced?
Tenant identity comes **only** from the authenticated API key or JWT, never from request parameters. `projectId` is denormalized on every telemetry table and is the leading column of the main indexes, so isolation and performance reinforce each other.

### Known limitations
- No sampling in the MVP (all traces are stored).
- Clock skew between services can distort waterfalls; spans use each host's wall clock for start times and a monotonic clock for durations.
- High-cardinality attributes can bloat GIN indexes; limits on attribute count and size are enforced at validation.
- Prisma lacks native partition management; partitions live in raw SQL migrations.

### Design targets (to validate with load tests, not measured claims)

| Metric | Target |
|---|---|
| Gateway `202` latency (p99) | < 20 ms |
| Sustained ingest per worker | 5k–10k rows/s |
| Trace detail query | < 100 ms for traces under 1k spans |
| SDK overhead per request | < 1 ms synchronous work |

---

## 9. Repository Layout

```text
sentinix/
├── docker-compose.yml
├── package.json                  # npm workspaces: server, sdk, client
├── README.md
├── server/
│   ├── prisma/
│   │   ├── schema.prisma
│   │   ├── migrations/           # includes raw SQL for partitions
│   │   └── seed.js
│   ├── src/
│   │   ├── gateway/              # ingest route, auth, rate-limit middleware, Lua scripts
│   │   ├── worker/               # stream consumer, batching, DLQ, reclaim loop
│   │   ├── api/                  # query API routes (traces, logs, services)
│   │   ├── lib/                  # prisma client, redis client, logger, config (env validation)
│   │   └── index.js
│   ├── .env.example
│   └── Dockerfile
├── sdk/                          # @sentinix/node-sdk
│   ├── src/                      # middleware, traceparent, buffer, transport, error handler
│   └── package.json
└── client/                       # React (Vite) + Tailwind dashboard
    ├── src/                      # pages: Traces, TraceDetail (waterfall), Logs, Services
    └── package.json
```

---

## 10. Contributing & License

Contributions are welcome. Open an issue to discuss significant changes, keep pull requests focused, and include tests for behavior changes.

Licensed under the **MIT License**. See [`LICENSE`](./LICENSE).

<div align="center">

Built to make production debugging boring. 🛰️

</div>
