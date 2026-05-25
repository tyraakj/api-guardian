# API Guardian

> A production-hardened API gateway built in Spring Boot — non-blocking reverse proxy, JWT auth, token bucket rate limiting, async Kafka event streaming, real-time SSE dashboard, and S3 report archival.

---

## What It Does

Most API gateways are black boxes. API Guardian is fully transparent — every layer is readable, debuggable, and intentional. It sits in front of any HTTP service and adds:

- **Non-blocking reverse proxy** — WebClient forwarding with hard connect + read timeouts. The target service can never hang the gateway.
- **JWT authentication** — signed tokens, 24-hour expiry. Every developer gets their own account. Invite-only registration — only an authenticated admin can create new accounts.
- **Rate limiting** — Bucket4j token bucket per client IP, stored in Redis. Burst-friendly, realistic behavior vs a fixed window.
- **Async event streaming** — every request published to Kafka without blocking the request thread. Fire-and-forget with error callback.
- **Idempotent analytics** — UUID per event, Redis SET NX deduplication. Replaying the Kafka topic never corrupts counters.
- **Real-time dashboard** — Server-Sent Events push to React frontend. No polling, no wasted requests.
- **S3 report export** — daily analytics archived to S3 with Spring Retry (3 attempts, exponential backoff) and a Kafka dead letter queue on final failure.
- **Resilience** — Resilience4j circuit breakers on Redis and S3. If Redis dies, the gateway fails open. If S3 fails, events go to a DLQ.

---

## Why Not Just Use Nginx?

Nginx is a config file. API Guardian is code.

You can see exactly how rate limiting works, trace how a request event flows from the proxy through Kafka into Redis counters, inspect how the circuit breaker decides to fail open, and step through the S3 export retry logic in a debugger. That observability and extensibility is the point — this is built to be understood, not just configured.

---

## Architecture

```
Client Request
      ↓
┌────────────────────────────────────────┐
│  JwtAuthFilter                         │  ← validates Bearer token on /analytics/**
│  SecurityConfig (Spring Security)      │  ← /auth/** open, /analytics/** protected
│    └── UserDetailsServiceImpl          │  ← loads user from Supabase (Postgres)
└──────────────┬─────────────────────────┘
               │
               ▼
┌────────────────────────────────────────┐
│  ProxyFilter (OncePerRequestFilter)    │
│  ContentCachingRequestWrapper          │  ← body preserved for downstream reads
│  RateLimitService                      │  ← Bucket4j token bucket per client IP
│    └── CircuitBreaker (Resilience4j)   │  ← Redis down → fail open, allow request
└──────────────┬─────────────────────────┘
               │
               ▼
┌────────────────────────────────────────┐
│  ProxyForwarder (WebClient)            │
│  connectTimeout: 2s                    │  ← TCP handshake deadline
│  readTimeout: 5s                       │  ← response body deadline → 504
└──────────────┬─────────────────────────┘
               │
        ┌──────┴───────┐
        ▼              ▼
  Response         RequestEvent (UUID)
  to client        → Kafka: api-requests
                         ↓
                   RequestEventConsumer
                   Redis SET NX (dedup)   ← idempotent — replay never corrupts
                         ↓
                   AnalyticsService
                   (Redis counters)
                         ↓
                ┌────────┴────────┐
                ▼                 ▼
        REST /analytics/*    SSE /analytics/stream
        (JWT required)       (push to dashboard)
                                  ↓
                            React Dashboard
                            (EventSource hook)
                                  ↓
                         ReportExporter (@Scheduled)
                         Spring Retry (3 attempts)
                         DLQ on final failure
                                  ↓
                         AWS S3 — reports/2024-01-15.json
                         Presigned URL (24h TTL)

External dependencies:
  Supabase (hosted Postgres) ← developer accounts, bcrypt password hashes
  Redis (Docker)             ← rate limit buckets, analytics counters, dedup keys
  Kafka (Docker, KRaft)      ← request event stream, DLQ
  AWS S3                     ← cold report archive
```

---

## Auth Flow

```
Admin (you)
  → POST /auth/register  (requires your JWT)   ← invite-only, no open registration
  → creates developer account in Supabase

Developer
  → POST /auth/login                           ← returns signed JWT, 24h expiry
  → GET  /analytics/summary
      Authorization: Bearer eyJhbGci...        ← validated on every analytics request

Admin removing access
  → DELETE /auth/users/{id}  (requires your JWT)  ← revokes account immediately
```

No roles, no permissions matrix — every authenticated developer sees the same analytics. Access is binary: you have an account or you don't.

---

## Layer-by-Layer Breakdown

### Layer 1 — Infrastructure scaffold

The foundation everything else runs on.

- **Kafka in KRaft mode** — no Zookeeper dependency. Retention set to 7 days so events are replayable. Flush interval set to 1 — every message hits disk immediately, no data loss on crash.
- **Redis 7** — used for three completely separate things: rate limit buckets, real-time analytics counters, and event deduplication keys. Each has its own TTL.
- **Supabase** — hosted Postgres for developer accounts. No local DB container needed. Spring connects via a standard JDBC URL from the Supabase dashboard. Free tier is sufficient.
- **`application.properties` / `application-local.properties` split** — safe config committed, secrets (JWT secret, Supabase credentials, S3 bucket) stay in the gitignored local file.

Key files: `docker-compose.yml`, `application.properties`, `ApiGuardianApplication.java`

---

### Layer 2 — Proxy (non-blocking forwarding)

The core of the gateway. Every request that isn't `/auth/**` or `/analytics/**` gets forwarded here.

- **`ContentCachingRequestWrapper`** wraps the incoming request on entry. A servlet request body is a stream — reading it once consumes it entirely. The wrapper buffers it in memory so any downstream filter can read the same body again without getting an empty stream.
- **WebClient** replaces RestTemplate. RestTemplate is blocking — each forwarded request holds a thread until the target responds. Under load this exhausts the thread pool. WebClient is non-blocking I/O — threads are released immediately and callbacks fire when the response arrives.
- **Hard timeouts** — 2 seconds to establish the TCP connection, 5 seconds to receive the full response. If either is exceeded the gateway returns 504. The target service can never hang the gateway indefinitely.
- **Latency logging** — every forwarded request logs method, path, status code, and latency in milliseconds.

Key files: `ProxyFilter.java`, `ProxyForwarder.java`

---

### Layer 3 — Rate limiting (Bucket4j + Redis + circuit breaker)

Protects the target service from being overwhelmed. Configured for 100 requests per minute per client IP.

- **Token bucket algorithm** — each IP gets a bucket of 100 tokens. Each request consumes one token. Tokens refill at a steady rate. Unlike a fixed window (which blocks everything after the limit then suddenly allows all at once), the token bucket allows controlled bursting — realistic traffic behavior.
- **Redis-backed buckets** — buckets live in Redis so the limit is shared across multiple gateway instances. One Redis key per client IP with a 60-second TTL.
- **Resilience4j circuit breaker** — the entire rate limit check is wrapped in `@CircuitBreaker(name="redis")`. If Redis goes down the fallback returns `true` — the gateway fails open and allows the request through. Failing closed would take the entire gateway down whenever Redis has a blip.

Key files: `RateLimitService.java`, `RateLimitConfig.java`, `ResilienceConfig.java`

---

### Layer 4 — Kafka event streaming (async, durable, idempotent)

Every proxied request becomes a Kafka event. The proxy never waits for this — it publishes and moves on.

- **Fire-and-forget producer** — `kafkaTemplate.send()` is called without `.get()`. Calling `.get()` blocks the request thread until Kafka acknowledges — that defeats the purpose of async messaging. An error callback logs failures without blocking.
- **UUID per event** — every `RequestEvent` gets a UUID `eventId` at creation time. This is the idempotency key.
- **Redis SET NX deduplication** — the consumer runs `SET NX processed:{eventId}` before touching any counter. If the key already exists the event is skipped. Kafka guarantees at-least-once delivery — without this, a consumer restart replays old events and corrupts every counter.
- **Dead letter topic** — events that fail processing go to `api-requests-dlq` instead of being silently dropped.

Key files: `RequestEventProducer.java`, `RequestEventConsumer.java`, `KafkaConfig.java`, `RequestEvent.java`

---

### Layer 5 — Analytics + JWT security

Two things built together because they're tightly coupled — the analytics endpoints don't exist without auth protecting them.

**Analytics:**
- `AnalyticsService` maintains counters in Redis using `INCR` — atomic, fast, no race conditions.
- Counters tracked: total requests, requests per endpoint, requests per status code, error rate, average latency.
- All updates are idempotent — the Kafka consumer deduplicates before calling any counter update.

**JWT security:**
- `POST /auth/login` accepts username + password, verifies against the Supabase `users` table, returns a signed JWT with 24-hour expiry.
- `POST /auth/register` requires a valid JWT — only authenticated developers (starting with you) can invite others. No open registration.
- `DELETE /auth/users/{id}` requires a valid JWT — removes a developer account from Supabase, immediately invalidating their access.
- `JwtAuthFilter` runs on every request — extracts the `Authorization: Bearer ...` header, validates the signature, extracts the username, sets the Spring `SecurityContext`.
- `SecurityConfig` rules: `/auth/**` open, `/analytics/**` requires valid JWT, proxy routes open.
- No roles — every authenticated developer has identical access. Access is binary.
- Token signing uses a 256-bit secret from `application-local.properties` — never hardcoded, never committed.

Key files: `JwtService.java`, `JwtAuthFilter.java`, `AuthController.java`, `SecurityConfig.java`, `AnalyticsService.java`, `AnalyticsController.java`

---

### Layer 6 — Real-time SSE dashboard

Replaces polling with push. The React dashboard connects once and receives updates as they happen.

- **`SseEmitter`** — Spring's built-in Server-Sent Events support. Each connected dashboard client gets an emitter registered in a thread-safe list.
- **Push on Kafka batch** — every time the Kafka consumer processes a batch, it pushes a fresh metrics snapshot to all connected SSE clients. Updates are event-driven, not timer-driven.
- **React `useAnalyticsStream` hook** — wraps the browser's native `EventSource` API. Connects to `/analytics/stream`, updates component state on each push, handles reconnection automatically.
- **Result** — eliminates 12 HTTP requests per minute per open tab. Real-time latency is sub-100ms from request to dashboard update.

Key files: `AnalyticsStreamController.java`, `useAnalyticsStream.ts`, `Dashboard.tsx`

---

### Layer 7 — S3 report export (retry + dead letter queue)

Daily reports archived permanently. Nothing is silently lost.

- **`@Scheduled`** — runs at midnight daily. Configurable via cron expression in `application.properties`.
- **`@Retryable(maxAttempts=3, backoff=@Backoff(delay=5000, multiplier=2))`** — retries after 5s, then 10s, then 20s. Covers transient S3 errors, network blips, and credential issues.
- **DLQ fallback** — if all 3 attempts fail, the report payload is published to `analytics-reports-dlq`. Stays there for 7 days, replayable manually.
- **Presigned URLs** — reports are private in S3. The `/analytics/reports` endpoint generates 24-hour presigned URLs so the dashboard can download them without AWS credentials in the browser.

Key files: `ReportExporter.java`, `S3Config.java`

---

### Layer 8 — User management (Supabase + Spring Data JPA)

Persistent developer accounts. The only people who can see analytics are people you've explicitly invited.

- **Supabase** — hosted Postgres. Free tier, dashboard to inspect the `users` table, no container to manage.
- **`users` table** — three columns: `id` (UUID), `username`, `password` (bcrypt hashed). No role column — access is binary.
- **`UserRepository`** — Spring Data JPA, one method: `findByUsername(String username)`.
- **`UserDetailsServiceImpl`** — queries Supabase via JPA, returns Spring `UserDetails`. Spring Security uses this during login to verify the bcrypt password hash.
- **Invite-only** — `POST /auth/register` checks for a valid JWT before creating an account. You log in first, then use your token to invite a new developer.
- **Revocation** — `DELETE /auth/users/{id}` removes the account from Supabase. Their existing JWT will still be valid until it expires (24h max) — acceptable tradeoff without a token blacklist.
- **Connection** — copy the JDBC URL from Supabase dashboard → Project Settings → Database → Connection String → JDBC. Goes in `application-local.properties`.

Key files: `User.java`, `UserRepository.java`, `UserDetailsServiceImpl.java`, `AuthController.java`

---

## Data Lifecycle

```
HOT        → Redis      rate limit buckets, real-time counters, dedup keys    TTL: 60s–24h
WARM       → Kafka      full event stream — replayable, idempotent safe       Retention: 7 days
COLD       → S3         daily aggregated reports — JSON                        Permanent archive
PERSISTENT → Supabase   developer accounts, bcrypt password hashes            Forever
```

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Proxy | Spring Boot + WebClient | Non-blocking I/O, timeout-controlled forwarding |
| Auth | Spring Security + JJWT | JWT with invite-only access, constant-time validation |
| User Storage | Supabase (hosted Postgres) | Managed DB, no ops overhead, free tier |
| Rate Limiting | Bucket4j + Redis | Token bucket — burst-friendly, distributed |
| Resilience | Resilience4j | Circuit breakers on Redis + S3, graceful degradation |
| Event Streaming | Apache Kafka (KRaft) | Async, durable, replayable — no Zookeeper |
| Analytics | Spring Boot + Redis | Idempotent INCR counters, replay-safe |
| Report Storage | AWS S3 + Spring Retry | Cold archive, presigned URLs, retry + DLQ |
| Dashboard | React + TypeScript + SSE | Real-time push via EventSource — no polling |

---

## Production Engineering Decisions

| Fix | Severity | What Changed and Why |
|---|---|---|
| WebClient over RestTemplate | HIGH | RestTemplate is blocking — one slow target exhausts the thread pool. WebClient is non-blocking I/O. |
| JWT over API key | HIGH | API keys have no expiry and no revocation. JWT expires in 24h and access is revoked by deleting the account from Supabase. |
| Invite-only registration | HIGH | Open registration would let anyone create an account and read your analytics. `POST /auth/register` requires a valid JWT — only existing authenticated developers can invite others. |
| Supabase over self-managed Postgres | HIGH | Managed hosted DB — no container config, no backup setup, no connection pooling to manage. Standard JDBC URL, works with Spring Data JPA unchanged. |
| Removed `.get()` from Kafka producer | HIGH | Calling `.get()` on the producer future blocks the request thread until Kafka acknowledges. Replaced with async callback. |
| ContentCachingRequestWrapper | HIGH | Servlet request body is a stream — reading it once consumes it. The wrapper buffers it so downstream filters can read it too. |
| Hard proxy timeouts | HIGH | Without timeouts, a hung target hangs the gateway indefinitely. 2s connect, 5s read, then 504. |
| Redis circuit breaker | HIGH | If Redis is down and rate limiting throws, the gateway goes down with it. Resilience4j wraps it — failure returns `true` (fail open). |
| Kafka durability config | HIGH | Default Kafka config loses messages on restart. Retention, flush interval, and replication factor set explicitly. |
| No RBAC | MEDIUM | Every authenticated developer sees the same analytics — there's no partial access use case. RBAC would add complexity with no real benefit. Access is binary: account exists or it doesn't. |
| Idempotent Kafka consumer | MEDIUM | Kafka guarantees at-least-once delivery. Without deduplication, counter replay corrupts metrics. Redis SET NX on event UUID fixes this. |
| S3 export retry + DLQ | MEDIUM | S3 calls fail transiently. Spring Retry retries 3 times with exponential backoff. Final failure publishes to Kafka DLQ — nothing silently dropped. |
| SSE over polling | LOW | React dashboard was polling every 5s — 12 requests/min/tab. Replaced with SseEmitter push. |

---

## File Structure

```
api-guardian/
├── docker-compose.yml                      ← Kafka (KRaft) + Redis only — users are in Supabase
├── README.md
│
├── src/main/java/com/apiguardian/
│   ├── ApiGuardianApplication.java         ← @EnableScheduling + @EnableRetry
│   │
│   ├── proxy/
│   │   ├── ProxyFilter.java                ← ContentCachingRequestWrapper, latency logging
│   │   └── ProxyForwarder.java             ← WebClient, 2s connect + 5s read timeout
│   │
│   ├── ratelimit/
│   │   ├── RateLimitService.java           ← Bucket4j + Redis + @CircuitBreaker(fail open)
│   │   └── RateLimitConfig.java            ← Token bucket: 100 req/min per IP
│   │
│   ├── kafka/
│   │   ├── RequestEventProducer.java       ← Async send, no .get(), error callback only
│   │   ├── RequestEventConsumer.java       ← @KafkaListener + Redis SET NX dedup
│   │   └── KafkaConfig.java               ← DLQ topic, retry config, error handler
│   │
│   ├── analytics/
│   │   ├── AnalyticsService.java           ← Idempotent Redis INCR counters
│   │   ├── AnalyticsController.java        ← GET /analytics/* (JWT required)
│   │   └── AnalyticsStreamController.java  ← GET /analytics/stream (SSE push)
│   │
│   ├── s3/
│   │   ├── ReportExporter.java             ← @Scheduled + @Retryable(3) + DLQ fallback
│   │   └── S3Config.java                   ← AWS client, presigned URL config
│   │
│   ├── security/
│   │   ├── SecurityConfig.java             ← JWT filter chain, open proxy, protect analytics
│   │   ├── JwtAuthFilter.java              ← Extracts + validates Bearer token
│   │   ├── JwtService.java                 ← Signs + validates JWT, extracts username
│   │   ├── AuthController.java             ← POST /auth/login, POST /auth/register (JWT required), DELETE /auth/users/{id}
│   │   └── UserDetailsServiceImpl.java     ← Loads user from Supabase via UserRepository
│   │
│   ├── resilience/
│   │   └── ResilienceConfig.java           ← Circuit breaker beans for Redis + S3
│   │
│   ├── model/
│   │   ├── RequestEvent.java               ← @Data @Builder, includes UUID eventId
│   │   └── AnalyticsReport.java            ← Daily report model for S3 export
│   │
│   └── user/
│       ├── User.java                       ← JPA entity — id (UUID), username, password (bcrypt)
│       └── UserRepository.java             ← findByUsername(String username)
│
└── src/main/resources/
    ├── application.properties              ← Safe config — committed to git
    └── application-local.properties        ← Secrets (JWT secret, Supabase URL, S3) — gitignored
```

---

## REST API

| Method | Endpoint | Auth | Description |
|---|---|---|---|
| `POST` | `/auth/login` | None | Returns JWT access token |
| `POST` | `/auth/register` | JWT required | Invite-only — creates developer account |
| `DELETE` | `/auth/users/{id}` | JWT required | Removes developer account |
| `ANY` | `/*` | None | Proxy — forwards to configured target |
| `GET` | `/analytics/summary` | JWT | Current metrics snapshot |
| `GET` | `/analytics/top-endpoints` | JWT | Most hit endpoints |
| `GET` | `/analytics/errors` | JWT | Error rate over time |
| `GET` | `/analytics/stream` | JWT | SSE stream — real-time push |
| `POST` | `/analytics/export` | JWT | Trigger manual S3 export |
| `GET` | `/analytics/export-status` | JWT | Last export result + timestamp |
| `GET` | `/analytics/reports` | JWT | S3 report list with presigned URLs |

---

## Local Setup

### Prerequisites
- Java 17+
- Maven
- Docker Desktop
- Node.js 18+ (for the React dashboard)
- A free [Supabase](https://supabase.com) account

### 1. Clone and configure

```bash
git clone https://github.com/yourusername/api-guardian.git
cd api-guardian
```

Create `src/main/resources/application-local.properties` (gitignored — never commit this):

```properties
# JWT signing key — minimum 32 characters
jwt.secret=your-256-bit-secret-min-32-chars-long-change-this
jwt.expiration=86400000

# Supabase — Project Settings → Database → Connection String → JDBC
spring.datasource.url=jdbc:postgresql://db.xxxx.supabase.co:5432/postgres
spring.datasource.username=postgres
spring.datasource.password=your-supabase-db-password

# AWS S3
aws.s3.bucket=your-bucket-name
```

### 2. Create the users table in Supabase

In your Supabase dashboard → SQL Editor, run:

```sql
CREATE TABLE users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  username VARCHAR(255) UNIQUE NOT NULL,
  password VARCHAR(255) NOT NULL
);

-- Create your first admin account (bcrypt hash of your password)
-- Generate the hash at: https://bcrypt-generator.com (12 rounds)
INSERT INTO users (username, password)
VALUES ('admin', '$2a$12$your-bcrypt-hash-here');
```

### 3. Start infrastructure

```powershell
docker-compose up -d

# Verify Redis
docker exec api-guardian-redis-1 redis-cli ping          # → PONG

# Verify Kafka
docker exec api-guardian-kafka-1 `
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### 4. Run the gateway

```powershell
mvn spring-boot:run
```

### 5. Run the dashboard

```powershell
cd dashboard; npm install; npm run dev
```

### 6. Test

```powershell
# Login and get your token
curl -X POST http://localhost:8080/auth/login `
  -H "Content-Type: application/json" `
  -d '{"username":"admin","password":"yourpassword"}'
# Returns: {"token":"eyJhbGci..."}

# Invite a new developer (requires your token)
curl -X POST http://localhost:8080/auth/register `
  -H "Authorization: Bearer eyJhbGci..." `
  -H "Content-Type: application/json" `
  -d '{"username":"dev1","password":"theirpassword"}'

# Analytics
curl -H "Authorization: Bearer eyJhbGci..." `
  http://localhost:8080/analytics/summary

# Proxy — no auth
curl http://localhost:8080/api/test

# Trigger rate limit (101st request returns 429)
for ($i=1; $i -le 110; $i++) { curl -s http://localhost:8080/api/test }
```

---

## Architecture Patterns

**Primary: Event-Driven Architecture (EDA)**

The proxy emits request events to Kafka. Downstream consumers react independently. The proxy never waits for analytics — write path and read path are fully decoupled.

| Pattern | Role |
|---|---|
| Event-Driven Architecture | Proxy → Kafka → consumers, fully decoupled |
| CQRS (light) | Write path (proxy + Kafka) separate from read path (analytics API) |
| Circuit Breaker | Resilience4j on Redis and S3 — graceful degradation |
| Sidecar-adjacent | Wraps any HTTP service without modifying it |
| Token Bucket | Rate limiting — burst-friendly vs fixed window |

### Interview one-liner

> "Event-driven architecture — the proxy emits request events to Kafka and downstream consumers react independently. Write and read paths are fully decoupled. Auth is JWT-based with invite-only registration backed by Supabase — no open sign-up, access is revoked by deleting the account. Redis and S3 are wrapped in Resilience4j circuit breakers so the gateway degrades gracefully under dependency failures rather than cascading."

---

