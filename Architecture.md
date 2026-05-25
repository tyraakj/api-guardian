# API Guardian — Architecture & Engineering Reference

> Internal document. Full breakdown of every layer, every design decision, every production fault fixed, and how to extend the system.

---

## System Overview

API Guardian is an event-driven API gateway. The proxy layer emits every request as a Kafka event. Downstream consumers process those events independently — the proxy never waits for analytics. Write path and read path are fully decoupled.

```
                        ┌─────────────────────────────────────────────┐
                        │  SECURITY LAYER                             │
                        │  JwtAuthFilter                              │
                        │    → checks Redis blacklist (revoked tokens)│
                        │    → validates JWT signature                │
                        │    → sets Spring SecurityContext            │
                        │  SecurityConfig                             │
                        │    → /auth/** open                          │
                        │    → /analytics/** requires valid JWT       │
                        │    → /* (proxy) open                        │
                        └──────────────────┬──────────────────────────┘
                                           │
                        ┌──────────────────▼──────────────────────────┐
                        │  PROXY LAYER                                │
                        │  ProxyFilter (OncePerRequestFilter)         │
                        │    → ContentCachingRequestWrapper           │
                        │    → RateLimitService (Bucket4j + Redis)    │
                        │        └── @CircuitBreaker → fail open      │
                        └──────────────────┬──────────────────────────┘
                                           │
                        ┌──────────────────▼──────────────────────────┐
                        │  FORWARDING LAYER                           │
                        │  ProxyForwarder (WebClient)                 │
                        │    → connectTimeout: 2s                     │
                        │    → readTimeout: 5s → 504 on breach        │
                        └──────────┬───────────────────┬──────────────┘
                                   │                   │
                            Response to          RequestEvent
                              client              (UUID eventId)
                                                       │
                        ┌──────────────────────────────▼──────────────┐
                        │  KAFKA PIPELINE                             │
                        │  RequestEventProducer                       │
                        │    → kafkaTemplate.send() — no .get()       │
                        │    → error callback only                    │
                        │  Topic: api-requests (7 day retention)      │
                        │  RequestEventConsumer                       │
                        │    → Redis SET NX processed:{eventId}       │
                        │    → skip if already processed (dedup)      │
                        │    → AnalyticsService.increment()           │
                        │    → push to SSE emitters                   │
                        └──────────┬────────────────────┬─────────────┘
                                   │                    │
                   ┌───────────────▼───┐    ┌───────────▼─────────────┐
                   │  ANALYTICS API    │    │  SSE STREAM             │
                   │  Redis INCR       │    │  SseEmitter list        │
                   │  counters:        │    │  CopyOnWriteArrayList   │
                   │  - total reqs     │    │  dead emitters removed  │
                   │  - per endpoint   │    │  on IOException         │
                   │  - per status     │    └───────────┬─────────────┘
                   │  - error rate     │                │
                   │  - avg latency    │    ┌───────────▼─────────────┐
                   │  JWT required     │    │  REACT DASHBOARD        │
                   └───────────────────┘    │  useAnalyticsStream()   │
                                            │  EventSource hook       │
                                            │  auto-reconnect         │
                                            └─────────────────────────┘
                                   │
                   ┌───────────────▼──────────────────────────────────┐
                   │  S3 EXPORT                                       │
                   │  @Scheduled (midnight daily)                     │
                   │  @Retryable(3 attempts, exponential backoff)     │
                   │  → attempt 1: immediate                          │
                   │  → attempt 2: +5s                                │
                   │  → attempt 3: +10s                               │
                   │  → final failure: publish to analytics-reports-dlq│
                   │  Presigned URLs: 24h TTL                         │
                   └──────────────────────────────────────────────────┘

External dependencies:
  Supabase    → developer accounts (hosted Postgres, HikariCP pool)
  Redis       → token blacklist, rate limit buckets, analytics counters, dedup keys
  Kafka       → request event stream + DLQ (KRaft, no Zookeeper)
  AWS S3      → cold report archive
```

---

## Layer 1 — Infrastructure Scaffold

### What it does
Starts Kafka and Redis via Docker. Supabase is remote — no local container needed. All config is split between a committed safe file and a gitignored secrets file.

### Key decisions

**Kafka in KRaft mode**
No Zookeeper. KRaft became production-ready in Kafka 3.3 — simpler ops, one less process to manage. `KAFKA_LOG_RETENTION_HOURS=168` keeps events for 7 days — consumer can crash and replay without data loss. `KAFKA_LOG_FLUSH_INTERVAL_MESSAGES=1` flushes every message to disk immediately — slower throughput, but this is a gateway, correctness over speed.

**Redis for four separate concerns**
Rate limit buckets (TTL 60s), analytics counters (TTL 24h), event dedup keys (TTL 24h), JWT blacklist (TTL = remaining token lifetime). Same Redis instance, different key namespaces. Fine for this scale — separate instances if Redis becomes a bottleneck.

**Supabase over self-hosted Postgres**
No container config, no backup setup, no connection pooling to manage ourselves. Standard JDBC URL — Spring Data JPA doesn't know or care it's Supabase. Free tier: 500MB storage, 2 CPUs, sufficient for user accounts.

**HikariCP explicitly configured**
Spring Boot's default Supabase connection pool is 10. Under load, requests queue waiting for a connection. Set `maximum-pool-size=20`, `minimum-idle=5`, `connection-timeout=30000`. Supabase free tier supports up to 60 direct connections.

**Config split**
`application.properties` — committed, safe values only. `application-local.properties` — gitignored, contains JWT secret, Supabase credentials, S3 bucket. Spring merges both at startup via `spring.profiles.active=local`.

### Files
- `docker-compose.yml`
- `src/main/resources/application.properties`
- `src/main/resources/application-local.properties` (gitignored)
- `ApiGuardianApplication.java`

---

## Layer 2 — Proxy (Non-blocking Forwarding)

### What it does
Intercepts every non-analytics, non-auth request and forwards it to the configured target service. Returns the response verbatim. Records latency.

### Key decisions

**WebClient over RestTemplate**
RestTemplate is synchronous — each forwarded request holds a thread from Spring's thread pool until the target responds. Under load (100+ concurrent requests), the thread pool exhausts and new requests queue. WebClient uses non-blocking I/O — the thread is released immediately when the request is sent, and a callback fires when the response arrives. Same result, fraction of the threads.

**ContentCachingRequestWrapper**
An `HttpServletRequest` body is an `InputStream` — it can only be read once. Without wrapping, the first filter to read the body (e.g. a logging filter) leaves an empty stream for everything downstream. `ContentCachingRequestWrapper` reads and buffers the body in memory on first access. Subsequent reads get the buffered copy.

**Hard timeouts — non-negotiable**
`connectTimeout=2000ms` — if TCP handshake to the target takes more than 2 seconds, something is wrong. `readTimeout=5000ms` — if the target doesn't respond within 5 seconds, return 504. Without these, one slow or hung target service blocks a WebClient thread indefinitely. Even async threads have limits.

**OncePerRequestFilter**
Spring can invoke filters multiple times in an error dispatch cycle. `OncePerRequestFilter` guarantees the proxy logic runs exactly once per HTTP request regardless of how Spring internally re-dispatches.

**shouldNotFilter**
Analytics and auth requests are excluded from proxying — they're handled by their own controllers. The filter checks `request.getRequestURI().startsWith("/analytics")` and `/auth` and returns early.

### Files
- `proxy/ProxyFilter.java`
- `proxy/ProxyForwarder.java`

---

## Layer 3 — Rate Limiting (Bucket4j + Redis + Circuit Breaker)

### What it does
Enforces 100 requests per minute per client IP. Returns HTTP 429 on breach. Degrades gracefully if Redis is unavailable.

### Key decisions

**Token bucket over fixed window**
Fixed window: allow 100 requests, then block everything until the window resets. This causes thundering herd — all blocked clients retry at the same moment the window opens. Token bucket: 100 tokens, refill at 100/min, each request consumes one. Allows bursting up to 100, then throttles smoothly. Realistic traffic behavior.

**Redis-backed buckets**
Buckets live in Redis so the rate limit is shared across multiple gateway instances. If you run two gateway pods, a client can't bypass the limit by hitting different instances — they share the same Redis bucket. One key per client IP, TTL 60 seconds.

**Resilience4j circuit breaker — fail open**
`RateLimitService.allowRequest()` is annotated `@CircuitBreaker(name="redis", fallbackMethod="allowRequestFallback")`. If Redis goes down, Resilience4j opens the circuit after 50% failure rate over a 10-request sliding window. The fallback returns `true` — requests are allowed through. The alternative (fail closed, return 429) would take the entire gateway down whenever Redis has a blip. Fail open is the right default for a gateway.

### Files
- `ratelimit/RateLimitService.java`
- `ratelimit/RateLimitConfig.java`
- `resilience/ResilienceConfig.java`

---

## Layer 4 — Kafka Event Streaming (Async, Durable, Idempotent)

### What it does
Every proxied request is published as a `RequestEvent` to Kafka. Completely async — the proxy never waits. The consumer updates analytics counters idempotently.

### Key decisions

**Fire-and-forget — no .get()**
`kafkaTemplate.send(topic, event)` returns a `CompletableFuture`. Calling `.get()` blocks the current thread until Kafka acknowledges the write — this is synchronous Kafka, the same as not using async at all. We attach an error callback via `.whenComplete()` and return immediately. If Kafka is slow, the proxy doesn't care.

**UUID eventId for idempotency**
Kafka guarantees at-least-once delivery — on consumer restart, it may replay events from the last committed offset. Without dedup, replaying 1000 events adds 1000 to every counter. Each `RequestEvent` gets a `UUID.randomUUID()` eventId at creation. The consumer runs `SET NX processed:{eventId} 1 EX 86400` before touching any counter. If the key exists, the event is a duplicate — skip it. This makes the consumer replay-safe.

**Dead letter queue**
Events that fail processing (deserialization error, unexpected exception) after retries go to `api-requests-dlq`. They stay for 7 days (Kafka retention). You can inspect them, fix the bug, and replay manually. Nothing is silently dropped.

**KRaft — no Zookeeper**
Zookeeper was Kafka's external metadata store. KRaft moves metadata management into Kafka itself. One less process, simpler Docker setup, same guarantees.

### Files
- `kafka/RequestEventProducer.java`
- `kafka/RequestEventConsumer.java`
- `kafka/KafkaConfig.java`
- `model/RequestEvent.java`

---

## Layer 5 — Analytics + JWT Security

### What it does
Maintains real-time metrics in Redis. Protects all analytics endpoints with JWT. Handles login, developer invite, token revocation.

### Analytics decisions

**Redis INCR for counters**
`INCR` is atomic in Redis — no race conditions with concurrent consumers. Counters tracked: `analytics:total`, `analytics:endpoint:{path}`, `analytics:status:{code}`, `analytics:errors`, `analytics:latency:sum` + `analytics:latency:count` (for average).

**Idempotent updates**
The Kafka consumer deduplicates before calling `AnalyticsService`. Counter updates are only called for events that passed the SET NX check — safe to process at-least-once.

### JWT security decisions

**Why JWT over API key**
API keys don't expire, can't be selectively revoked without a database lookup, and carry no identity. JWT carries a subject (username), issued-at, and expiry. Validation is stateless — just a signature check. Revocation is handled by the Redis blacklist.

**Redis token blacklist**
`JwtAuthFilter` checks `EXISTS blacklisted:{jti}` before accepting a token. On `POST /auth/logout` or `DELETE /auth/users/{id}`, the token's `jti` (JWT ID) is stored in Redis with TTL equal to the token's remaining lifetime. After expiry, the key disappears automatically — no cleanup needed. This closes the revocation gap.

**PasswordEncoder bean — BCrypt strength 12**
`PasswordEncoder` is declared as a Spring bean with explicit strength 12. `POST /auth/register` calls `passwordEncoder.encode(rawPassword)` before saving. Passwords are never stored in plaintext, never hashed externally. Strength 12 = ~250ms hash time — slow enough to resist brute force, fast enough for login.

**Invite-only registration**
`POST /auth/register` checks `SecurityContextHolder` for an authenticated user. If no valid JWT is present on the first call (bootstrapping), one account is allowed through — after that, a JWT is required. This prevents open registration while allowing the first admin to be created.

**No RBAC**
Every authenticated developer has identical analytics access. There is no partial access use case — you're either a trusted developer with an account or you're not. RBAC would add complexity (role column, role checks, role assignment logic) with zero practical benefit. If a multi-tenant use case emerges, add a `role` column to Supabase and a `hasRole()` check in `SecurityConfig` — the rest of the layer stays identical.

**HTTPS gap — known, documented**
JWT is transmitted in the `Authorization` header. Over plain HTTP, it's interceptable on the local network. In production, API Guardian runs behind a reverse proxy (Nginx, Cloudflare) that terminates TLS. Locally, the risk is accepted as a dev convenience. Document it, don't ignore it.

### Files
- `security/SecurityConfig.java`
- `security/JwtAuthFilter.java`
- `security/JwtService.java`
- `security/AuthController.java`
- `security/UserDetailsServiceImpl.java`
- `analytics/AnalyticsService.java`
- `analytics/AnalyticsController.java`
- `user/User.java`
- `user/UserRepository.java`

---

## Layer 6 — Real-time SSE Dashboard

### What it does
Pushes analytics snapshots to connected React clients on every Kafka batch. No polling.

### Key decisions

**SSE over WebSocket**
WebSocket is bidirectional — overkill for a dashboard that only receives data. SSE is unidirectional server → client, simpler to implement, works over HTTP/1.1, auto-reconnects natively in the browser via `EventSource`.

**CopyOnWriteArrayList for emitter registry**
Multiple Kafka consumer threads push to the emitter list concurrently. `CopyOnWriteArrayList` is thread-safe for iteration — no `ConcurrentModificationException`. Write operations (add/remove) are slower but rare compared to reads.

**Dead emitter cleanup — non-negotiable**
When a browser tab closes, its `SseEmitter` throws `IOException` on the next send attempt. Without cleanup, dead emitters accumulate indefinitely — memory leak under sustained use. The push loop catches `IOException`, calls `emitter.complete()`, and removes it from the list immediately.

**Push on Kafka batch, not on timer**
Updates are event-driven — the dashboard updates exactly when new data arrives. A timer would push stale data on quiet periods and might lag behind on busy ones.

### Files
- `analytics/AnalyticsStreamController.java`
- `dashboard/src/hooks/useAnalyticsStream.ts`
- `dashboard/src/components/Dashboard.tsx`

---

## Layer 7 — S3 Report Export

### What it does
Archives daily analytics snapshots to S3. Retries on failure. Sends to DLQ if all retries exhausted.

### Key decisions

**Spring Retry with exponential backoff**
`@Retryable(maxAttempts=3, backoff=@Backoff(delay=5000, multiplier=2))` — retries at 5s, 10s, 20s. Exponential backoff avoids hammering a temporarily unavailable S3 endpoint. Covers: transient network errors, S3 throttling, temporary credential expiry.

**Kafka DLQ on final failure**
On `@Recover`, the report payload is published to `analytics-reports-dlq`. It stays for 7 days. This is better than logging and discarding — the data is preserved and replayable once the issue is fixed.

**Presigned URLs**
Reports are private in S3 — no public bucket ACLs. The `/analytics/reports` endpoint generates presigned URLs with 24h TTL. The dashboard downloads directly from S3 without credentials in the browser. URLs expire automatically.

**Resilience4j on S3**
S3 calls are wrapped in a circuit breaker. If S3 is consistently failing, the circuit opens and export attempts are skipped rather than blocking the scheduler thread.

### Files
- `s3/ReportExporter.java`
- `s3/S3Config.java`

---

## Layer 8 — User Management (Supabase)

### What it does
Persistent developer accounts in Supabase. The only people who can access analytics are people you've explicitly invited.

### Schema

```sql
CREATE TABLE users (
  id       UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  username VARCHAR(255) UNIQUE NOT NULL,
  password VARCHAR(255) NOT NULL       -- bcrypt, strength 12, never plaintext
);
```

No role column — access is binary. Account exists = full analytics access.

### Key decisions

**Supabase over self-hosted Postgres**
Free tier (500MB, 2 CPUs) is sufficient for user accounts. No Docker container to manage, no backup config, no WAL setup. Spring connects via standard JDBC URL — the JPA layer doesn't know it's Supabase.

**HikariCP pool config**
Supabase free tier allows 60 direct connections. We configure HikariCP with `maximum-pool-size=20`, `minimum-idle=5`, `connection-timeout=30000ms`, `idle-timeout=600000ms`. Spring Boot's default of 10 connections is fine for dev but starves under concurrent login requests.

**bcrypt strength 12**
12 rounds ≈ 250ms on modern hardware. Slow enough to make brute force attacks impractical (250ms × 1M attempts = 70 hours). Fast enough that login feels instant to humans. Declared as a `@Bean` — `PasswordEncoder` is injected wherever hashing or verification is needed.

**Revocation on delete**
`DELETE /auth/users/{id}` removes the account from Supabase AND blacklists the user's current token in Redis (if provided in the request). Their existing JWT remains cryptographically valid but is rejected by `JwtAuthFilter` on the blacklist check. Maximum unauthorized window = 0 if logout is called first, 24h if account is deleted without the token.

### Files
- `user/User.java`
- `user/UserRepository.java`
- `security/UserDetailsServiceImpl.java`
- `security/AuthController.java` (register + delete endpoints)

---

## Production Faults Fixed

| # | Fault | Severity | Fix |
|---|---|---|---|
| 1 | JWT revocation gap — deleted accounts valid for 24h | HIGH | Redis blacklist on `jti` claim, TTL = remaining token lifetime |
| 2 | No HTTPS | HIGH | Documented as known gap — TLS termination at reverse proxy in prod |
| 3 | No HikariCP config — Supabase starved at 10 connections | HIGH | Explicit pool: max 20, min idle 5, 30s timeout |
| 4 | bcrypt rounds set externally | HIGH | `PasswordEncoder` bean, strength 12, all hashing in code |
| 5 | SSE emitter list unbounded — memory leak | MEDIUM | `CopyOnWriteArrayList`, remove dead emitters on `IOException` |
| 6 | No health endpoint | LOW | Spring Actuator `/actuator/health` |
| 7 | RestTemplate blocking proxy | HIGH | WebClient non-blocking I/O |
| 8 | Kafka `.get()` on hot path | HIGH | Fire-and-forget with error callback |
| 9 | Request body consumed on first read | HIGH | `ContentCachingRequestWrapper` |
| 10 | No proxy timeouts | HIGH | 2s connect, 5s read, 504 on breach |
| 11 | No Redis circuit breaker | HIGH | Resilience4j fail-open on rate limit |
| 12 | Kafka default durability config | HIGH | Retention 7d, flush interval 1, replication factor 1 |
| 13 | Idempotent consumer missing | MEDIUM | Redis SET NX on UUID eventId |
| 14 | S3 export no retry or DLQ | MEDIUM | Spring Retry 3 attempts + Kafka DLQ |
| 15 | SSE polling | LOW | SseEmitter push on Kafka batch |

---

## Extending the System

### Add a new analytics metric
1. Add a Redis key in `AnalyticsService` — one `INCR` call
2. Increment it in `RequestEventConsumer` after the dedup check
3. Expose it in `AnalyticsController` — one `GET` endpoint
4. Display it in the React dashboard — subscribe to the SSE stream, it's already pushed

### Add a new target service
Change `proxy.target.url` in `application.properties`. The entire proxy layer is target-agnostic.

### Add RBAC
1. Add `role VARCHAR(50)` to the Supabase `users` table
2. Include role as a claim in `JwtService.generateToken()`
3. Extract it in `JwtAuthFilter` and add to `GrantedAuthority`
4. Add `.hasRole("ADMIN")` to relevant routes in `SecurityConfig`

### Scale horizontally
Rate limiting and analytics already work across multiple instances — both use Redis as shared state. Kafka consumer group ensures each event is processed once across all instances. Supabase handles concurrent connections via HikariCP pool. S3 export should be moved to a separate scheduled job to avoid duplicate exports.

---

## Data Lifecycle

```
HOT        → Redis      rate limit buckets, counters, dedup keys, JWT blacklist   TTL: 60s–24h
WARM       → Kafka      full event stream, DLQ                                    Retention: 7 days
COLD       → S3         daily aggregated reports (JSON)                           Permanent
PERSISTENT → Supabase   developer accounts, bcrypt hashes                         Forever
```

---

## Interview One-liner

> "Event-driven architecture — the proxy emits request events to Kafka and downstream consumers react independently. Write and read paths are fully decoupled. JWT auth with a Redis blacklist handles immediate token revocation. Redis and S3 are wrapped in Resilience4j circuit breakers — the gateway degrades gracefully under dependency failures rather than cascading. 15 production faults identified and fixed before writing a line of feature code."