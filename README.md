# API Guardian

> A production-hardened API gateway — non-blocking reverse proxy, JWT auth, token bucket rate limiting, async Kafka event streaming, real-time SSE dashboard, and S3 report archival.

---

## What It Does

API Guardian sits in front of any HTTP service and adds:

- **Non-blocking reverse proxy** — WebClient forwarding with hard connect + read timeouts
- **JWT authentication** — signed tokens, 24h expiry, Redis token blacklist for immediate revocation
- **Rate limiting** — Bucket4j token bucket per client IP backed by Redis
- **Async event streaming** — every request published to Kafka without blocking the request thread
- **Idempotent analytics** — UUID per event, Redis SET NX dedup, replay-safe counters
- **Real-time dashboard** — Server-Sent Events push to React frontend, no polling
- **S3 report export** — daily analytics archived with Spring Retry + Kafka DLQ on failure
- **Resilience** — Resilience4j circuit breakers on Redis and S3, fail-open degradation

---

## Stack

| Layer | Technology |
|---|---|
| Proxy | Spring Boot + WebClient |
| Auth | Spring Security + JJWT + Redis blacklist |
| Users | Supabase (hosted Postgres) + HikariCP |
| Rate Limiting | Bucket4j + Redis + Resilience4j |
| Event Streaming | Apache Kafka (KRaft) |
| Analytics | Redis INCR counters |
| Report Storage | AWS S3 + Spring Retry |
| Dashboard | React + TypeScript + SSE |

---

## Architecture

```
Request → JwtAuthFilter → ProxyFilter (RateLimit) → ProxyForwarder (WebClient)
                                                            ↓
                                                     Response to client
                                                     RequestEvent → Kafka
                                                            ↓
                                                     Consumer (dedup) → Redis counters
                                                            ↓
                                                     REST /analytics/* + SSE stream
                                                            ↓
                                                     ReportExporter → S3 (+ DLQ)
```

Full architecture diagram and layer-by-layer breakdown in `ARCHITECTURE.md`.

---

## Local Setup

### Prerequisites
- Java 17+, Maven, Docker Desktop, Node.js 18+
- Free [Supabase](https://supabase.com) account

### 1. Clone

```bash
git clone https://github.com/yourusername/api-guardian.git
cd api-guardian
```

### 2. Create `src/main/resources/application-local.properties` (gitignored)

```properties
jwt.secret=your-256-bit-secret-minimum-32-characters
jwt.expiration=86400000

spring.datasource.url=jdbc:postgresql://db.xxxx.supabase.co:5432/postgres
spring.datasource.username=postgres
spring.datasource.password=your-supabase-password

aws.s3.bucket=your-bucket-name
```

### 3. Create the users table in Supabase

```sql
CREATE TABLE users (
  id       UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  username VARCHAR(255) UNIQUE NOT NULL,
  password VARCHAR(255) NOT NULL
);
```

Then create your first account via `POST /auth/register` — the app bcrypt-hashes the password for you.

### 4. Start infrastructure

```powershell
docker-compose up -d
docker exec api-guardian-redis-1 redis-cli ping   # → PONG
```

### 5. Run

```powershell
# Terminal 1 — gateway
mvn spring-boot:run

# Terminal 2 — dashboard
cd dashboard; npm install; npm run dev
```

### 6. First login

```powershell
# Register your admin account (open only on first run — lock down after)
curl -X POST http://localhost:8080/auth/register `
  -H "Content-Type: application/json" `
  -d '{"username":"admin","password":"yourpassword"}'

# Login
curl -X POST http://localhost:8080/auth/login `
  -H "Content-Type: application/json" `
  -d '{"username":"admin","password":"yourpassword"}'
# → {"token":"eyJhbGci..."}

# Invite a developer (requires your token)
curl -X POST http://localhost:8080/auth/register `
  -H "Authorization: Bearer eyJhbGci..." `
  -H "Content-Type: application/json" `
  -d '{"username":"dev1","password":"theirpassword"}'
```

### 7. Health check

```powershell
curl http://localhost:8080/actuator/health   # → {"status":"UP"}
```

---

## REST API

| Method | Endpoint | Auth | Description |
|---|---|---|---|
| `POST` | `/auth/register` | None (first user) / JWT (subsequent) | Create developer account |
| `POST` | `/auth/login` | None | Returns JWT |
| `POST` | `/auth/logout` | JWT | Blacklists token immediately |
| `DELETE` | `/auth/users/{id}` | JWT | Removes account + blacklists token |
| `ANY` | `/*` | None | Proxy — forwards to target |
| `GET` | `/analytics/summary` | JWT | Metrics snapshot |
| `GET` | `/analytics/top-endpoints` | JWT | Most hit endpoints |
| `GET` | `/analytics/errors` | JWT | Error rate over time |
| `GET` | `/analytics/stream` | JWT | SSE real-time push |
| `POST` | `/analytics/export` | JWT | Trigger S3 export |
| `GET` | `/analytics/export-status` | JWT | Last export result |
| `GET` | `/analytics/reports` | JWT | S3 reports with presigned URLs |
| `GET` | `/actuator/health` | None | Gateway health status |

---

## Contributing

All analytics endpoints require a JWT. Get one from `POST /auth/login`. Ask the project owner to create you an account via `POST /auth/register`.

See `ARCHITECTURE.md` for a full breakdown of every layer, every design decision, and how to extend the system.

---

## License

MIT
