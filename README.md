# ReachInbox - Full-Stack Email Job Scheduler

[![CI Pipeline](https://github.com/reachinbox/reachinbox-scheduler/actions/workflows/ci.yml/badge.svg)](https://github.com/reachinbox/reachinbox-scheduler/actions/workflows/ci.yml)
[![Node.js](https://img.shields.io/badge/node-%3E%3D20.0.0-brightgreen.svg)](https://nodejs.org/)
[![TypeScript](https://img.shields.io/badge/typescript-5.4-blue.svg)](https://www.typescriptlang.org/)
[![BullMQ](https://img.shields.io/badge/BullMQ-Redis%20Queue-red.svg)](https://bullmq.io/)
[![Prisma](https://img.shields.io/badge/ORM-Prisma%20MySQL-16a34a.svg)](https://www.prisma.io/)

A production-grade, distributed email job scheduler built for the **ReachInbox Software Development Intern Assignment**. This project implements reliable delayed email dispatching using **Node.js, Express, TypeScript, BullMQ, Redis, MySQL, Prisma ORM, Nodemailer (Ethereal test SMTP), and React (Vite)**.

---

## Table of Contents
1. [Architectural Overview](#architectural-overview)
2. [Key Features](#key-features)
3. [Monorepo Directory Structure](#monorepo-directory-structure)
4. [Prerequisites & macOS Installation](#prerequisites--macos-installation)
5. [Quick Start Guide](#quick-start-guide)
6. [Environment Variables Reference](#environment-variables-reference)
7. [Database Migrations](#database-migrations)
8. [Queue Monitoring (Bull Board & Swagger)](#queue-monitoring-bull-board--swagger)
9. [REST API Documentation & curl Examples](#rest-api-documentation--curl-examples)
10. [Step-by-Step Testing Guide](#step-by-step-testing-guide)
11. [How Delayed Jobs & Recovery Work](#how-delayed-jobs--recovery-work)
12. [Troubleshooting Common Issues](#troubleshooting-common-issues)
13. [Security & Distributed System Considerations](#security--distributed-system-considerations)
14. [How to Present This Project in Your Interview](#how-to-present-this-project-in-your-interview)

---

## Architectural Overview

This system separates HTTP API traffic from heavy asynchronous background processing using a dedicated queue:

```
[ React + Vite Client ]
        │  (HTTP / REST)
        ▼
[ Express REST API ] ──(Saves Record)──► [ MySQL Database ]
        │                                 (Prisma ORM)
        │ (Delayed Job Enqueue)
        ▼
[ Redis Broker (AOF Persisted) ]
        │
        │ (Delayed Timer Fires)
        ▼
[ BullMQ Background Worker ] ────► [ Ethereal Email SMTP Sandbox ]
        │                                  │ (Generates Web Preview URL)
        │ (Updates Status: SENT/FAILED)    │
        └──────────────────────────────────┘
```

### Architectural Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    actor User as User / Client
    participant API as Express API Server
    participant DB as MySQL (Prisma)
    participant Redis as Redis (BullMQ Queue)
    participant Worker as Background Worker Process
    participant SMTP as Ethereal SMTP Server

    User->>API: POST /api/emails/schedule { recipient, subject, body, scheduledAt, idempotencyKey }
    API->>API: Validate with Zod (Reject past dates)
    API->>DB: Check idempotencyKey (Prevent duplicate schedules)
    API->>DB: INSERT into emails (Status: SCHEDULED)
    API->>Redis: Enqueue Job with delay = (scheduledAt - now) and jobId = email.id
    API->>DB: UPDATE emails SET bullJobId = job.id
    API-->>User: 201 Created { id, scheduledAt, status: "SCHEDULED" }

    Note over Redis,Worker: Delay countdown in Redis (Zero Node.js CPU / setTimeout)

    Redis->>Worker: Job Ready Event (Target Time Reached)
    Worker->>DB: Check email status (Skip if CANCELLED or already SENT)
    Worker->>DB: UPDATE emails SET status = PROCESSING, attempts = attempts + 1
    Worker->>SMTP: Transmit email via Nodemailer
    SMTP-->>Worker: 250 OK { messageId, previewUrl }
    Worker->>DB: UPDATE emails SET status = SENT, sentAt = NOW(), previewUrl = URL
    Worker->>DB: INSERT into delivery_attempts (status: SUCCESS)
    Worker-->>Redis: Mark BullMQ Job as Completed
```

---

## Key Features

- **No setTimeout / No In-Memory Scheduling**: Jobs are scheduled with Redis sorted sets in BullMQ. Node.js threads never block or sleep.
- **Separate Worker Architecture**: API and Worker run as completely separate Node.js processes. High HTTP load never lags email dispatches, and worker crashes never bring down the API.
- **Full Restart Persistence**: Redis is configured with Append-Only File (`AOF`) disk persistence. Delayed jobs survive container and host restarts.
- **Automatic Startup Reconciliation**: On boot, the API scans MySQL for any pending jobs missing in Redis and re-enqueues them with remaining delay times.
- **Idempotency Support**: Clients can supply an `idempotencyKey` to guarantee that network retries or duplicate clicks never create duplicate emails.
- **Job Cancellation**: Users can cancel pending `SCHEDULED` jobs with one click. The job is instantly pruned from BullMQ and updated to `CANCELLED` in MySQL.
- **Retry Mechanism with Exponential Backoff**: Transient SMTP network drops trigger 3 retry attempts with exponential backoff (`5s`, `10s`, `20s`).
- **Ethereal Email Integration**: Uses safe test SMTP credentials. Emails are safely captured in the cloud and render an instant web preview URL.
- **Live Observability**: Integrated **Bull Board** at `/admin/queues` and **Swagger OpenAPI Docs** at `/api/docs`.
- **Responsive React Dashboard**: Clean UI with overview cards, live health status, search, status filtering, details modal, and quick testing presets.

---

## Monorepo Directory Structure

```text
reachinbox-scheduler/
├── apps/
│   ├── api/                     # Express REST API Backend
│   │   ├── src/
│   │   │   ├── config/          # Zod env, Prisma MySQL, Redis, Swagger, Bull Board
│   │   │   ├── controllers/     # Request handlers
│   │   │   ├── middleware/      # Centralized error handler & 404 handler
│   │   │   ├── queues/          # BullMQ queue producer definition
│   │   │   ├── routes/          # /api/emails and /api/health routes
│   │   │   ├── services/        # DB transactions, queue reconciliation, idempotency
│   │   │   ├── utils/           # Pino logger & custom AppError class
│   │   │   ├── validators/      # Zod validation schemas
│   │   │   └── app.ts           # Express server setup & graceful shutdown
│   │   ├── prisma/
│   │   │   ├── schema.prisma    # Models: Email & DeliveryAttempt
│   │   │   └── migrations/      # Production SQL migration files
│   │   ├── tests/               # Supertest & Vitest test suites
│   │   └── package.json
│   │
│   ├── worker/                  # BullMQ Background Worker Daemon
│   │   ├── src/
│   │   │   ├── config/          # Worker Zod environment & connection clients
│   │   │   ├── processors/      # Email processor with lifecycle state transitions
│   │   │   ├── services/        # Nodemailer SMTP service & test credentials helper
│   │   │   ├── workers/         # BullMQ Worker instance with error & stalled handlers
│   │   │   └── index.ts         # Worker bootstrap & graceful shutdown
│   │   ├── tests/               # Worker retry & processor test suites
│   │   └── package.json
│   │
│   └── web/                     # React + Vite Frontend Dashboard
│       ├── src/
│       │   ├── api/             # Typed Axios client
│       │   ├── components/      # Sidebar, StatCard, StatusBadge, EmailTable, Form, Modal
│       │   ├── pages/           # Dashboard, Scheduled, Sent, Schedule, Details
│       │   ├── styles/          # Responsive dark theme stylesheet
│       │   ├── utils/           # Date formatters & input constraints
│       │   ├── App.tsx          # React Router layout
│       │   └── main.tsx         # Root mount
│       └── package.json
│
├── packages/
│   └── shared/                  # Shared TypeScript Contracts & Types
│       └── src/
│           ├── email.types.ts   # EmailStatus enum, DTOs, API response interfaces
│           └── index.ts
│
├── .github/
│   └── workflows/
│       └── ci.yml               # Automated GitHub Actions test & build pipeline
├── docker-compose.yml           # Persistent MySQL 8.0 & Redis 7.0 with AOF
├── package.json                 # Monorepo root with npm workspaces scripts
├── tsconfig.base.json           # Shared strict TypeScript configuration
└── .env.example                 # Environment variables template
```

---

## Prerequisites & macOS Installation

### 1. Install Apple Developer Tools
```bash
xcode-select --install
```

### 2. Install Homebrew (if not already installed)
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

### 3. Install Node.js (v20+ LTS recommended)
```bash
brew install node
node -v   # Should output v20.x or higher
npm -v    # Should output 10.x or higher
```

### 4. Database & Redis Services
Choose either **Option A (Docker)** or **Option B (Native Homebrew Services)**:

#### Option A: Docker Desktop (Recommended)
1. Download and run [Docker Desktop for Mac](https://www.docker.com/products/docker-desktop/).
2. Run from the project root:
   ```bash
   npm run docker:up
   ```

#### Option B: Standalone Services via Homebrew
If you prefer not to use Docker:
```bash
brew install mysql redis
brew services start mysql
brew services start redis
```
*(Create the database once: `mysql -u root -e "CREATE DATABASE IF NOT EXISTS reachinbox_scheduler;"`)*

---

## Quick Start Guide

### Step 1: Install Dependencies
From the repository root, install all workspace packages in one command:
```bash
npm install
```

### Step 2: Configure Environment Variables
Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```

### Step 3: Generate Ethereal Email SMTP Credentials
Run our automated credential generator:
```bash
npx tsx apps/worker/src/services/generate-ethereal.ts
```
Copy the printed `SMTP_USER` and `SMTP_PASS` into your `.env` file.

### Step 4: Run Database Migrations
Generate Prisma Client and apply migrations:
```bash
npm run prisma:generate
npm run prisma:migrate
```

### Step 5: Start All Services
Open 3 terminal windows or tabs:

* **Terminal 1: Start Express REST API**
  ```bash
  npm run dev:api
  ```
  *(API runs on http://localhost:4000)*

* **Terminal 2: Start BullMQ Worker**
  ```bash
  npm run dev:worker
  ```
  *(Worker listens to Redis queue)*

* **Terminal 3: Start React Dashboard**
  ```bash
  npm run dev:web
  ```
  *(Frontend dashboard opens at http://localhost:5173)*

---

## Environment Variables Reference

| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `NODE_ENV` | `development` | Runtime environment (`development`, `production`, `test`) |
| `PORT` | `4000` | Port for Express REST API |
| `DATABASE_URL` | `mysql://root:reachinbox@localhost:3306/reachinbox_scheduler` | MySQL connection string for Prisma ORM |
| `REDIS_HOST` | `localhost` | Redis server hostname for BullMQ queue |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_PASSWORD` | `""` | Redis auth password (blank if unauthenticated) |
| `SMTP_HOST` | `smtp.ethereal.email` | Test SMTP host |
| `SMTP_PORT` | `587` | Test SMTP port |
| `SMTP_SECURE` | `false` | TLS setting (`false` for port 587 STARTTLS) |
| `SMTP_USER` | *(from Ethereal)* | Test SMTP username |
| `SMTP_PASS` | *(from Ethereal)* | Test SMTP password |
| `SMTP_FROM` | `"ReachInbox Scheduler" <scheduler@reachinbox.test>` | Sender display header |
| `WORKER_CONCURRENCY`| `5` | Concurrent jobs processed simultaneously by worker |
| `LOG_LEVEL` | `info` | Pino log level (`fatal`, `error`, `warn`, `info`, `debug`) |
| `VITE_API_URL` | `http://localhost:4000/api` | Base URL used by React Axios client |

---

## Database Migrations

The database schema is managed via **Prisma ORM**.

- **Apply Migrations in Development:**
  ```bash
  npm run prisma:migrate
  ```
- **Apply Migrations in Production / CI:**
  ```bash
  npx prisma migrate deploy --schema=apps/api/prisma/schema.prisma
  ```
- **Open Prisma Visual Studio (GUI Database Browser):**
  ```bash
  npm run prisma:studio
  ```

### Database Schema Structure:
1. **`emails` table**:
   - `id`: CUID unique string
   - `recipient`: VarChar(255)
   - `subject`: VarChar(255)
   - `body`: Text
   - `scheduledAt`: DateTime(3)
   - `status`: ENUM (`SCHEDULED`, `PROCESSING`, `SENT`, `FAILED`, `CANCELLED`)
   - `bullJobId`: VarChar(255) (Unique index)
   - `idempotencyKey`: VarChar(255) (Unique index)
   - `attempts`: Int (default 0)
   - `lastError`: Text (nullable)
   - `previewUrl`: VarChar(500) (nullable)
   - `sentAt`: DateTime(3) (nullable)
   - `createdAt`, `updatedAt`
2. **`delivery_attempts` table**:
   - Audits every delivery attempt with `attemptNumber`, `status` (`SUCCESS` / `FAILED`), `errorMessage`, and timestamp.

---

## Queue Monitoring (Bull Board & Swagger)

### 1. Interactive Bull Board Queue Dashboard
Open in your browser:
👉 **[http://localhost:4000/admin/queues](http://localhost:4000/admin/queues)**
- Inspect delayed jobs with real-time countdowns.
- View active jobs being executed by the worker.
- Inspect completed jobs, returned preview URLs, and failed job stack traces.
- Manually trigger retries or clean queue records.

### 2. Swagger / OpenAPI Documentation
Open in your browser:
👉 **[http://localhost:4000/api/docs](http://localhost:4000/api/docs)**
- Interactive API documentation with schema specifications and live "Try it out" requests.

---

## REST API Documentation & curl Examples

### 1. Schedule a New Email Job
**`POST /api/emails/schedule`**

*Request:*
```bash
curl -X POST http://localhost:4000/api/emails/schedule \
  -H "Content-Type: application/json" \
  -d '{
    "recipient": "candidate@example.com",
    "subject": "Interview Scheduled",
    "body": "Hello, your interview is confirmed for tomorrow.",
    "scheduledAt": "2026-10-16T21:30:00.000Z",
    "idempotencyKey": "unique-client-req-001"
  }'
```

*Response (`201 Created`):*
```json
{
  "success": true,
  "message": "Email scheduled successfully",
  "data": {
    "id": "cm2r18abc0000xyz",
    "recipient": "candidate@example.com",
    "subject": "Interview Scheduled",
    "body": "Hello, your interview is confirmed for tomorrow.",
    "scheduledAt": "2026-10-16T21:30:00.000Z",
    "status": "SCHEDULED",
    "bullJobId": "cm2r18abc0000xyz",
    "idempotencyKey": "unique-client-req-001",
    "attempts": 0,
    "lastError": null,
    "previewUrl": null,
    "sentAt": null
  }
}
```

*(If you send the exact same request again with the same `idempotencyKey`, it responds with `200 OK` and returns the existing record without duplicating the queue job).*

---

### 2. List Scheduled Emails
**`GET /api/emails/scheduled?page=1&limit=10&search=interview`**

*Response (`200 OK`):*
```json
{
  "success": true,
  "data": {
    "items": [ ... ],
    "total": 1,
    "page": 1,
    "limit": 10,
    "totalPages": 1
  }
}
```

---

### 3. List Sent and Delivered Emails
**`GET /api/emails/sent?page=1&limit=10&status=SENT`**

*Response (`200 OK`):*
```json
{
  "success": true,
  "data": {
    "items": [
      {
        "id": "cm2r18abc0000xyz",
        "recipient": "candidate@example.com",
        "subject": "Interview Scheduled",
        "status": "SENT",
        "attempts": 1,
        "previewUrl": "https://ethereal.email/message/WaQKMgKddxQDoou... ",
        "sentAt": "2026-10-16T21:30:02.150Z"
      }
    ],
    "total": 1
  }
}
```

---

### 4. Get Single Email Details
**`GET /api/emails/:id`**

*Response (`200 OK`):*
```json
{
  "success": true,
  "data": {
    "id": "cm2r18abc0000xyz",
    "recipient": "candidate@example.com",
    "subject": "Interview Scheduled",
    "status": "SENT",
    "bullJobId": "cm2r18abc0000xyz",
    "attempts": 1,
    "previewUrl": "https://ethereal.email/message/WaQKMgKddxQDoou...",
    "deliveryLogs": [
      {
        "attemptNumber": 1,
        "status": "SUCCESS",
        "previewUrl": "https://ethereal.email/message/WaQKMgKddxQDoou...",
        "createdAt": "2026-10-16T21:30:02.150Z"
      }
    ]
  }
}
```

---

### 5. Cancel a Pending Scheduled Email
**`DELETE /api/emails/:id`**

*Response (`200 OK`):*
```json
{
  "success": true,
  "message": "Scheduled email has been cancelled successfully",
  "data": {
    "id": "cm2r18abc0000xyz",
    "status": "CANCELLED"
  }
}
```

*(Attempting to cancel an email that has already been `SENT` returns `400 Bad Request`)*.

---

### 6. System Health Check
**`GET /api/health`**

*Response (`200 OK`):*
```json
{
  "status": "healthy",
  "timestamp": "2026-10-16T21:30:00.000Z",
  "uptimeSeconds": 124,
  "services": {
    "database": "connected",
    "redis": "connected"
  }
}
```

---

## Step-by-Step Testing Guide

### 1. Run Automated Unit & Integration Tests
Run all test suites across the monorepo:
```bash
npm run test
```
*Tests cover Zod validation, past date rejection, idempotency deduplication, job cancellation safeguards, queue reconciliation recovery, and worker retry logic.*

### 2. End-to-End Test: Scheduling & Delivery
1. Open the Dashboard at **http://localhost:5173**.
2. Click **Schedule Email**.
3. Fill in recipient (`test@reachinbox.test`), subject, and body.
4. Click the **+2 mins** preset button.
5. Click **Schedule Email Job**.
6. Switch to the **Scheduled Jobs** page. You will see the job with a `Scheduled` badge.
7. Open **Bull Board** at **http://localhost:4000/admin/queues** and watch the delayed counter count down to 0.
8. When the 2 minutes elapse, the worker process will output:
   `Email successfully sent and database updated to SENT`
9. In the dashboard, open **Sent History**. The status is now `Sent`.
10. Click the **External Link icon** or **View Details** to open the **Ethereal web preview URL** and inspect the rendered HTML email in your browser!

### 3. Test Idempotency
1. Submit an email scheduling request with `idempotencyKey = "demo-key-100"`. Notice HTTP `201 Created`.
2. Immediately submit the exact same form again with the same `idempotencyKey = "demo-key-100"`.
3. Notice that the server responds with HTTP `200 OK` ("idempotent request detected") and does **not** create a duplicate BullMQ job or duplicate database row.

### 4. Test Job Cancellation
1. Schedule an email for 10 minutes in the future.
2. In the **Scheduled Jobs** table, click the red **Cancel (X)** button.
3. Check Bull Board: the job is immediately removed from Redis.
4. The database status changes to `CANCELLED`. The worker will never send this email.

### 5. Test Restart Persistence & Recovery
1. Schedule an email for 5 minutes in the future.
2. Stop the API server (`Ctrl+C`) and stop the Worker (`Ctrl+C`).
3. If using Docker, restart the Redis container (`docker compose restart redis`). Because Redis uses Append-Only File (`AOF`) persistence, the delayed sorted set in Redis survives the restart.
4. Restart the API server (`npm run dev:api`) and Worker (`npm run dev:worker`).
5. On boot, the API runs `reconcileQueue()` and confirms that your scheduled job is verified and waiting.
6. When the scheduled time arrives, the worker picks up the job and dispatches it seamlessly.

---

## How Delayed Jobs & Recovery Work

### Why Not `setTimeout` or `setInterval`?
- JavaScript's `setTimeout` runs inside the Node.js process memory. If the server restarts, crashes, or is deployed with a new version, all active timers are wiped and forgotten.
- Keeping hundreds of thousands of active `setTimeout` handles consumes immense RAM and can block the Node.js event loop.

### How BullMQ Solves This:
BullMQ delegates timer management directly to **Redis Sorted Sets (`ZSET`)**:
1. When you schedule an email for `2026-10-16T21:40:00Z`, BullMQ adds the job ID to a Redis `ZSET` with the unix epoch timestamp in milliseconds as its score.
2. When the clock reaches that score, Redis atomically shifts the job from the delayed set to the active waiting queue.
3. The background worker picks up the job via an atomic blocking pop (`BLMOVE`), transitions the database record to `PROCESSING`, and transmits the email.

---

## Troubleshooting Common Issues

### 1. `Redis connection error: ECONNREFUSED`
- **Cause**: Redis is not running on port 6379.
- **Fix**: Run `npm run docker:up` or `brew services start redis`. Verify with `redis-cli ping` (should reply `PONG`).

### 2. `Database connection check failed (MySQL)`
- **Cause**: MySQL service is offline or credentials in `.env` are mismatched.
- **Fix**: Ensure MySQL is running on port 3306. Check your `DATABASE_URL` in `.env`. Run `npx prisma db pull` to test connectivity.

### 3. `SMTP authentication failed`
- **Cause**: Ethereal test credentials expired or were not set.
- **Fix**: Run `npx tsx apps/worker/src/services/generate-ethereal.ts` to generate fresh credentials and update `SMTP_USER` and `SMTP_PASS` in `.env`.

### 4. `BullMQ: maxRetriesPerRequest must be null`
- **Cause**: BullMQ requires long-running blocking commands.
- **Fix**: Already handled in our [`apps/api/src/config/redis.ts`](file:///Users/navgurukul/Downloads/reachinbox-scheduler/apps/api/src/config/redis.ts) via `maxRetriesPerRequest: null`.

---

## Security & Distributed System Considerations

1. **Environment Variable Protection**:
   - Secrets (`DATABASE_URL`, `REDIS_PASSWORD`, `SMTP_PASS`) are validated using Zod at runtime and are never exposed to frontend code or checked into Git.
2. **At-Least-Once Delivery & Idempotency**:
   - In distributed systems, network partitions can occur between SMTP and MySQL. If SMTP succeeds but the network drops before MySQL records `status = SENT`, a retry could resend the email.
   - We minimize duplicates by checking `status === SENT` in the worker before sending and by generating client idempotency keys.
3. **Graceful Shutdown**:
   - Both API and Worker capture `SIGTERM` and `SIGINT` signals. The worker finishes in-flight emails and pauses the queue before closing Redis and MySQL connections.

---

## How to Present This Project in Your Interview

When presenting this project to the ReachInbox engineering team, structure your walkthrough as follows:

1. **Start with the Problem Statement**:
   *"In email automation platforms like ReachInbox, sending millions of follow-up emails at specific future times cannot be done with simple Node.js memory timers or cron polling because servers restart and databases get overwhelmed. I designed a decoupled, persistent architecture using BullMQ and Redis."*

2. **Highlight the Architecture (API vs. Worker)**:
   *"I separated the REST API from the Background Worker into independent processes. The API only handles validation, database persistence, and queueing. The worker focuses purely on SMTP communication, rate limits, and retries. If our worker experiences high CPU load or SMTP latency, our API remains lightning-fast."*

3. **Demonstrate Reliability & Fault Tolerance**:
   *"Show the **Bull Board** at `/admin/queues` and demonstrate scheduling a job. Then show **Idempotency** by clicking send twice with the same key to show no duplicate jobs are created. Finally, explain the **Startup Reconciliation** logic that guarantees zero lost jobs even if Redis or the worker restarts."*

4. **Show the Ethereal Email Preview**:
   *"Click the generated Ethereal preview link to show the rendered HTML email in the browser, explaining that Ethereal was used to safely test delivery without spamming real inboxes."*
