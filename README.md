# Highly Scalable Tinder Backend System Design & Implementation

A production-grade, highly scalable microservices backend for Tinder engineered to handle millions of daily active users, high write-throughput (WPS) swiping, atomic match detection, real-time change data capture (CDC), and sub-5ms geospatial feed delivery.

---

## 🏛 System Architecture Overview

```
                                      +---------------------------------------------+
                                      |              Client Applications            |
                                      +---------------------------------------------+
                                                             |
                                                             v
                                        +-----------------------------------------+
                                        |          API Gateway (Port 8080)        |
                                        |  - Reverse Proxy Routing                |
                                        |  - JWT Authentication & Authorization   |
                                        |  - Token Bucket / Sliding Rate Limiting |
                                        |  - Daily Swipe Quota Enforcer (100/day) |
                                        +-----------------------------------------+
                                          /                  |                  \
                                         /                   |                   \
                                        v                    v                    v
+---------------------------------------+  +-------------------------------+  +-------------------------------+
|     Profile Service (Port 8081)       |  |   Swipe Service (Port 8082)   |  | Notification Service (8083)   |
| - User Registration & Authentication  |  | - Append-only Swipe Recorder  |  | - Kafka Event Consumer        |
| - Profile Creation & Preferences      |  | - Atomic Match Resolution     |  | - Mutual Match Notifier       |
| - Discovery Settings (Distance, Age)  |  | - Concurrent Lock Manager     |  | - Swipe-Right Alerts          |
| - Mutual Matches Storage (PostgreSQL) |  +-------------------------------+  | - Push & Email Dispatcher     |
| - Real-time Outbox CDC Producer       |             /             \         +-------------------------------+
| - Low-latency Distance-Sorted Feed    |            /               \                        ^
+---------------------------------------+           /                 \                       |
         |                 \                       v                   v                      |
         v                  \        +------------------+   +--------------------+            |
+--------------------+       \       |  Apache Cassandra|   | Redis Cluster      |            |
| PostgreSQL 16      |        \      |  (High WPS Swipes|   | - Atomic Lua Match |            |
| - ACID Consistency |         \     |   CommitLog O(1))|   | - Feed Stack Cache |            |
| - Users & Profiles |          \    +------------------+   | - Rate Limit Keys  |            |
| - Matches Table    |           \                          +--------------------+            |
+--------------------+            \                                   |                       |
                                   \                                  |                       |
                                    \                                 v                       |
                                     \               +-----------------------------------+    |
                                      \              |     Apache Kafka Event Bus        |----+
                                       +------------>| - tinder.profile.events (CDC)     |
                                                     | - tinder.swipe.events             |
                                                     | - tinder.match.events             |
                                                     +-----------------------------------+
                                                                      |
                                                                      v
                                                     +-----------------------------------+
                                                     |       Elasticsearch Cluster       |
                                                     | - Spatial geo_point Index         |
                                                     | - geo_distance Filter & Sorting   |
                                                     +-----------------------------------+
                                                                      ^
                                                                      |
                                                     +-----------------------------------+
                                                     |     Feed Stack Cron Worker        |
                                                     | - Scheduled Background Job        |
                                                     | - Checks Cassandra Past Swipes    |
                                                     | - Top-ups Redis Feed Stacks       |
                                                     +-----------------------------------+
```

---

## 🔑 Key Engineering Architectural Highlights

### 1. Database Partitioning & Write Optimization (Cassandra vs. PostgreSQL)
- **High WPS Swipes in Apache Cassandra**:
  - Tinder processes hundreds of millions of swipes daily. Swipes are append-only.
  - Partition Key: `((swiper_id), target_id)`.
  - Writes bypass disk reads entirely by appending to the CommitLog and MemTable in memory (sub-millisecond writes).
  - Querying all historical swipes by a user hits exactly **one partition**.
- **Relational Consistency in PostgreSQL**:
  - User credentials, profile entities, and confirmed `matches` require strict ACID transactions and relational constraints.
  - Matches table enforces canonical uniqueness: `UNIQUE(user1_id, user2_id)` where `user1_id < user2_id`.

### 2. Atomic Mutual Match Detection with Redis
- **Problem**: When User 1 and User 2 swipe right on each other at the exact same millisecond across two different instances, naive checks create race conditions resulting in duplicate matches, double emails, and inconsistent states.
- **Solution**: Atomic Redis Lua script (`EVAL`):
  ```lua
  local reverseKey = KEYS[1]  -- "swipe:right:user2:user1"
  local myKey = KEYS[2]       -- "swipe:right:user1:user2"
  local lockKey = KEYS[3]     -- "match:lock:minUser:maxUser"
  local ttl = ARGV[1]

  redis.call('SET', myKey, '1', 'EX', ttl)
  local reverseExists = redis.call('EXISTS', reverseKey)

  if reverseExists == 1 then
    local lockAcquired = redis.call('SET', lockKey, '1', 'NX', 'EX', 300)
    if lockAcquired then
      return 1 -- Match! Sole coordinator for DB match creation & Kafka event emission
    else
      return 2 -- Match! Already coordinated by concurrent counterpart
    end
  end

  return 0 -- First swipe right, waiting for counterpart
  ```
- **Guarantees**: Exactly-once match creation, no race conditions, immediate response.

### 3. Feed Generation with Elasticsearch & Pre-Computed Redis Stack Cache
- **Change Data Capture (CDC)**:
  - Updates in PostgreSQL emit `PROFILE_CREATED` / `PROFILE_UPDATED` events to Kafka topic `tinder.profile.events`.
  - Stream consumer indexes documents into Elasticsearch with a native `geo_point` coordinate field.
- **Exclusion of Swiped Profiles**:
  - When User 1 swipes on User 2, `targetId` is immediately added to the Redis Set `user:swipes:user1`.
  - When querying candidates, swiped IDs are excluded using Elasticsearch's `must_not` query filter.
- **Redis Feed Stack Cache (`feed:stack:<userId>`)**:
  - A scheduled cron worker continuously queries candidate profiles for active users and pushes them into a Redis List (`LPUSH`).
  - When a user opens their app and requests `GET /api/v1/feed`, candidates are served in O(1) time via `RPOP`, guaranteeing **<5ms latency**.

---

## 🚀 Running the Project

### Prerequisites
- Node.js (>= 18.x)
- Docker & Docker Compose (Optional for local containerized deployment)

### 1. Installation
```bash
npm install
```

### 2. Run the End-to-End Verification Test
Executes the full automated workflow (Gateway, Profile, Swipe, Notification, and Cron Worker):
```bash
npm run test:flow
```

### 3. Spin up with Docker Compose (Full Distributed Stack)
```bash
docker-compose up --build
```

### 4. Running Individual Services Locally
```bash
npm run gateway              # Port 8080
npm run profile-service      # Port 8081
npm run swipe-service        # Port 8082
npm run notification-service # Port 8083
npm run feed-worker          # Cron Background Worker
```

---

## 📡 API Endpoints Reference

### API Gateway (Port 8080)

| Method | Endpoint | Description | Auth Required |
|---|---|---|---|
| `GET` | `/health` | Gateway health check | No |
| `POST` | `/api/v1/auth/register` | Register new user | No |
| `POST` | `/api/v1/auth/login` | Authenticate and obtain JWT | No |
| `POST` | `/api/v1/profile` | Create/update user profile | Yes (Bearer) |
| `GET` | `/api/v1/profile/me` | Fetch authenticated profile | Yes (Bearer) |
| `GET` | `/api/v1/profile/:userId` | Get public profile | Yes (Bearer) |
| `PUT` | `/api/v1/profile/settings` | Update discovery settings | Yes (Bearer) |
| `GET` | `/api/v1/feed` | Fetch distance-sorted candidates | Yes (Bearer) |
| `POST` | `/api/v1/swipe` | Record swipe (Cassandra + Redis) | Yes (Bearer) |
| `GET` | `/api/v1/matches` | Get list of mutual matches | Yes (Bearer) |
| `GET` | `/api/v1/notifications` | Get user notifications | Yes (Bearer) |
| `GET` | `/api/v1/notifications/emails`| Inspect dispatched emails | No / Internal |

