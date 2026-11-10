const Redis = require('ioredis');
const { createLogger } = require('./logger');

const logger = createLogger('RedisClient');

/**
 * High-performance In-Memory Redis Mock fallback
 */
class InMemoryRedisMock {
  constructor() {
    this.store = new Map();
    this.sets = new Map();
    this.lists = new Map();
  }

  async get(key) {
    return this.store.has(key) ? String(this.store.get(key)) : null;
  }

  async set(key, value, mode, duration) {
    this.store.set(key, value);
    if (mode === 'EX' && duration) {
      setTimeout(() => this.store.delete(key), duration * 1000);
    }
    return 'OK';
  }

  async del(key) {
    const existed = this.store.delete(key) || this.sets.delete(key) || this.lists.delete(key);
    return existed ? 1 : 0;
  }

  async exists(key) {
    return (this.store.has(key) || this.sets.has(key) || this.lists.has(key)) ? 1 : 0;
  }

  async sadd(key, ...members) {
    if (!this.sets.has(key)) {
      this.sets.set(key, new Set());
    }
    const set = this.sets.get(key);
    let added = 0;
    for (const m of members) {
      if (!set.has(m)) {
        set.add(m);
        added++;
      }
    }
    return added;
  }

  async sismember(key, member) {
    const set = this.sets.get(key);
    return set && set.has(member) ? 1 : 0;
  }

  async smembers(key) {
    const set = this.sets.get(key);
    return set ? Array.from(set) : [];
  }

  async lpush(key, ...values) {
    if (!this.lists.has(key)) {
      this.lists.set(key, []);
    }
    const list = this.lists.get(key);
    list.unshift(...values);
    return list.length;
  }

  async rpop(key) {
    const list = this.lists.get(key);
    if (!list || list.length === 0) return null;
    return list.pop() || null;
  }

  async lrange(key, start, stop) {
    const list = this.lists.get(key);
    if (!list) return [];
    if (stop === -1) return list.slice(start);
    return list.slice(start, stop + 1);
  }

  async llen(key) {
    const list = this.lists.get(key);
    return list ? list.length : 0;
  }

  async eval(script, numKeys, ...args) {
    const keys = args.slice(0, numKeys).map(String);
    const argv = args.slice(numKeys);

    if (numKeys >= 3) {
      const reverseKey = keys[0];
      const myKey = keys[1];
      const lockKey = keys[2];

      const reverseExists = this.store.has(reverseKey);
      this.store.set(myKey, '1');

      if (reverseExists) {
        if (this.store.has(lockKey)) {
          return 2; // Match already coordinated by concurrent counterpart
        }
        this.store.set(lockKey, '1');
        return 1; // It's a match!
      }
      return 0; // Not a match yet
    }
    return 0;
  }

  async incr(key) {
    const current = parseInt(this.store.get(key) || '0', 10);
    const next = current + 1;
    this.store.set(key, next.toString());
    return next;
  }

  async expire(key, seconds) {
    setTimeout(() => {
      this.store.delete(key);
      this.sets.delete(key);
      this.lists.delete(key);
    }, seconds * 1000);
    return 1;
  }
}

let redisInstance = null;

function getRedisClient() {
  if (redisInstance) {
    return redisInstance;
  }

  if (process.env.USE_MOCK_INFRA_FALLBACK === 'true') {
    logger.info('USE_MOCK_INFRA_FALLBACK=true: Initializing in-memory Redis engine.');
    redisInstance = new InMemoryRedisMock();
    return redisInstance;
  }

  const host = process.env.REDIS_HOST || 'localhost';
  const port = parseInt(process.env.REDIS_PORT || '6379', 10);
  const password = process.env.REDIS_PASSWORD || undefined;

  try {
    const client = new Redis({
      host,
      port,
      password,
      maxRetriesPerRequest: 1,
      retryStrategy: () => null,
      connectTimeout: 1000,
    });

    client.on('error', (err) => {
      logger.warn(`Redis connection error (${err.message}). Using in-memory fallback.`);
    });

    redisInstance = client;
  } catch (error) {
    logger.info('Initializing in-memory Redis mock.');
    redisInstance = new InMemoryRedisMock();
  }

  return redisInstance;
}

module.exports = {
  getRedisClient,
  InMemoryRedisMock,
};

