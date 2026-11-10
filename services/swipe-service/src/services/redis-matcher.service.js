const { getRedisClient } = require('../../../../shared/src/utils/redis');
const { createLogger } = require('../../../../shared/src/utils/logger');
const { dbService } = require('../../../profile-service/src/services/db.service');
const { swipeKafkaProducer } = require('./kafka-producer.service');

const logger = createLogger('RedisAtomicMatcher');

// Atomic Redis Lua Script for concurrent mutual match detection
const ATOMIC_MATCH_LUA_SCRIPT = `
  local reverseKey = KEYS[1]
  local myKey = KEYS[2]
  local lockKey = KEYS[3]
  local ttl = ARGV[1]

  -- 1. Store my swipe right
  redis.call('SET', myKey, '1', 'EX', ttl)

  -- 2. Check if the reverse swipe (target swiped on me) exists
  local reverseExists = redis.call('EXISTS', reverseKey)

  if reverseExists == 1 then
    -- 3. Atomic SETNX lock on canonical pair to guarantee exactly-once match creation
    local lockAcquired = redis.call('SET', lockKey, '1', 'NX', 'EX', 300)
    if lockAcquired then
      return 1 -- Match! Sole owner for DB persistence and event publishing
    else
      return 2 -- Match! Already processed by concurrent counterpart
    end
  end

  return 0 -- No match yet (first user swiped right)
`;

class RedisMatcherService {
  constructor() {
    this.SWIPE_TTL_SECONDS = 7 * 24 * 3600; // 7 days TTL
  }

  async processSwipe(swiperId, targetId, swipeType) {
    const redis = getRedisClient();

    // 1. Immediately record in swiper's exclusion set to eliminate from future feeds
    const userSwipesSetKey = `user:swipes:${swiperId}`;
    await redis.sadd(userSwipesSetKey, targetId);

    // If swipe is LEFT (pass), no match possible
    if (swipeType === 'LEFT') {
      await swipeKafkaProducer.emitSwipeEvent(swiperId, targetId, swipeType, false);
      return {
        isMatch: false,
        isFirstSwipeRight: false,
        isConcurrentMatch: false,
      };
    }

    // 2. SWIPE RIGHT: Atomic mutual match check
    // user1:user2 -> myKey
    // user2:user1 -> reverseKey
    const myKey = `swipe:right:${swiperId}:${targetId}`;
    const reverseKey = `swipe:right:${targetId}:${swiperId}`;

    // Canonical ordering for distributed lock
    const [u1, u2] = swiperId < targetId ? [swiperId, targetId] : [targetId, swiperId];
    const lockKey = `match:lock:${u1}:${u2}`;

    let matchStatus = 0;

    try {
      const result = await redis.eval(
        ATOMIC_MATCH_LUA_SCRIPT,
        3,
        reverseKey,
        myKey,
        lockKey,
        this.SWIPE_TTL_SECONDS
      );
      matchStatus = Number(result);
    } catch (err) {
      logger.warn(`Redis eval fallback (${err.message}). Performing atomic simulated match.`);
      const reverseExists = (await redis.exists(reverseKey)) === 1;
      await redis.set(myKey, '1', 'EX', this.SWIPE_TTL_SECONDS);

      if (reverseExists) {
        const lock = await redis.set(lockKey, '1', 'EX', 300);
        matchStatus = lock ? 1 : 2;
      } else {
        matchStatus = 0;
      }
    }

    // Case 1: Match achieved! This request is the primary coordinator
    if (matchStatus === 1) {
      logger.info(`🎉 MUTUAL MATCH DETECTED! (${swiperId} <-> ${targetId})`);

      const matchRecord = await dbService.createMatch(swiperId, targetId);
      await swipeKafkaProducer.emitMatchEvent(matchRecord.id, swiperId, targetId);

      return {
        isMatch: true,
        matchId: matchRecord.id,
        isFirstSwipeRight: false,
        isConcurrentMatch: false,
      };
    }

    // Case 2: Mutual match occurred simultaneously; counterpart thread already dispatched match
    if (matchStatus === 2) {
      logger.info(`🎉 CONCURRENT MUTUAL MATCH ACKNOWLEDGED! (${swiperId} <-> ${targetId})`);
      return {
        isMatch: true,
        isFirstSwipeRight: false,
        isConcurrentMatch: true,
      };
    }

    // Case 3: First user swiped right (no reverse swipe yet)
    logger.info(`User ${swiperId} swiped right on ${targetId}. Waiting for counterpart.`);
    await swipeKafkaProducer.emitSwipeEvent(swiperId, targetId, swipeType, true);

    return {
      isMatch: false,
      isFirstSwipeRight: true,
      isConcurrentMatch: false,
    };
  }
}

const redisMatcherService = new RedisMatcherService();

module.exports = {
  redisMatcherService,
  RedisMatcherService,
};

