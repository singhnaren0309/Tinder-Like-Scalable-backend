'use strict';

require('dotenv').config();

const { getRedisClient } = require('../../../shared/src/utils/redis');
const { createLogger } = require('../../../shared/src/utils/logger');
const { dbService } = require('../../../services/profile-service/src/services/db.service');
const { cassandraService } = require('../../../services/swipe-service/src/services/cassandra.service');
const { elasticsearchService } = require('../../../services/profile-service/src/services/elasticsearch.service');

const logger = createLogger('FeedStackGenerator');
const TARGET_STACK_SIZE = 20;

class FeedGeneratorWorker {
  /**
   * Refreshes the Redis Feed Stack cache for a specific user.
   * @param {string} userId
   * @returns {Promise<number>} new stack length
   */
  async refreshUserFeedStack(userId) {
    const redis = getRedisClient();
    const stackKey = `feed:stack:${userId}`;

    // 1. Check current stack depth
    const currentLength = await redis.llen(stackKey);
    if (currentLength >= TARGET_STACK_SIZE) {
      return currentLength;
    }

    const neededCount = TARGET_STACK_SIZE - currentLength;

    // 2. Fetch user's profile & preferences
    const profile = await dbService.getProfile(userId);
    if (!profile || !profile.isActive) {
      return 0;
    }

    // 3. Collect past swipes from both Cassandra & Redis to eliminate already-swiped users
    const excludedIds = new Set();

    // From Cassandra (permanent store of all historical swipes)
    try {
      const cassandraSwipes = await cassandraService.getPastSwipesBySwiper(userId);
      for (const s of cassandraSwipes) {
        excludedIds.add(s.targetId);
      }
    } catch (e) {
      logger.warn(`Could not fetch Cassandra swipes for user ${userId}: ${e.message}`);
    }

    // From Redis (real-time hot set)
    const redisSwiped = await redis.smembers(`user:swipes:${userId}`);
    for (const id of redisSwiped) {
      excludedIds.add(id);
    }

    // Also exclude profiles already in the stack
    const currentStackMembers = await redis.lrange(stackKey, 0, -1);
    for (const id of currentStackMembers) {
      excludedIds.add(id);
    }

    // 4. Query Elasticsearch for candidate profiles within radius, sorted by distance
    const candidates = await elasticsearchService.searchCandidates(
      userId,
      profile.location,
      profile.maxDistanceKm,
      profile.ageMin,
      profile.ageMax,
      profile.interestedIn,
      profile.gender,
      excludedIds,
      neededCount
    );

    if (candidates.length === 0) {
      return currentLength;
    }

    // 5. Push candidates to Redis Stack Cache (LPUSH)
    const candidateIds = candidates.map((c) => c.userId);
    await redis.lpush(stackKey, ...candidateIds);

    logger.info(
      `[FeedCron] Pre-populated ${candidateIds.length} candidate profiles into Redis Stack Cache for userId=${userId}`
    );

    return currentLength + candidateIds.length;
  }

  /**
   * Processes all active profiles in the system.
   * @returns {Promise<void>}
   */
  async runBatchGeneration() {
    logger.info('[FeedCron] Starting batch feed generation job...');
    const activeProfiles = await dbService.getAllActiveProfiles();

    let refreshedCount = 0;
    for (const profile of activeProfiles) {
      try {
        await this.refreshUserFeedStack(profile.userId);
        refreshedCount++;
      } catch (err) {
        logger.error(`Error generating feed for userId=${profile.userId}: ${err.message}`);
      }
    }

    logger.info(`[FeedCron] Completed batch feed generation for ${refreshedCount} active users.`);
  }
}

const feedGeneratorWorker = new FeedGeneratorWorker();

module.exports = { FeedGeneratorWorker, feedGeneratorWorker };

