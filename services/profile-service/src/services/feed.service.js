const { getRedisClient } = require('../../../../shared/src/utils/redis');
const { createLogger } = require('../../../../shared/src/utils/logger');
const { dbService } = require('./db.service');
const { elasticsearchService } = require('./elasticsearch.service');

const logger = createLogger('FeedService');

class FeedService {
  async getFeedForUser(userId, limit = 10) {
    const userProfile = await dbService.getProfile(userId);
    if (!userProfile) {
      throw new Error(`Profile not found for userId: ${userId}. Complete profile before requesting feed.`);
    }

    const redis = getRedisClient();
    const stackKey = `feed:stack:${userId}`;

    // 1. Try reading pre-computed candidates from Redis Stack Cache (O(1) pop/slice)
    const cachedCandidateIds = await redis.lrange(stackKey, 0, limit - 1);

    if (cachedCandidateIds && cachedCandidateIds.length > 0) {
      logger.info(`Serving feed for userId=${userId} directly from Redis Stack Cache (${cachedCandidateIds.length} profiles)`);

      const candidates = [];
      for (const candidateId of cachedCandidateIds) {
        const profile = await dbService.getProfile(candidateId);
        if (profile) {
          candidates.push(profile);
        }
      }

      if (candidates.length > 0) {
        for (let i = 0; i < candidates.length; i++) {
          await redis.rpop(stackKey);
        }
        return candidates;
      }
    }

    // 2. Dynamic generation via Elasticsearch
    logger.info(`Redis stack cache empty for userId=${userId}. Generating feed dynamically via Elasticsearch.`);
    const swipedIds = await this.getSwipedUserIds(userId);

    const candidates = await elasticsearchService.searchCandidates(
      userId,
      userProfile.location,
      userProfile.maxDistanceKm,
      userProfile.ageMin,
      userProfile.ageMax,
      userProfile.interestedIn,
      userProfile.gender,
      swipedIds,
      limit
    );

    return candidates;
  }

  async getSwipedUserIds(userId) {
    const redis = getRedisClient();
    const setKey = `user:swipes:${userId}`;
    const members = await redis.smembers(setKey);
    return new Set(members);
  }
}

const feedService = new FeedService();

module.exports = {
  feedService,
  FeedService,
};

