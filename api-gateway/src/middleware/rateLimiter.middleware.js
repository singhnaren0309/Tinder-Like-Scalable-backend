const { getRedisClient } = require('../../../shared/src/utils/redis');
const { createLogger } = require('../../../shared/src/utils/logger');

const logger = createLogger('GatewayRateLimiter');

const RATE_LIMIT_WINDOW_SECONDS = 60; // 1 minute
const MAX_REQUESTS_PER_WINDOW = parseInt(process.env.RATE_LIMIT_MAX_REQUESTS || '120', 10);
const DAILY_SWIPE_LIMIT = parseInt(process.env.SWIPE_MAX_PER_DAY || '100', 10);
const SECONDS_IN_A_DAY = 86400;

async function rateLimiterMiddleware(req, res, next) {
  try {
    const redis = getRedisClient();
    const identifier = req.headers['x-user-id'] || req.ip || 'anonymous';

    // 1. General API Window Rate Limiting (Sliding / Counter window)
    const rateLimitKey = `ratelimit:reqs:${identifier}`;
    const currentRequests = await redis.incr(rateLimitKey);

    if (currentRequests === 1) {
      await redis.expire(rateLimitKey, RATE_LIMIT_WINDOW_SECONDS);
    }

    res.setHeader('X-RateLimit-Limit', MAX_REQUESTS_PER_WINDOW);
    res.setHeader('X-RateLimit-Remaining', Math.max(0, MAX_REQUESTS_PER_WINDOW - currentRequests));

    if (currentRequests > MAX_REQUESTS_PER_WINDOW) {
      logger.warn(`General rate limit exceeded for ${identifier}`);
      return res.status(429).json({
        success: false,
        error: 'Too Many Requests: Rate limit exceeded. Please slow down.',
        retryAfterSeconds: RATE_LIMIT_WINDOW_SECONDS,
      });
    }

    // 2. Specific Swipe Quota Rate Limiter (e.g. 100 swipes/day)
    if (req.method === 'POST' && req.path.startsWith('/api/v1/swipe')) {
      const swipeLimitKey = `ratelimit:swipes:daily:${identifier}`;
      const swipesToday = await redis.incr(swipeLimitKey);

      if (swipesToday === 1) {
        await redis.expire(swipeLimitKey, SECONDS_IN_A_DAY);
      }

      res.setHeader('X-SwipeLimit-Daily-Max', DAILY_SWIPE_LIMIT);
      res.setHeader('X-SwipeLimit-Daily-Remaining', Math.max(0, DAILY_SWIPE_LIMIT - swipesToday));

      if (swipesToday > DAILY_SWIPE_LIMIT) {
        logger.warn(`Daily swipe limit reached for user ${identifier}`);
        return res.status(429).json({
          success: false,
          error: 'Daily swipe quota exhausted. Upgrade to Tinder Gold or wait until tomorrow.',
          swipesToday,
          dailyLimit: DAILY_SWIPE_LIMIT,
        });
      }
    }

    next();
  } catch (error) {
    logger.error(`Rate limiter check failed: ${error.message}`);
    // Fail-open strategy to prevent blocking traffic if Redis encounters a transient hiccup
    next();
  }
}

module.exports = {
  rateLimiterMiddleware,
};

