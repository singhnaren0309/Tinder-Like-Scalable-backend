const { cassandraService } = require('../services/cassandra.service');
const { redisMatcherService } = require('../services/redis-matcher.service');
const { SwipeType } = require('../../../../shared/src/types/swipe.types');
const { createLogger } = require('../../../../shared/src/utils/logger');

const logger = createLogger('SwipeController');

async function handleSwipe(req, res) {
  try {
    const swiperId = req.headers['x-user-id'] || req.body.swiperId;
    const { targetId, swipeType } = req.body;

    if (!swiperId) {
      return res.status(401).json({ success: false, error: 'Unauthorized: Swiper ID missing' });
    }

    if (!targetId || !swipeType) {
      return res.status(400).json({
        success: false,
        error: 'targetId and swipeType (LEFT, RIGHT, SUPER_LIKE) are required',
      });
    }

    if (swiperId === targetId) {
      return res.status(400).json({ success: false, error: 'Users cannot swipe on themselves' });
    }

    const validSwipeTypes = Object.values(SwipeType);
    if (!validSwipeTypes.includes(swipeType)) {
      return res.status(400).json({
        success: false,
        error: `Invalid swipeType. Allowed values: ${validSwipeTypes.join(', ')}`,
      });
    }

    // 1. High WPS persistence: Append to Cassandra table
    await cassandraService.recordSwipe(swiperId, targetId, swipeType);

    // 2. Atomic mutual match evaluation in Redis
    const matchResult = await redisMatcherService.processSwipe(swiperId, targetId, swipeType);

    logger.info(
      `Swipe processed: ${swiperId} -> ${targetId} (${swipeType}) | isMatch=${matchResult.isMatch}`
    );

    res.status(200).json({
      success: true,
      swiperId,
      targetId,
      swipeType,
      isMatch: matchResult.isMatch,
      matchId: matchResult.matchId,
      message: matchResult.isMatch
        ? "It's a Match! 🎉"
        : swipeType === 'RIGHT'
        ? 'Swipe right recorded. Notification triggered if eligible.'
        : 'Swipe left recorded.',
    });
  } catch (err) {
    logger.error(`Error processing swipe: ${err.message}`);
    res.status(500).json({ success: false, error: err.message });
  }
}

async function getUserSwipeHistory(req, res) {
  try {
    const swiperId = req.headers['x-user-id'] || req.params.userId;
    if (!swiperId) {
      return res.status(401).json({ success: false, error: 'User ID is required' });
    }

    const history = await cassandraService.getPastSwipesBySwiper(swiperId);
    res.status(200).json({
      success: true,
      count: history.length,
      history,
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
}

module.exports = {
  handleSwipe,
  getUserSwipeHistory,
};

