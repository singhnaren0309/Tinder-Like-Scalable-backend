const { feedService } = require('../services/feed.service');
const { createLogger } = require('../../../../shared/src/utils/logger');

const logger = createLogger('FeedController');

async function getFeed(req, res) {
  try {
    const userId = req.headers['x-user-id'] || req.query.userId;
    if (!userId) {
      return res.status(401).json({ success: false, error: 'User ID is required to fetch feed' });
    }

    const limit = req.query.limit ? parseInt(req.query.limit, 10) : 10;
    const candidates = await feedService.getFeedForUser(userId, limit);

    logger.info(`Returned ${candidates.length} candidate profiles for userId=${userId}`);

    res.status(200).json({
      success: true,
      count: candidates.length,
      feed: candidates.map((c) => ({
        userId: c.userId,
        displayName: c.displayName,
        age: c.age,
        gender: c.gender,
        bio: c.bio,
        photos: c.photos,
        location: c.location,
        distanceKm: c.distanceKm,
      })),
    });
  } catch (err) {
    logger.error(`Error fetching feed: ${err.message}`);
    res.status(500).json({ success: false, error: err.message });
  }
}

module.exports = {
  getFeed,
};

