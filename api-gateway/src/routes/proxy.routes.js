const { Router } = require('express');
const axios = require('axios');
const { createLogger } = require('../../../shared/src/utils/logger');

const logger = createLogger('GatewayRouter');
const router = Router();

const PROFILE_SERVICE_URL = process.env.PROFILE_SERVICE_URL || 'http://localhost:8081';
const SWIPE_SERVICE_URL = process.env.SWIPE_SERVICE_URL || 'http://localhost:8082';
const NOTIFICATION_SERVICE_URL = process.env.NOTIFICATION_SERVICE_URL || 'http://localhost:8083';

/**
 * Generic reverse proxy forwarder that preserves HTTP headers and body
 */
async function forwardRequest(targetBaseUrl, req, res) {
  const targetUrl = `${targetBaseUrl}${req.originalUrl}`;
  try {
    const response = await axios({
      method: req.method,
      url: targetUrl,
      data: req.body,
      headers: {
        ...req.headers,
        host: undefined, // Let axios compute correct host
      },
      validateStatus: () => true, // Forward any status code back to client
    });

    res.status(response.status).json(response.data);
  } catch (err) {
    logger.error(`Forwarding error to ${targetUrl}: ${err.message}`);
    res.status(502).json({
      success: false,
      error: `Bad Gateway: Target service unreachable (${targetUrl})`,
      details: err.message,
    });
  }
}

// 1. Profile Service Routes (Auth, Profile, Settings, Feed, Matches)
router.all('/api/v1/auth*', (req, res) => forwardRequest(PROFILE_SERVICE_URL, req, res));
router.all('/api/v1/profile*', (req, res) => forwardRequest(PROFILE_SERVICE_URL, req, res));
router.all('/api/v1/feed*', (req, res) => forwardRequest(PROFILE_SERVICE_URL, req, res));
router.all('/api/v1/matches*', (req, res) => forwardRequest(PROFILE_SERVICE_URL, req, res));

// 2. Swipe Service Routes (High-throughput Swiping, Atomic Match)
router.all('/api/v1/swipe*', (req, res) => forwardRequest(SWIPE_SERVICE_URL, req, res));

// 3. Notification Service Routes (Notifications, Subscriptions)
router.all('/api/v1/notifications*', (req, res) => forwardRequest(NOTIFICATION_SERVICE_URL, req, res));

module.exports = router;

