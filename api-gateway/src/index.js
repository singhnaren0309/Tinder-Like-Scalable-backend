const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
dotenv.config();

const { createLogger } = require('../../shared/src/utils/logger');
const { authMiddleware } = require('./middleware/auth.middleware');
const { rateLimiterMiddleware } = require('./middleware/rateLimiter.middleware');
const { errorHandler } = require('./middleware/error.middleware');
const proxyRoutes = require('./routes/proxy.routes');

const logger = createLogger('APIGateway');
const app = express();
const PORT = parseInt(process.env.GATEWAY_PORT || '8080', 10);

app.use(cors());
app.use(express.json());

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'OK',
    service: 'tinder-api-gateway',
    timestamp: new Date().toISOString(),
  });
});

// Gateway Middlewares
app.use(authMiddleware);
app.use(rateLimiterMiddleware);

// Routes
app.use(proxyRoutes);

// Error handling
app.use(errorHandler);

if (require.main === module) {
  app.listen(PORT, () => {
    logger.info(`🚀 Tinder API Gateway listening on port ${PORT}`);
    logger.info(`➡️ Profile Service: ${process.env.PROFILE_SERVICE_URL || 'http://localhost:8081'}`);
    logger.info(`➡️ Swipe Service: ${process.env.SWIPE_SERVICE_URL || 'http://localhost:8082'}`);
    logger.info(`➡️ Notification Service: ${process.env.NOTIFICATION_SERVICE_URL || 'http://localhost:8083'}`);
  });
}

module.exports = app;

