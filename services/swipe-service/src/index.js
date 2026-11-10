const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
dotenv.config();

const { createLogger } = require('../../../shared/src/utils/logger');
const swipeController = require('./controllers/swipe.controller');

const logger = createLogger('SwipeService');
const app = express();
const PORT = parseInt(process.env.SWIPE_SERVICE_PORT || '8082', 10);

app.use(cors());
app.use(express.json());

// Health Check
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'OK', service: 'swipe-service' });
});

// Swipe routes
app.post('/api/v1/swipe', swipeController.handleSwipe);
app.get('/api/v1/swipe/history', swipeController.getUserSwipeHistory);
app.get('/api/v1/swipe/history/:userId', swipeController.getUserSwipeHistory);

if (require.main === module) {
  app.listen(PORT, () => {
    logger.info(`🚀 Swipe Service (Cassandra + Redis Matcher) running on port ${PORT}`);
  });
}

module.exports = app;

