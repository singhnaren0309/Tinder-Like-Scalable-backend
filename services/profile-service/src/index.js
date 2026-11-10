const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
dotenv.config();

const { createLogger } = require('../../../shared/src/utils/logger');
const authController = require('./controllers/auth.controller');
const profileController = require('./controllers/profile.controller');
const feedController = require('./controllers/feed.controller');

const logger = createLogger('ProfileService');
const app = express();
const PORT = parseInt(process.env.PROFILE_SERVICE_PORT || '8081', 10);

app.use(cors());
app.use(express.json());

// Health Check
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'OK', service: 'profile-service' });
});

// Authentication routes
app.post('/api/v1/auth/register', authController.register);
app.post('/api/v1/auth/login', authController.login);

// Profile routes
app.post('/api/v1/profile', profileController.createOrUpdateProfile);
app.get('/api/v1/profile/me', profileController.getMyProfile);
app.get('/api/v1/profile/:userId', profileController.getProfileById);
app.put('/api/v1/profile/settings', profileController.updateSettings);

// Matches routes
app.get('/api/v1/matches', profileController.getMatches);

// Feed route
app.get('/api/v1/feed', feedController.getFeed);

if (require.main === module) {
  app.listen(PORT, () => {
    logger.info(`🚀 Profile Service running on port ${PORT}`);
  });
}

module.exports = app;

