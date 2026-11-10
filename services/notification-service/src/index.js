const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
dotenv.config();

const { createLogger } = require('../../../shared/src/utils/logger');
const { pushService } = require('./services/push.service');
const { emailService } = require('./services/email.service');
require('./consumers/notification.consumer'); // Initialize consumer on startup

const logger = createLogger('NotificationService');
const app = express();
const PORT = parseInt(process.env.NOTIFICATION_SERVICE_PORT || '8083', 10);

app.use(cors());
app.use(express.json());

// Health Check
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'OK', service: 'notification-service' });
});

// Notifications for authenticated user
app.get('/api/v1/notifications', (req, res) => {
  const userId = req.headers['x-user-id'] || req.query.userId;
  if (!userId) {
    return res.status(401).json({ success: false, error: 'User ID is required' });
  }

  const notifications = pushService.getNotificationsForUser(userId);
  res.status(200).json({
    success: true,
    count: notifications.length,
    notifications,
  });
});

// Inspection route to view dispatched mock emails
app.get('/api/v1/notifications/emails', (req, res) => {
  const emails = emailService.getSentEmails();
  res.status(200).json({
    success: true,
    count: emails.length,
    emails,
  });
});

if (require.main === module) {
  app.listen(PORT, () => {
    logger.info(`🚀 Notification Service running on port ${PORT}`);
  });
}

module.exports = app;

