const { createLogger } = require('../../../../shared/src/utils/logger');

const logger = createLogger('PushNotificationService');

class PushNotificationService {
  constructor() {
    this.notifications = [];
  }

  async sendPushNotification(userId, title, message, type, metadata) {
    const notification = {
      id: Math.random().toString(36).substring(2, 11),
      userId,
      title,
      message,
      type,
      metadata,
      createdAt: new Date(),
    };

    this.notifications.push(notification);
    logger.info(`🔔 [PUSH NOTIFICATION] To: userId=${userId} | "${title}" - ${message}`);
    return notification;
  }

  getNotificationsForUser(userId) {
    return this.notifications.filter((n) => n.userId === userId);
  }
}

const pushService = new PushNotificationService();

module.exports = {
  pushService,
  PushNotificationService,
};

