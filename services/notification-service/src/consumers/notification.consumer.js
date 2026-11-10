const { Kafka } = require('kafkajs');
const { KafkaEventType } = require('../../../../shared/src/types/event.types');
const { createLogger } = require('../../../../shared/src/utils/logger');
const { emailService } = require('../services/email.service');
const { pushService } = require('../services/push.service');
const { swipeKafkaProducer } = require('../../../swipe-service/src/services/kafka-producer.service');
const { dbService } = require('../../../profile-service/src/services/db.service');

const logger = createLogger('NotificationKafkaConsumer');

class NotificationKafkaConsumer {
  constructor() {
    this.kafka = null;
    this.consumer = null;
    this.isConnected = false;

    this.init();
    this.listenToLocalBus();
  }

  async init() {
    if (process.env.USE_MOCK_INFRA_FALLBACK === 'true') {
      logger.info('USE_MOCK_INFRA_FALLBACK=true: Notification consumer using local in-memory event bus.');
      this.isConnected = false;
      return;
    }
    try {
      const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
      this.kafka = new Kafka({
        clientId: 'notification-service-consumer',
        brokers,
        connectionTimeout: 1500,
      });

      this.consumer = this.kafka.consumer({ groupId: 'notification-workers' });
      await this.consumer.connect();
      await this.consumer.subscribe({
        topics: [
          process.env.KAFKA_TOPIC_SWIPE_EVENTS || 'tinder.swipe.events',
          process.env.KAFKA_TOPIC_MATCH_EVENTS || 'tinder.match.events',
        ],
        fromBeginning: false,
      });

      this.isConnected = true;
      logger.info('Kafka consumer connected and subscribed to swipe and match topics.');

      await this.consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          if (!message.value) return;
          try {
            const data = JSON.parse(message.value.toString());
            await this.processMessage(data);
          } catch (err) {
            logger.error(`Error parsing message on ${topic}: ${err.message}`);
          }
        },
      });
    } catch (err) {
      logger.warn(`Kafka consumer connection failed (${err.message}). Using local in-memory event dispatcher.`);
      this.isConnected = false;
    }
  }

  listenToLocalBus() {
    swipeKafkaProducer.subscribeSwipe(async (payload) => {
      await this.handleSwipeEvent(payload);
    });

    swipeKafkaProducer.subscribeMatch(async (payload) => {
      await this.handleMatchEvent(payload);
    });
  }

  async processMessage(payload) {
    if (payload.eventType === KafkaEventType.SWIPE_EVENT) {
      await this.handleSwipeEvent(payload);
    } else if (payload.eventType === KafkaEventType.MATCH_CREATED) {
      await this.handleMatchEvent(payload);
    }
  }

  async handleSwipeEvent(payload) {
    if (!payload.isFirstSwipeRight) return;

    logger.info(`Processing first swipe right notification: ${payload.swiperId} -> ${payload.targetId}`);

    const targetUser = await dbService.findUserById(payload.targetId);

    // 1. Push notification to target user
    await pushService.sendPushNotification(
      payload.targetId,
      'Someone liked you! 👀',
      'Someone just swiped right on your profile. Swipe back to match!',
      'SWIPE_RIGHT',
      { swiperId: payload.swiperId }
    );

    // 2. Email alert
    if (targetUser && targetUser.email) {
      await emailService.sendSwipeRightAlertEmail(targetUser.email);
    }
  }

  async handleMatchEvent(payload) {
    logger.info(`Processing match notification for matchId=${payload.matchId}`);

    const user1 = await dbService.findUserById(payload.user1Id);
    const user2 = await dbService.findUserById(payload.user2Id);
    const profile1 = await dbService.getProfile(payload.user1Id);
    const profile2 = await dbService.getProfile(payload.user2Id);

    const name1 = profile1 ? profile1.displayName : 'Someone special';
    const name2 = profile2 ? profile2.displayName : 'Someone special';

    // 1. Push notification to User 1
    await pushService.sendPushNotification(
      payload.user1Id,
      "It's a Match! 🎉",
      `You and ${name2} liked each other! Start chatting now.`,
      'MATCH',
      { matchId: payload.matchId, partnerId: payload.user2Id }
    );

    // 2. Push notification to User 2
    await pushService.sendPushNotification(
      payload.user2Id,
      "It's a Match! 🎉",
      `You and ${name1} liked each other! Start chatting now.`,
      'MATCH',
      { matchId: payload.matchId, partnerId: payload.user1Id }
    );

    // 3. Email notifications to both users
    if (user1 && user1.email) {
      await emailService.sendMatchEmail(user1.email, name2, payload.matchId);
    }
    if (user2 && user2.email) {
      await emailService.sendMatchEmail(user2.email, name1, payload.matchId);
    }
  }
}

const notificationConsumer = new NotificationKafkaConsumer();

module.exports = {
  notificationConsumer,
  NotificationKafkaConsumer,
};

