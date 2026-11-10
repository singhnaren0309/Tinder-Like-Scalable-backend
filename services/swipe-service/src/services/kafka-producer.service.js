const { Kafka } = require('kafkajs');
const { KafkaEventType } = require('../../../../shared/src/types/event.types');
const { createLogger } = require('../../../../shared/src/utils/logger');

const logger = createLogger('SwipeKafkaProducer');

class SwipeKafkaProducerService {
  constructor() {
    this.kafka = null;
    this.producer = null;
    this.isConnected = false;
    this.swipeTopic = process.env.KAFKA_TOPIC_SWIPE_EVENTS || 'tinder.swipe.events';
    this.matchTopic = process.env.KAFKA_TOPIC_MATCH_EVENTS || 'tinder.match.events';

    this.swipeListeners = [];
    this.matchListeners = [];

    this.init();
  }

  async init() {
    if (process.env.USE_MOCK_INFRA_FALLBACK === 'true') {
      logger.info('USE_MOCK_INFRA_FALLBACK=true: Initializing in-memory Kafka event bus.');
      this.isConnected = false;
      return;
    }
    try {
      const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
      this.kafka = new Kafka({
        clientId: 'swipe-service-producer',
        brokers,
        connectionTimeout: 1500,
      });

      this.producer = this.kafka.producer();
      await this.producer.connect();
      this.isConnected = true;
      logger.info(`Connected to Kafka brokers for Swipe Service at ${brokers.join(', ')}`);
    } catch (err) {
      logger.warn(`Kafka connection failed (${err.message}). Using local in-memory event bus.`);
      this.isConnected = false;
    }
  }

  subscribeSwipe(callback) {
    this.swipeListeners.push(callback);
  }

  subscribeMatch(callback) {
    this.matchListeners.push(callback);
  }

  async emitSwipeEvent(swiperId, targetId, swipeType, isFirstSwipeRight) {
    const payload = {
      eventType: KafkaEventType.SWIPE_EVENT,
      timestamp: new Date().toISOString(),
      swiperId,
      targetId,
      swipeType,
      isFirstSwipeRight,
    };

    logger.info(`[Kafka] Emitting swipe event: ${swiperId} -> ${targetId} (${swipeType})`);

    for (const listener of this.swipeListeners) {
      try {
        listener(payload);
      } catch (err) {
        logger.error(`Error in swipe event listener: ${err.message}`);
      }
    }

    if (this.isConnected && this.producer) {
      try {
        await this.producer.send({
          topic: this.swipeTopic,
          messages: [{ key: swiperId, value: JSON.stringify(payload) }],
        });
      } catch (err) {
        logger.error(`Failed to publish swipe to Kafka: ${err.message}`);
      }
    }
  }

  async emitMatchEvent(matchId, user1Id, user2Id) {
    const payload = {
      eventType: KafkaEventType.MATCH_CREATED,
      timestamp: new Date().toISOString(),
      matchId,
      user1Id,
      user2Id,
    };

    logger.info(`[Kafka] Emitting MATCH_CREATED event: matchId=${matchId} (${user1Id} <-> ${user2Id})`);

    for (const listener of this.matchListeners) {
      try {
        listener(payload);
      } catch (err) {
        logger.error(`Error in match event listener: ${err.message}`);
      }
    }

    if (this.isConnected && this.producer) {
      try {
        await this.producer.send({
          topic: this.matchTopic,
          messages: [{ key: matchId, value: JSON.stringify(payload) }],
        });
      } catch (err) {
        logger.error(`Failed to publish match to Kafka: ${err.message}`);
      }
    }
  }
}

const swipeKafkaProducer = new SwipeKafkaProducerService();

module.exports = {
  swipeKafkaProducer,
  SwipeKafkaProducerService,
};

