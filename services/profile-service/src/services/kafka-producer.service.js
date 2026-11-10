const { Kafka } = require('kafkajs');
const { KafkaEventType } = require('../../../../shared/src/types/event.types');
const { createLogger } = require('../../../../shared/src/utils/logger');

const logger = createLogger('ProfileKafkaProducer');

class ProfileKafkaProducerService {
  constructor() {
    this.kafka = null;
    this.producer = null;
    this.isConnected = false;
    this.topic = process.env.KAFKA_TOPIC_PROFILE_EVENTS || 'tinder.profile.events';
    this.localListeners = [];

    this.init();
  }

  async init() {
    if (process.env.USE_MOCK_INFRA_FALLBACK === 'true') {
      logger.info('USE_MOCK_INFRA_FALLBACK=true: Initializing in-memory Kafka CDC event bus.');
      this.isConnected = false;
      return;
    }
    try {
      const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
      this.kafka = new Kafka({
        clientId: 'profile-service-producer',
        brokers,
        connectionTimeout: 1500,
      });

      this.producer = this.kafka.producer();
      await this.producer.connect();
      this.isConnected = true;
      logger.info(`Connected to Kafka brokers at ${brokers.join(', ')}`);
    } catch (err) {
      logger.warn(`Kafka connection failed (${err.message}). Using local in-memory event bus for CDC.`);
      this.isConnected = false;
    }
  }

  subscribe(callback) {
    this.localListeners.push(callback);
  }

  async emitProfileCDC(eventType, profile) {
    const payload = {
      eventType,
      timestamp: new Date().toISOString(),
      profile,
    };

    logger.info(`[CDC] Publishing ${eventType} for userId=${profile.userId} to Kafka topic [${this.topic}]`);

    for (const listener of this.localListeners) {
      try {
        listener(payload);
      } catch (err) {
        logger.error(`Error dispatching to local listener: ${err.message}`);
      }
    }

    if (this.isConnected && this.producer) {
      try {
        await this.producer.send({
          topic: this.topic,
          messages: [
            {
              key: profile.userId,
              value: JSON.stringify(payload),
            },
          ],
        });
      } catch (err) {
        logger.error(`Failed to publish message to Kafka: ${err.message}`);
      }
    }
  }
}

const profileKafkaProducer = new ProfileKafkaProducerService();

module.exports = {
  profileKafkaProducer,
  ProfileKafkaProducerService,
};

