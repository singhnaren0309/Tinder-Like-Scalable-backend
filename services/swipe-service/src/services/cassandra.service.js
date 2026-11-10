const cassandra = require('cassandra-driver');
const { createLogger } = require('../../../../shared/src/utils/logger');

const logger = createLogger('CassandraService');

class CassandraService {
  constructor() {
    this.client = null;
    this.isConnected = false;
    this.keyspace = process.env.CASSANDRA_KEYSPACE || 'tinder_swipe_keyspace';
    this.inMemorySwipes = new Map();

    this.init();
  }

  async init() {
    if (process.env.USE_MOCK_INFRA_FALLBACK === 'true') {
      logger.info('USE_MOCK_INFRA_FALLBACK=true: Initializing in-memory Cassandra high-WPS storage engine.');
      this.isConnected = false;
      return;
    }
    try {
      const contactPoints = (process.env.CASSANDRA_CONTACT_POINTS || 'localhost').split(',');
      const localDataCenter = process.env.CASSANDRA_LOCAL_DC || 'datacenter1';

      this.client = new cassandra.Client({
        contactPoints,
        localDataCenter,
        keyspace: this.keyspace,
        socketOptions: { connectTimeout: 1500 },
      });

      await this.client.connect();
      this.isConnected = true;
      logger.info(`Connected to Apache Cassandra cluster at ${contactPoints.join(', ')}`);
    } catch (err) {
      logger.warn(`Cassandra connection failed (${err.message}). Using high-performance in-memory fallback.`);
      this.isConnected = false;
    }
  }

  async recordSwipe(swiperId, targetId, swipeType) {
    const record = {
      swiperId,
      targetId,
      swipeType,
      createdAt: new Date(),
    };

    if (this.isConnected && this.client) {
      const query = `
        INSERT INTO swipes (swiper_id, target_id, swipe_type, created_at)
        VALUES (?, ?, ?, ?);
      `;
      await this.client.execute(query, [swiperId, targetId, swipeType, record.createdAt], {
        prepare: true,
      });

      const reverseQuery = `
        INSERT INTO swipes_received (target_id, swiper_id, swipe_type, created_at)
        VALUES (?, ?, ?, ?);
      `;
      this.client.execute(reverseQuery, [targetId, swiperId, swipeType, record.createdAt], {
        prepare: true,
      }).catch((e) => logger.error(`Failed to record reverse swipe: ${e.message}`));
    } else {
      if (!this.inMemorySwipes.has(swiperId)) {
        this.inMemorySwipes.set(swiperId, new Map());
      }
      this.inMemorySwipes.get(swiperId).set(targetId, record);
    }

    return record;
  }

  async getPastSwipesBySwiper(swiperId) {
    if (this.isConnected && this.client) {
      const query = 'SELECT swiper_id, target_id, swipe_type, created_at FROM swipes WHERE swiper_id = ?;';
      const result = await this.client.execute(query, [swiperId], { prepare: true });
      return result.rows.map((r) => ({
        swiperId: r.swiper_id.toString(),
        targetId: r.target_id.toString(),
        swipeType: r.swipe_type,
        createdAt: r.created_at,
      }));
    } else {
      const userSwipes = this.inMemorySwipes.get(swiperId);
      return userSwipes ? Array.from(userSwipes.values()) : [];
    }
  }
}

const cassandraService = new CassandraService();

module.exports = {
  cassandraService,
  CassandraService,
};

