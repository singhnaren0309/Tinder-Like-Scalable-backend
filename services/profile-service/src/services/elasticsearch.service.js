const { Client } = require('@elastic/elasticsearch');
const { calculateDistanceKm } = require('../../../../shared/src/utils/distance');
const { createLogger } = require('../../../../shared/src/utils/logger');
const { profileKafkaProducer } = require('./kafka-producer.service');
const { dbService } = require('./db.service');

const logger = createLogger('ElasticsearchService');

class ElasticsearchService {
  constructor() {
    this.client = null;
    this.isConnected = false;
    this.indexName = process.env.ELASTICSEARCH_INDEX || 'tinder_profiles';
    this.inMemoryIndex = new Map();

    this.init();
    this.listenToCDC();
  }

  async init() {
    if (process.env.USE_MOCK_INFRA_FALLBACK === 'true') {
      logger.info('USE_MOCK_INFRA_FALLBACK=true: Initializing in-memory geospatial search engine.');
      this.isConnected = false;
      return;
    }
    try {
      const node = process.env.ELASTICSEARCH_NODE || 'http://localhost:9200';
      this.client = new Client({
        node,
        maxRetries: 1,
        requestTimeout: 1500,
      });

      const ping = await this.client.ping();
      if (ping) {
        this.isConnected = true;
        logger.info(`Connected to Elasticsearch cluster at ${node}`);
        await this.ensureIndex();
      }
    } catch (err) {
      logger.warn(`Elasticsearch connection failed (${err.message}). Using in-memory geospatial search fallback.`);
      this.isConnected = false;
    }
  }

  async ensureIndex() {
    if (!this.isConnected || !this.client) return;
    try {
      const exists = await this.client.indices.exists({ index: this.indexName });
      if (!exists) {
        await this.client.indices.create({
          index: this.indexName,
          body: {
            mappings: {
              properties: {
                user_id: { type: 'keyword' },
                display_name: { type: 'text' },
                age: { type: 'integer' },
                gender: { type: 'keyword' },
                interested_in: { type: 'keyword' },
                bio: { type: 'text' },
                photos: { type: 'keyword' },
                location: { type: 'geo_point' },
                max_distance_km: { type: 'integer' },
                age_min: { type: 'integer' },
                age_max: { type: 'integer' },
                is_active: { type: 'boolean' },
                updated_at: { type: 'date' },
              },
            },
          },
        });
        logger.info(`Elasticsearch index '${this.indexName}' created with geo_point mappings.`);
      }
    } catch (err) {
      logger.error(`Error ensuring Elasticsearch index: ${err.message}`);
    }
  }

  listenToCDC() {
    profileKafkaProducer.subscribe(async (payload) => {
      logger.info(`[CDC -> Elasticsearch] Syncing profile for userId=${payload.profile.userId}`);
      await this.upsertProfile(payload.profile);
    });
  }

  async upsertProfile(profile) {
    this.inMemoryIndex.set(profile.userId, profile);

    if (this.isConnected && this.client) {
      try {
        await this.client.index({
          index: this.indexName,
          id: profile.userId,
          document: {
            user_id: profile.userId,
            display_name: profile.displayName,
            age: profile.age,
            gender: profile.gender,
            interested_in: profile.interestedIn,
            bio: profile.bio,
            photos: profile.photos,
            location: {
              lat: profile.location.lat,
              lon: profile.location.lon,
            },
            max_distance_km: profile.maxDistanceKm,
            age_min: profile.ageMin,
            age_max: profile.ageMax,
            is_active: profile.isActive,
            updated_at: profile.updatedAt,
          },
        });
      } catch (err) {
        logger.error(`Failed to index profile to Elasticsearch: ${err.message}`);
      }
    }
  }

  async searchCandidates(
    currentUserId,
    userLocation,
    maxDistanceKm,
    ageMin,
    ageMax,
    interestedIn,
    userGender,
    excludedUserIds,
    limit = 20
  ) {
    if (this.isConnected && this.client) {
      try {
        const mustClauses = [
          { term: { is_active: true } },
          { range: { age: { gte: ageMin, lte: ageMax } } },
        ];

        if (interestedIn === 'MALE' || interestedIn === 'FEMALE') {
          mustClauses.push({ term: { gender: interestedIn } });
        }

        if (userGender) {
          mustClauses.push({
            bool: {
              should: [
                { term: { interested_in: 'EVERYONE' } },
                { term: { interested_in: userGender } },
              ],
              minimum_should_match: 1,
            },
          });
        }

        const mustNotClauses = [
          { term: { user_id: currentUserId } },
          ...Array.from(excludedUserIds).map((id) => ({ term: { user_id: id } })),
        ];

        const queryBody = {
          query: {
            bool: {
              must: mustClauses,
              must_not: mustNotClauses,
              filter: {
                geo_distance: {
                  distance: `${maxDistanceKm}km`,
                  location: {
                    lat: userLocation.lat,
                    lon: userLocation.lon,
                  },
                },
              },
            },
          },
          sort: [
            {
              _geo_distance: {
                location: {
                  lat: userLocation.lat,
                  lon: userLocation.lon,
                },
                order: 'asc',
                unit: 'km',
                mode: 'min',
                distance_type: 'arc',
              },
            },
          ],
          size: limit,
        };

        const response = await this.client.search({
          index: this.indexName,
          body: queryBody,
        });

        return response.hits.hits.map((hit) => {
          const src = hit._source;
          const dist = hit.sort && hit.sort[0] ? Math.round(hit.sort[0] * 10) / 10 : undefined;
          return {
            userId: src.user_id,
            displayName: src.display_name,
            age: src.age,
            gender: src.gender,
            interestedIn: src.interested_in,
            bio: src.bio,
            photos: src.photos || [],
            location: { lat: src.location.lat, lon: src.location.lon },
            maxDistanceKm: src.max_distance_km,
            ageMin: src.age_min,
            ageMax: src.age_max,
            isActive: src.is_active,
            updatedAt: new Date(src.updated_at),
            distanceKm: dist,
          };
        });
      } catch (err) {
        logger.error(`Elasticsearch search failed: ${err.message}. Falling back to in-memory geospatial search.`);
      }
    }

    return this.searchCandidatesInMemory(
      currentUserId,
      userLocation,
      maxDistanceKm,
      ageMin,
      ageMax,
      interestedIn,
      userGender,
      excludedUserIds,
      limit
    );
  }

  async searchCandidatesInMemory(
    currentUserId,
    userLocation,
    maxDistanceKm,
    ageMin,
    ageMax,
    interestedIn,
    userGender,
    excludedUserIds,
    limit = 20
  ) {
    if (this.inMemoryIndex.size === 0) {
      const allProfiles = await dbService.getAllActiveProfiles();
      for (const p of allProfiles) {
        this.inMemoryIndex.set(p.userId, p);
      }
    }

    const candidates = [];

    for (const profile of this.inMemoryIndex.values()) {
      if (profile.userId === currentUserId) continue;
      if (excludedUserIds.has(profile.userId)) continue;
      if (!profile.isActive) continue;

      if (profile.age < ageMin || profile.age > ageMax) continue;

      if (interestedIn !== 'EVERYONE' && profile.gender !== interestedIn) continue;
      if (profile.interestedIn !== 'EVERYONE' && profile.interestedIn !== userGender) continue;

      const distance = calculateDistanceKm(userLocation, profile.location);
      if (distance <= maxDistanceKm) {
        candidates.push({
          ...profile,
          distanceKm: distance,
        });
      }
    }

    candidates.sort((a, b) => (a.distanceKm || 0) - (b.distanceKm || 0));

    return candidates.slice(0, limit);
  }
}

const elasticsearchService = new ElasticsearchService();

module.exports = {
  elasticsearchService,
  ElasticsearchService,
};

