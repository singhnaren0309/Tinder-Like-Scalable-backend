const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');
const { createLogger } = require('../../../../shared/src/utils/logger');

const logger = createLogger('PostgresService');

class PostgresService {
  constructor() {
    this.pool = null;
    this.isConnected = false;

    // In-memory fallback repository
    this.users = new Map();
    this.profiles = new Map();
    this.matches = new Map();

    this.init();
  }

  async init() {
    if (process.env.USE_MOCK_INFRA_FALLBACK === 'true') {
      logger.info('USE_MOCK_INFRA_FALLBACK=true: Initializing in-memory PostgreSQL engine.');
      this.isConnected = false;
      return;
    }
    try {
      this.pool = new Pool({
        host: process.env.POSTGRES_HOST || 'localhost',
        port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
        user: process.env.POSTGRES_USER || 'postgres',
        password: process.env.POSTGRES_PASSWORD || 'postgres',
        database: process.env.POSTGRES_DB || 'tinder_profile_db',
        connectionTimeoutMillis: 1500,
      });

      const client = await this.pool.connect();
      client.release();
      this.isConnected = true;
      logger.info('Connected successfully to PostgreSQL database.');
    } catch (err) {
      logger.warn(`PostgreSQL connection failed (${err.message}). Using high-performance in-memory fallback.`);
      this.isConnected = false;
    }
  }

  // --- User Operations ---
  async createUser(email, passwordHash) {
    const user = {
      id: uuidv4(),
      email,
      passwordHash,
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    if (this.isConnected && this.pool) {
      const query = `
        INSERT INTO users (id, email, password_hash, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING *;
      `;
      const res = await this.pool.query(query, [
        user.id,
        user.email,
        user.passwordHash,
        user.createdAt,
        user.updatedAt,
      ]);
      return res.rows[0];
    } else {
      this.users.set(user.id, user);
      return user;
    }
  }

  async findUserByEmail(email) {
    if (this.isConnected && this.pool) {
      const res = await this.pool.query('SELECT * FROM users WHERE email = $1', [email]);
      return res.rows[0] || null;
    } else {
      for (const u of this.users.values()) {
        if (u.email === email) return u;
      }
      return null;
    }
  }

  async findUserById(id) {
    if (this.isConnected && this.pool) {
      const res = await this.pool.query('SELECT * FROM users WHERE id = $1', [id]);
      return res.rows[0] || null;
    } else {
      return this.users.get(id) || null;
    }
  }

  // --- Profile Operations ---
  async upsertProfile(profile) {
    profile.updatedAt = new Date();

    if (this.isConnected && this.pool) {
      const query = `
        INSERT INTO profiles (
          user_id, display_name, age, gender, interested_in, bio, photos,
          location_lat, location_lon, max_distance_km, age_min, age_max, is_active, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (user_id) DO UPDATE SET
          display_name = EXCLUDED.display_name,
          age = EXCLUDED.age,
          gender = EXCLUDED.gender,
          interested_in = EXCLUDED.interested_in,
          bio = EXCLUDED.bio,
          photos = EXCLUDED.photos,
          location_lat = EXCLUDED.location_lat,
          location_lon = EXCLUDED.location_lon,
          max_distance_km = EXCLUDED.max_distance_km,
          age_min = EXCLUDED.age_min,
          age_max = EXCLUDED.age_max,
          is_active = EXCLUDED.is_active,
          updated_at = EXCLUDED.updated_at
        RETURNING *;
      `;
      const res = await this.pool.query(query, [
        profile.userId,
        profile.displayName,
        profile.age,
        profile.gender,
        profile.interestedIn,
        profile.bio,
        profile.photos,
        profile.location.lat,
        profile.location.lon,
        profile.maxDistanceKm,
        profile.ageMin,
        profile.ageMax,
        profile.isActive,
        profile.updatedAt,
      ]);
      return profile;
    } else {
      this.profiles.set(profile.userId, profile);
      return profile;
    }
  }

  async getProfile(userId) {
    if (this.isConnected && this.pool) {
      const res = await this.pool.query('SELECT * FROM profiles WHERE user_id = $1', [userId]);
      if (res.rows.length === 0) return null;
      const r = res.rows[0];
      return {
        userId: r.user_id,
        displayName: r.display_name,
        age: r.age,
        gender: r.gender,
        interestedIn: r.interested_in,
        bio: r.bio,
        photos: r.photos || [],
        location: { lat: r.location_lat, lon: r.location_lon },
        maxDistanceKm: r.max_distance_km,
        ageMin: r.age_min,
        ageMax: r.age_max,
        isActive: r.is_active,
        updatedAt: r.updated_at,
      };
    } else {
      return this.profiles.get(userId) || null;
    }
  }

  async getAllActiveProfiles() {
    if (this.isConnected && this.pool) {
      const res = await this.pool.query('SELECT * FROM profiles WHERE is_active = true');
      return res.rows.map((r) => ({
        userId: r.user_id,
        displayName: r.display_name,
        age: r.age,
        gender: r.gender,
        interestedIn: r.interested_in,
        bio: r.bio,
        photos: r.photos || [],
        location: { lat: r.location_lat, lon: r.location_lon },
        maxDistanceKm: r.max_distance_km,
        ageMin: r.age_min,
        ageMax: r.age_max,
        isActive: r.is_active,
        updatedAt: r.updated_at,
      }));
    } else {
      return Array.from(this.profiles.values()).filter((p) => p.isActive);
    }
  }

  // --- Match Operations ---
  async createMatch(user1Id, user2Id) {
    // Canonical ordering to avoid duplicate matches (e.g. u1 < u2)
    const [u1, u2] = user1Id < user2Id ? [user1Id, user2Id] : [user2Id, user1Id];
    const match = {
      id: uuidv4(),
      user1Id: u1,
      user2Id: u2,
      matchedAt: new Date(),
      isActive: true,
    };

    if (this.isConnected && this.pool) {
      const query = `
        INSERT INTO matches (id, user1_id, user2_id, matched_at, is_active)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (user1_id, user2_id) DO NOTHING
        RETURNING *;
      `;
      const res = await this.pool.query(query, [
        match.id,
        match.user1Id,
        match.user2Id,
        match.matchedAt,
        match.isActive,
      ]);
      return res.rows[0] || match;
    } else {
      const key = `${u1}:${u2}`;
      if (!this.matches.has(key)) {
        this.matches.set(key, match);
      }
      return this.matches.get(key);
    }
  }

  async getMatchesForUser(userId) {
    if (this.isConnected && this.pool) {
      const res = await this.pool.query(
        'SELECT * FROM matches WHERE (user1_id = $1 OR user2_id = $1) AND is_active = true ORDER BY matched_at DESC',
        [userId]
      );
      return res.rows.map((r) => ({
        id: r.id,
        user1Id: r.user1_id,
        user2Id: r.user2_id,
        matchedAt: r.matched_at,
        isActive: r.is_active,
      }));
    } else {
      return Array.from(this.matches.values()).filter(
        (m) => (m.user1Id === userId || m.user2Id === userId) && m.isActive
      );
    }
  }
}

const dbService = new PostgresService();

module.exports = {
  dbService,
  PostgresService,
};

