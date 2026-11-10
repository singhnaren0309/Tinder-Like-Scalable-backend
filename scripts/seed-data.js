'use strict';

require('dotenv').config();

const bcrypt = require('bcryptjs');
const { dbService } = require('../services/profile-service/src/services/db.service');
const { profileKafkaProducer } = require('../services/profile-service/src/services/kafka-producer.service');
const { KafkaEventType } = require('../shared/src/types/event.types');
const { createLogger } = require('../shared/src/utils/logger');

const logger = createLogger('SeedData');

const SEED_PROFILES = [
  {
    email: 'alice@example.com',
    password: 'password123',
    displayName: 'Alice Johnson',
    age: 24,
    gender: 'FEMALE',
    interestedIn: 'MALE',
    bio: 'Software engineer who loves hiking, latte art, and Golden Retrievers 🐶',
    photos: ['https://images.unsplash.com/photo-1494790108377-be9c29b29330'],
    location: { lat: 37.7749, lon: -122.4194 }, // SF Downtown
    maxDistanceKm: 30,
    ageMin: 23,
    ageMax: 32,
  },
  {
    email: 'bob@example.com',
    password: 'password123',
    displayName: 'Bob Smith',
    age: 26,
    gender: 'MALE',
    interestedIn: 'FEMALE',
    bio: 'Architect, sourdough enthusiast, rock climber. Looking for spontaneous adventures 🧗‍♂️',
    photos: ['https://images.unsplash.com/photo-1507003211169-0a1dd7228f2d'],
    location: { lat: 37.7833, lon: -122.4167 }, // SF Mission (~1.1 km away)
    maxDistanceKm: 25,
    ageMin: 22,
    ageMax: 30,
  },
  {
    email: 'clara@example.com',
    password: 'password123',
    displayName: 'Clara Chen',
    age: 25,
    gender: 'FEMALE',
    interestedIn: 'MALE',
    bio: 'Product designer & foodie. Show me your favorite ramen spot 🍜',
    photos: ['https://images.unsplash.com/photo-1534528741775-53994a69daeb'],
    location: { lat: 37.7699, lon: -122.4469 }, // SF Haight-Ashbury (~2.5 km away)
    maxDistanceKm: 40,
    ageMin: 24,
    ageMax: 35,
  },
  {
    email: 'david@example.com',
    password: 'password123',
    displayName: 'David Miller',
    age: 28,
    gender: 'MALE',
    interestedIn: 'FEMALE',
    bio: "Musician and coffee roaster. Let's trade playlists 🎵☕",
    photos: ['https://images.unsplash.com/photo-1500648767791-00dcc994a43e'],
    location: { lat: 37.8044, lon: -122.2712 }, // Oakland (~12 km away)
    maxDistanceKm: 30,
    ageMin: 22,
    ageMax: 30,
  },
  {
    email: 'emma@example.com',
    password: 'password123',
    displayName: 'Emma Watson',
    age: 27,
    gender: 'FEMALE',
    interestedIn: 'MALE',
    bio: 'Biomedical researcher. Book nerd, marathon runner 📚🏃‍♀️',
    photos: ['https://images.unsplash.com/photo-1524504388940-b1c1722653e1'],
    location: { lat: 37.4419, lon: -122.143 }, // Palo Alto (~45 km away)
    maxDistanceKm: 50,
    ageMin: 25,
    ageMax: 35,
  },
  {
    email: 'frank@example.com',
    password: 'password123',
    displayName: 'Frank Castle',
    age: 31,
    gender: 'MALE',
    interestedIn: 'FEMALE',
    bio: 'Fitness trainer and motorcycle traveler 🏍️',
    photos: ['https://images.unsplash.com/photo-1519085360753-af0119f7cbe7'],
    location: { lat: 37.3382, lon: -121.8863 }, // San Jose (~70 km away - outside 50km radius)
    maxDistanceKm: 50,
    ageMin: 25,
    ageMax: 35,
  },
];

async function seedDatabase() {
  logger.info('🌱 Seeding database with realistic Tinder user profiles...');

  const seededUsers = [];

  for (const item of SEED_PROFILES) {
    let user = await dbService.findUserByEmail(item.email);
    if (!user) {
      const salt = await bcrypt.genSalt(10);
      const passwordHash = await bcrypt.hash(item.password, salt);
      user = await dbService.createUser(item.email, passwordHash);
    }

    const profile = await dbService.upsertProfile({
      userId: user.id,
      displayName: item.displayName,
      age: item.age,
      gender: item.gender,
      interestedIn: item.interestedIn,
      bio: item.bio,
      photos: item.photos,
      location: item.location,
      maxDistanceKm: item.maxDistanceKm,
      ageMin: item.ageMin,
      ageMax: item.ageMax,
      isActive: true,
      updatedAt: new Date(),
    });

    // Publish CDC event so Elasticsearch & listeners index it
    await profileKafkaProducer.emitProfileCDC(KafkaEventType.PROFILE_CREATED, profile);

    seededUsers.push({ user, profile });
    logger.info(`✅ Seeded: ${profile.displayName} (${profile.gender}, age ${profile.age})`);
  }

  logger.info(`✨ Successfully seeded ${seededUsers.length} user profiles.`);
  return seededUsers;
}

if (require.main === module) {
  seedDatabase()
    .then(() => process.exit(0))
    .catch((err) => {
      logger.error(`Seeding error: ${err.message}`);
      process.exit(1);
    });
}

module.exports = { SEED_PROFILES, seedDatabase };

