const { dbService } = require('../services/db.service');
const { profileKafkaProducer } = require('../services/kafka-producer.service');
const { KafkaEventType } = require('../../../../shared/src/types/event.types');
const { createLogger } = require('../../../../shared/src/utils/logger');

const logger = createLogger('ProfileController');

async function createOrUpdateProfile(req, res) {
  try {
    const userId = req.headers['x-user-id'] || req.body.userId;
    if (!userId) {
      return res.status(401).json({ success: false, error: 'User ID is required' });
    }

    const {
      displayName,
      age,
      gender,
      interestedIn,
      bio,
      photos,
      location,
      maxDistanceKm,
      ageMin,
      ageMax,
      isActive,
    } = req.body;

    if (!displayName || !age || !gender || !interestedIn || !location) {
      return res.status(400).json({
        success: false,
        error: 'Missing required profile fields: displayName, age, gender, interestedIn, location (lat, lon)',
      });
    }

    const existing = await dbService.getProfile(userId);
    const eventType = existing ? KafkaEventType.PROFILE_UPDATED : KafkaEventType.PROFILE_CREATED;

    const profileData = {
      userId,
      displayName,
      age: parseInt(age, 10),
      gender,
      interestedIn,
      bio: bio || '',
      photos: Array.isArray(photos) ? photos : [],
      location: {
        lat: parseFloat(location.lat),
        lon: parseFloat(location.lon),
      },
      maxDistanceKm: maxDistanceKm ? parseInt(maxDistanceKm, 10) : 50,
      ageMin: ageMin ? parseInt(ageMin, 10) : 18,
      ageMax: ageMax ? parseInt(ageMax, 10) : 50,
      isActive: isActive !== undefined ? Boolean(isActive) : true,
      updatedAt: new Date(),
    };

    const saved = await dbService.upsertProfile(profileData);
    await profileKafkaProducer.emitProfileCDC(eventType, saved);

    logger.info(`Profile upserted for userId=${userId}, event=${eventType}`);

    res.status(200).json({
      success: true,
      message: 'Profile saved successfully and synced via CDC',
      profile: saved,
    });
  } catch (err) {
    logger.error(`Error saving profile: ${err.message}`);
    res.status(500).json({ success: false, error: err.message });
  }
}

async function getMyProfile(req, res) {
  try {
    const userId = req.headers['x-user-id'];
    if (!userId) {
      return res.status(401).json({ success: false, error: 'User ID missing from headers' });
    }

    const profile = await dbService.getProfile(userId);
    if (!profile) {
      return res.status(404).json({ success: false, error: 'Profile not found' });
    }

    res.status(200).json({ success: true, profile });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
}

async function getProfileById(req, res) {
  try {
    const { userId } = req.params;
    const profile = await dbService.getProfile(userId);
    if (!profile) {
      return res.status(404).json({ success: false, error: 'Profile not found' });
    }

    res.status(200).json({
      success: true,
      profile: {
        userId: profile.userId,
        displayName: profile.displayName,
        age: profile.age,
        gender: profile.gender,
        bio: profile.bio,
        photos: profile.photos,
      },
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
}

async function updateSettings(req, res) {
  try {
    const userId = req.headers['x-user-id'];
    if (!userId) {
      return res.status(401).json({ success: false, error: 'User ID missing' });
    }

    const currentProfile = await dbService.getProfile(userId);
    if (!currentProfile) {
      return res.status(404).json({ success: false, error: 'Profile not found' });
    }

    const { maxDistanceKm, ageMin, ageMax, interestedIn, isActive } = req.body;

    if (maxDistanceKm !== undefined) currentProfile.maxDistanceKm = parseInt(maxDistanceKm, 10);
    if (ageMin !== undefined) currentProfile.ageMin = parseInt(ageMin, 10);
    if (ageMax !== undefined) currentProfile.ageMax = parseInt(ageMax, 10);
    if (interestedIn !== undefined) currentProfile.interestedIn = interestedIn;
    if (isActive !== undefined) currentProfile.isActive = Boolean(isActive);

    const updated = await dbService.upsertProfile(currentProfile);
    await profileKafkaProducer.emitProfileCDC(KafkaEventType.PROFILE_UPDATED, updated);

    res.status(200).json({
      success: true,
      message: 'Discovery settings updated successfully',
      settings: {
        maxDistanceKm: updated.maxDistanceKm,
        ageMin: updated.ageMin,
        ageMax: updated.ageMax,
        interestedIn: updated.interestedIn,
        isActive: updated.isActive,
      },
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
}

async function getMatches(req, res) {
  try {
    const userId = req.headers['x-user-id'];
    if (!userId) {
      return res.status(401).json({ success: false, error: 'User ID missing' });
    }

    const matches = await dbService.getMatchesForUser(userId);

    const hydratedMatches = await Promise.all(
      matches.map(async (m) => {
        const partnerId = m.user1Id === userId ? m.user2Id : m.user1Id;
        const partnerProfile = await dbService.getProfile(partnerId);
        return {
          matchId: m.id,
          matchedAt: m.matchedAt,
          partner: partnerProfile
            ? {
                userId: partnerProfile.userId,
                displayName: partnerProfile.displayName,
                age: partnerProfile.age,
                bio: partnerProfile.bio,
                photos: partnerProfile.photos,
              }
            : { userId: partnerId },
        };
      })
    );

    res.status(200).json({
      success: true,
      count: hydratedMatches.length,
      matches: hydratedMatches,
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
}

module.exports = {
  createOrUpdateProfile,
  getMyProfile,
  getProfileById,
  updateSettings,
  getMatches,
};

