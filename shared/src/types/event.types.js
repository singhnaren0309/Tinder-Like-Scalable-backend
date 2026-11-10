/**
 * Kafka CDC and Event Types
 */
const KafkaEventType = Object.freeze({
  PROFILE_CREATED: 'PROFILE_CREATED',
  PROFILE_UPDATED: 'PROFILE_UPDATED',
  SWIPE_EVENT: 'SWIPE_EVENT',
  MATCH_CREATED: 'MATCH_CREATED',
});

module.exports = {
  KafkaEventType,
};

