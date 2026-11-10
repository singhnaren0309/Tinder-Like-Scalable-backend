-- ==========================================================
-- Tinder System Design: Cassandra CQL Initialization Script
-- Stores High Writes-Per-Second (WPS) Swiping Data
-- ==========================================================

CREATE KEYSPACE IF NOT EXISTS tinder_swipe_keyspace
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE tinder_swipe_keyspace;

-- Swipes partitioned by swiper_id
-- Clustering key target_id allows fast lookup of all profiles a user has swiped on
CREATE TABLE IF NOT EXISTS swipes (
    swiper_id uuid,
    target_id uuid,
    swipe_type text,      -- 'LEFT', 'RIGHT', 'SUPER_LIKE'
    created_at timestamp,
    PRIMARY KEY ((swiper_id), target_id)
) WITH CLUSTERING ORDER BY (target_id ASC);

-- Swipes partitioned by target_id
-- Clustering key swiper_id allows lookup of who swiped on this user
CREATE TABLE IF NOT EXISTS swipes_received (
    target_id uuid,
    swiper_id uuid,
    swipe_type text,
    created_at timestamp,
    PRIMARY KEY ((target_id), swiper_id)
);

