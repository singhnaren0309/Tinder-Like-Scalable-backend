-- ==========================================================
-- Tinder System Design: PostgreSQL Initialization Script
-- Stores consistent relational data: Users, Profiles, Matches
-- ==========================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Users Table (Authentication & Identity)
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Profiles Table (Rich Profile Data, Location, Discovery Settings)
CREATE TABLE IF NOT EXISTS profiles (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    display_name VARCHAR(100) NOT NULL,
    age INT NOT NULL CHECK (age >= 18),
    gender VARCHAR(20) NOT NULL,          -- 'MALE', 'FEMALE', 'NON_BINARY', 'OTHER'
    interested_in VARCHAR(20) NOT NULL,   -- 'MALE', 'FEMALE', 'EVERYONE'
    bio TEXT DEFAULT '',
    photos TEXT[] DEFAULT ARRAY[]::TEXT[],
    location_lat DOUBLE PRECISION NOT NULL,
    location_lon DOUBLE PRECISION NOT NULL,
    max_distance_km INT DEFAULT 50,
    age_min INT DEFAULT 18,
    age_max INT DEFAULT 50,
    is_active BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Spatial and Filtering Indexes
CREATE INDEX IF NOT EXISTS idx_profiles_location ON profiles (location_lat, location_lon);
CREATE INDEX IF NOT EXISTS idx_profiles_gender_interested ON profiles (gender, interested_in);
CREATE INDEX IF NOT EXISTS idx_profiles_age ON profiles (age);
CREATE INDEX IF NOT EXISTS idx_profiles_is_active ON profiles (is_active);

-- 3. Matches Table (Consistent record of mutual matches)
CREATE TABLE IF NOT EXISTS matches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user1_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    user2_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    matched_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    CONSTRAINT check_distinct_users CHECK (user1_id <> user2_id),
    CONSTRAINT unique_user_pair UNIQUE (user1_id, user2_id)
);

CREATE INDEX IF NOT EXISTS idx_matches_user1 ON matches (user1_id);
CREATE INDEX IF NOT EXISTS idx_matches_user2 ON matches (user2_id);
CREATE INDEX IF NOT EXISTS idx_matches_matched_at ON matches (matched_at DESC);

