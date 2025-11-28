CREATE DATABASE IF NOT EXISTS music_analysis;

-- =================================
-- Popularidad y frecuencias básicas
-- =================================

CREATE TABLE IF NOT EXISTS top_20_artists (
    artist_name VARCHAR(150),
    users_count INT,
    percentage  DECIMAL(5,2)
);

CREATE TABLE IF NOT EXISTS top_20_tracks (
    track_name VARCHAR(150),
    users_count INT,
    percentage  DECIMAL(5,2)
);

CREATE TABLE IF NOT EXISTS top_20_albums (
    album_name VARCHAR(150),
    users_count INT,
    percentage  DECIMAL(5,2)
); 

CREATE TABLE IF NOT EXISTS same_top1_artist (
    top1_artist  VARCHAR(150),
    users_count  INT,
    percentage   DECIMAL(5,2)
);

CREATE TABLE IF NOT EXISTS artist_mention_stats (
    metric       VARCHAR(20),   
    value        DECIMAL(10,3)
);

CREATE TABLE IF NOT EXISTS long_tail_80 (
    total_artists      INT,
    artists_in_tail    INT,
    tail_percentage    DECIMAL(5,2)
);