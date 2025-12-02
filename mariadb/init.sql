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

-- =======
-- Conteos simples
-- =======

CREATE TABLE IF NOT EXISTS user_artist_counts (
    user_id      VARCHAR(50),
    artist_count INT
);

CREATE TABLE IF NOT EXISTS user_track_counts (
    user_id      VARCHAR(50),
    track_count  INT
);

CREATE TABLE IF NOT EXISTS user_album_counts (
    user_id      VARCHAR(50),
    album_count  INT
);

CREATE TABLE IF NOT EXISTS user_artist_stats (
    metric VARCHAR(20),         
    value  DECIMAL(10,3)
);

CREATE TABLE IF NOT EXISTS user_track_stats (
    metric VARCHAR(20),
    value  DECIMAL(10,3)
);

CREATE TABLE IF NOT EXISTS user_album_stats (
    metric VARCHAR(20),
    value  DECIMAL(10,3)
);

-- =======
-- Calidad
-- =======

CREATE TABLE IF NOT EXISTS data_quality (
    problem          VARCHAR(50),
    affected_users   INT
);
 
CREATE TABLE IF NOT EXISTS outlier_users (
    user_id      VARCHAR(50),
    items_count  INT,
    percentile   DECIMAL(5,2)
);
 
CREATE TABLE IF NOT EXISTS low_coverage_artists (
    artist_name  VARCHAR(150),
    mentions     INT
); 

-- =======
-- Comparaciones Simples
-- =======

CREATE TABLE IF NOT EXISTS top_artists_active_users (
    artist_name VARCHAR(150),
    active_users INT
);

CREATE TABLE IF NOT EXISTS cross_popularity_artists (
    artist_name VARCHAR(150),
    mentions_in_artists INT,
    mentions_in_tracks  INT,
    difference INT
);

CREATE TABLE IF NOT EXISTS artists_diversity (
    artist_name VARCHAR(150),
    distinct_users INT,
    distinct_tracks INT
);



-- =======
-- Concurrencia
-- =======

CREATE TABLE IF NOT EXISTS top_50_artist_pairs (
    artist1     VARCHAR(150),
    artist2     VARCHAR(150),
    pair_count  INT
);

CREATE TABLE IF NOT EXISTS top_50_artist_triplets (
    artist1        VARCHAR(150),
    artist2        VARCHAR(150),
    artist3        VARCHAR(150),
    triplet_count  INT
);

CREATE TABLE IF NOT EXISTS artist_track_overlap (
    overlap_count  INT,
    total_users    INT,
    overlap_ratio  DECIMAL(6,4)
);

CREATE TABLE IF NOT EXISTS artist_average_position (
    artist_name  VARCHAR(150),
    avg_position DECIMAL(10,3),
    user_count   INT
);

CREATE TABLE IF NOT EXISTS top1_in_global_top5_frequency (
    matched_users  INT,
    total_users    INT,
    ratio          DECIMAL(6,4)
);

CREATE TABLE IF NOT EXISTS position_stability_users (
    stable_users  INT,
    total_users   INT,
    ratio         DECIMAL(6,4)
);