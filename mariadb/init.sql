CREATE DATABASE IF NOT EXISTS music_analysis;

CREATE TABLE IF NOT EXISTS top_20_artists (
    artist_name VARCHAR(255),
    mentions INT
);