CREATE DATABASE IF NOT EXISTS music_analysis;

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