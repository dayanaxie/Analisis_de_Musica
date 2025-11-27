CREATE DATABASE IF NOT EXISTS music_analysis;

CREATE TABLE IF NOT EXISTS top_20_artists (
    artist_name VARCHAR(150),
    mentions INT
);

CREATE TABLE IF NOT EXISTS top_20_tracks (
    track_name VARCHAR(150),
    mentions INT
);

CREATE TABLE IF NOT EXISTS top_20_albums (
    album_name VARCHAR(150),
    mentions INT
);