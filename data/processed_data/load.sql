BEGIN TRANSACTION;

CREATE OR REPLACE TEMP TABLE by_artist AS (
    SELECT
        LIST(DISTINCT song) AS artist_songs,
        artist,
        LIST({song: song, date: date, position: position} ORDER BY date) AS record_id
    FROM    
        charts,
        UNNEST(charts.artists) AS u(artist)
    GROUP BY u.artist
);

CREATE
OR
REPLACE
    TEMP
TABLE by_history AS (
    SELECT
        record_id,
        LIST (artist) AS artists_list,
        STRING_AGG (artist, ' and ') AS artist_name,
        artist_songs
    FROM by_artist
    GROUP BY
        record_id,
        artist_songs
);

CREATE TEMP
TABLE IF NOT EXISTS new_artists AS (
    SELECT ROW_NUMBER() OVER (
            ORDER BY record_id[1].date
        ) AS artist_id, artist_name
    FROM by_history
);

CREATE TEMP
TABLE IF NOT EXISTS by_song AS (
    SELECT c.song, h.artist_name, min(DATE) AS debut
    FROM
        charts c,
        UNNEST (c.artists) AS u (artist)
        JOIN by_history h ON array_contains (h.artists_list, u.artist)
    GROUP BY
        c.song,
        h.artist_name
);

CREATE TEMP
TABLE IF NOT EXISTS new_songs AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY debut
        ) AS song_id,
        song AS title,
        debut,
        LIST (DISTINCT artist_name) AS song_artists
    FROM by_song
    GROUP BY
        song,
        debut
);

COMMIT;

DROP TABLE IF EXISTS by_artist;

DROP TABLE IF EXISTS by_history;

DROP TABLE IF EXISTS by_song;

DROP TABLE IF EXISTS artists;

ALTER TABLE new_artists RENAME TO artists;

DROP TABLE IF EXISTS songs;

ALTER TABLE new_songs RENAME TO songs;

-- CREATE TEMP TABLE IF NOT EXISTS new_songs AS (
--     WITH
--         art_songs AS (
--             SELECT
--                 UNNEST (artist_songs) AS songs,
--                 artist_id,
--                 artists
--             FROM new_artists
--         )
--     SELECT
--         row_number() OVER (
--             ORDER BY MIN(DATE)
--         ) AS song_id,
--         song,
--         MIN(DATE) AS debut,
--         ARRAY_AGG (
--             DISTINCT art_songs.artist_id
--             ORDER BY artist_id
--         ) AS song_artists
--     FROM charts ch
--         JOIN art_songs ON ch.song = art_songs.songs
--     GROUP BY
--         song
-- );