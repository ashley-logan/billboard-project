BEGIN TRANSACTION;

CREATE TEMP TABLE shared_cte AS (
    SELECT
        LIST(DISTINCT song) AS artist_songs,
        artist,
        LIST({date: date, position: position} ORDER BY date) AS id
    FROM    
        charts,
        UNNEST(charts.artists) AS u(artist)
    GROUP BY u.artist
);

CREATE TEMP TABLE IF NOT EXISTS new_artists AS (
    SELECT
        ROW_NUMBER() OVER(ORDER BY id[1].date) AS artist_id,
        STRING_AGG(artist, ' and ') AS artists
    FROM shared_cte
    GROUP BY id
);

CREATE TEMP
TABLE IF NOT EXISTS new_songs AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY MIN(charts.date)
        ) AS song_id,
        MIN(charts.date) AS debut,
        LIST (DISTINCT shared.artist) AS song_artists
    FROM (
            SELECT UNNEST (artist_songs) AS art_songs
            FROM shared_cte
        ) shared
        JOIN charts ON charts.song = shared.art_songs
    GROUP BY
        charts.song
);

COMMIT;

DROP TABLE IF EXISTS shared_cte;

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