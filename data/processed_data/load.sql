-- BEGIN TRANSACTION;


CREATE TEMP TABLE IF NOT EXISTS new_artists AS ( 

    with by_artist as (
        SELECT
            ARRAY_AGG(DISTINCT song) AS artist_songs,
            u.artist,
            ARRAY_AGG({date: date, position: position} order by date) as id
        FROM    
            charts,
            unnest(charts.artists) as u(artist),
        GROUP BY u.artist
    )
    SELECT
        row_number() over(order by id[0]) as artist_id,
        ANY_VALUE(artist_songs) as artist_songs,
        STRING_AGG(artist, ' and ') as artists
    FROM by_artist
    GROUP BY id
    );

CREATE TEMP
TABLE IF NOT EXISTS new_songs AS (
    WITH
        art_songs AS (
            SELECT
                UNNEST (artist_songs) AS songs,
                artist_id,
                artists
            FROM new_artists
        )
    SELECT
        row_number() OVER (
            ORDER BY MIN(DATE)
        ) AS song_id,
        song,
        MIN(DATE) AS debut,
        ARRAY_AGG (
            DISTINCT art_songs.artist_id
            ORDER BY artist_id
        ) AS song_artists
    FROM charts ch
        JOIN art_songs ON ch.song = art_songs.songs
    GROUP BY
        song
);

-- DROP TABLE IF EXISTS artists;

-- ALTER TABLE new_artists RENAME TO artists;

-- COMMIT;