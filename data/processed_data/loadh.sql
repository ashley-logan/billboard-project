
CREATE TEMP TABLE IF NOT EXISTS create_song_hash as (
    select
        song,
        artists,
        md5_number_lower(string_agg({date: c.date, pos: c.position}, '|' order by date)) as song_hash,
        min(date) as debut
    from charts c,
    group by all

);

CREATE temp
TABLE IF NOT EXISTS create_artist_hash AS (
    SELECT
        artist,
        debut,
        list (
            song
            ORDER BY debut
        ) AS songs,
        md5_number_lower (
            string_agg (
                list (song_hash),
                '|'
                ORDER BY debut
            )
        ) AS artist_hash,
    FROM create_song_hash c, unnest (c.artists) AS u (artist)
    GROUP BY
        u.artist
);

CREATE TABLE IF NOT EXISTS new_artists AS (
    SELECT
        row_number() OVER (
            ORDER BY debut
        ) AS artist_id,
        string_agg (artist, ' and ') AS artist_name,
        any_value(songs) AS artist_songs
    FROM create_artist_hash
    GROUP BY
        artist_hash
);