CREATE TEMP
TABLE IF NOT EXISTS song_base AS (
    SELECT
        song,
        artists,
        row_number() OVER (
            ORDER BY min(DATE)
        ) AS id_song,
        min(DATE) AS debut
    FROM charts c,
    GROUP BY
        song,
        artists
);

CREATE TEMP
TABLE IF NOT EXISTS artist_base AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY ANY_VALUE(art_debut)
        ) AS id_artist,
        STRING_AGG (artist, ' and ' ORDER BY id_songs.pos) AS name,
        LIST (artist ORDER BY id_songs.pos) AS name_comps,
        id_songs[id] AS song_ids
    FROM 
        (SELECT
            artist,
            LIST ({'pos': list_indexof(b.artists, u.artist), 'id': id_song}) AS id_songs, 
            MIN(debut) AS art_debut
        FROM song_base b, unnest (b.artists) AS u (artist)
        GROUP BY
            u.artist)
        AS by_art
    GROUP BY
        id_songs.id
);

-- CREATE TABLE IF NOT EXISTS new_songs AS (
--     SELECT
--         id_song,
--         ANY_VALUE(song) AS title_song,
--         LIST (name) AS artist_song,
--         ANY_VALUE(debut) AS debut_song,
--     FROM
--         artist_base ab
--         JOIN song_base sb ON LIST_CONTAINS (ab.song_ids, sb.id_song)
--     GROUP BY
--         id_song
-- );