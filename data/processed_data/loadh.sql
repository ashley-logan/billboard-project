CREATE TEMP
TABLE IF NOT EXISTS song_base AS (
    SELECT
        song,
        artists,
        row_number() OVER (
            ORDER BY min(DATE)
        ) AS id_song,
        min(DATE) AS debut
    FROM charts,
    GROUP BY
        song,
        artists
);

-- CREATE TEMP
-- TABLE IF NOT EXISTS artist_base AS (
--     SELECT
--         ROW_NUMBER() OVER (
--             ORDER BY ANY_VALUE(art_debut)
--         ) AS id_artist,
--         STRING_AGG (
--             artist,
--             ' and '
--             ORDER BY row_pos
--         ) AS name,
--         LIST (
--             artist
--             ORDER BY row_pos
--         ) AS name_comps,
--         first(row_pos),
--         id_songs AS song_ids
--     FROM (
--             SELECT
--                 artist, any_value(
--                     list_indexof (b.artists, u.artist)
--                 ) AS row_pos, LIST (
--                     id_song
--                     ORDER BY debut
--                 ) AS id_songs, MIN(debut) AS art_debut
--             FROM song_base b, unnest (b.artists) AS u (artist)
--             GROUP BY
--                 u.artist
--         ) AS by_art
--     GROUP BY
--         id_songs
-- );

create temp table if not exists artist_base as (
    SELECT
        artist,
        list(id_song) as id_songs,
        first({'song':id_song, 'pos':list_indexof(sb.artists,u.artist)}) as art_order,
        min(debut) as art_debut
    FROM song_base sb, unnest(sb.artists) as u(artist)
    GROUP BY artist
);

CREATE temp
TABLE IF NOT EXISTS new_artists AS (
    SELECT
        row_number() OVER (
            ORDER BY first (art_debut)
        ) AS id,
        list (
            artist
            ORDER BY art_order.pos
        ) AS name_parts,
        string_agg (
            artist,
            ' and '
            ORDER BY art_order.pos
        ) AS name_string,
        list (art_debut) AS debuts
    FROM artist_base ab
    GROUP BY
        id_songs
);

CREATE temp
TABLE IF NOT EXISTS new_songs AS (
    WITH
        cte AS (
            SELECT id_song, name_string
            FROM
                song_base sb,
                unnest (sb.artists) AS u (art)
                JOIN artist_base ab ON LIST_CONTAINS (ab.name_parts, u.art)
            GROUP BY
                name_string
        )
    SELECT string_agg (
            name_string, '|'
            ORDER BY art_debut
        )
) CREATE temp
TABLE IF NOT EXISTS new_songs AS (
    SELECT id_song AS id, song AS title, list (name_parts) OVER (
            PARTITION BY
                art_pos.song
            ORDER BY art_pos.pos
        )
    FROM
        new_artists na,
        unnest (ab.art_order) AS u (art_pos)
        JOIN song_base sb ON art_pos.song = sb.id_song
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

-- SELECT row_number() OVER (
--         PARTITION BY
--             id_song
--     ) AS id_list, artist, id_song
-- FROM song_base b, unnest (b.artists) AS u (artist);