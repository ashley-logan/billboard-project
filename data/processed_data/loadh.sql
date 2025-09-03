CREATE TEMP
TABLE IF NOT EXISTS song_base AS (
    SELECT song, artists, row_number() OVER (
            ORDER BY min(DATE)
        ) AS id, min(DATE) AS debut
    FROM charts,
    GROUP BY
        song,
        artists
);

create temp table if not exists artist_base as (
    SELECT
        artist,
        list(id) as id_songs,
        first({'song': sb.id, 'pos':list_indexof(sb.artists,u.artist)}) as art_order,
        min(sb.debut) as art_debut
    FROM song_base sb, unnest(sb.artists) as u(artist)
    GROUP BY artist
);

CREATE TABLE IF NOT EXISTS new_artists AS (
    SELECT
        row_number() OVER (
            ORDER BY FIRST (art_debut)
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
        any_value(art_debut) AS art_debut
    FROM artist_base ab
    GROUP BY
        id_songs
);

CREATE TABLE IF NOT EXISTS new_songs AS (
    SELECT
        sb.id AS id,
        sb.song AS title,
        LIST (DISTINCT na.name_string) AS artists,
        ANY_VALUE(sb.debut) AS debut
    FROM
        charts c,
        UNNEST(c.artists) AS u art)
        JOIN new_artists na ON LIST_CONTAINS (na.name_parts, u.art)
    GROUP BY
        sb.id,
        sb.song
);

-- SELECT row_number() OVER (
--         PARTITION BY
--             id_song
--     ) AS id_list, artist, id_song
-- FROM song_base b, unnest (b.artists) AS u (artist);