import polars as pl
import duckdb
from hot100_pkg.utils import DB_PATH


def load(clean_df: pl.DataFrame):
    conn = duckdb.connect(database=DB_PATH)
    conn.register("charts_df", clean_df)
    conn.execute("INSERT INTO charts SELECT * FROM charts_df")
    conn.execute(
        """
        CREATE OR REPLACE TABLE songs AS (
            SELECT 
            song AS title,
            artists,
            ROW_NUMBER() OVER (ORDER BY MIN(DATE)) AS id,
            MIN(DATE) AS debut
        FROM charts,
        GROUP BY
            song,
            artists
        )
        """
    )
    conn.execute(
        """
        WITH artist_base AS (
        SELECT
        artist,
        LIST(id) AS id_songs,
        ANY_VALUE({'song': s.id, 'pos': LIST_INDEX_OF(s.artists, u.artist)}) AS art_order,
        MIN(s.debut) AS art_debut
        FROM songs s, UNNEST(s.artists) AS u(artist)
        GROUP BY artist
        )
        SELECT 
        ROW_NUMBER() OVER (
            ORDER BY FIRST (art_debut)
        ) AS id,
        LIST (
            artist
            ORDER BY art_order.pos
        ) AS name_parts,
        STRING_AGG (
            artist,
            ' and '
            ORDER BY art_order.pos
        ) AS name_string,
        ANY_VALUE(art_debut) AS art_debut
    FROM artist_base ab
    GROUP BY
        id_songs
        """
    )
    conn.execute()

    conn.close()
