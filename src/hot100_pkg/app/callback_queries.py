formulas: dict[str, str] = {
    "pop_rank": "SUM(1.0 / LN(position::FLOAT + 1.0))",
    "pow_rank": "SUM(1.0 / position::FLOAT)",
    "long_rank": "SUM(101 - position)",
}


def filter_table(rank_option):
    return f"""
    WITH scored AS (
    SELECT 
    song AS title,
    artists,
    MIN(date) AS debut,
    {formulas[rank_option]} - STDDEV({formulas[rank_option]}) OVER(PARTITION BY DECADE(MIN(date))) 
        / MEAN({formulas[rank_option]}) OVER(PARTITION BY DECADE(MIN(date)))
        AS z_score
    FROM charts,
    WHERE date >= ? AND date <= ?
    GROUP BY song, artists
    )
    SELECT 
    title,
    artists,
    debut,
    RANK() OVER(PARTITION BY DECADE(debut) ORDER BY z_score DESC) AS rank,
    FROM scored
    ORDER BY rank, (z_score) DESC
    LIMIT ?
    """
