WITH
    scored_table AS (
        SELECT 
            id_song,
            SUM(1.0 / CAST(.position AS FLOAT)) AS p_score,
            SUM(101 - position) AS l_score,
            SUM(1.0 / LN(CAST(position AS FLOAT) + 1.0)) AS _score
        FROM charts
        WHERE date >= start_date AND date <= end_date
        GROUP BY
            records.id_song
    ),
    joined_table AS (
        SELECT
            songs.name,
            songs.chart_debut,
            scored_table.p_score,
            scored_table.l_score,
            scored_table._score
        FROM scored_table
        INNER JOIN songs ON scored_table.id_song = songs.id
    )
    ---having date > time machine view
SELECT
    joined_table.name,
    STRFTIME(joined_table.chart_debut, '%B %-d, %Y') AS hot100_debut,
    --((p_score - p_mean) / p_stdv) AS pos_zscore,
    --((l_score - l_mean) / l_stdv) AS long_zscore,
    --((_score - _mean) / _stdv) AS unweighted_zscore,
    DENSE_RANK() OVER(PARTITION BY DECADE(chart_debut) ORDER BY _score DESC) AS decade_rank,
    DENSE_RANK() OVER(PARTITION BY DECADE(chart_debut) ORDER BY p_score DESC) AS decade_rank_pos,
    DENSE_RANK() OVER(PARTITION BY DECADE(chart_debut) ORDER BY l_score DESC) AS decade_rank_long,
    DENSE_RANK() OVER(ORDER BY _score DESC) AS general_rank,
    DENSE_RANK() OVER(ORDER BY p_score DESC) AS general_rank_pos,
    DENSE_RANK() OVER(ORDER BY l_score DESC) AS general_rank_long,
FROM joined_table
ORDER BY decade_rank DESC;