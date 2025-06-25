CREATE TABLE IF NOT EXISTS charts (
    charts.date DATE,
    charts.position INTEGER,
    charts.song TEXT,
    charts.artists TEXT(charts.date, charts.position) PRIMARY KEY
);

CREATE temp
TABLE (
    SELECT *, sum(1 / charts.position) AS p_score, sum(101 - charts.position) AS l_score, sum(1 / ln(charts.position + 1)) AS _score
    FROM charts
    GROUP BY
        charts.song,
        charts.artists
) AS pre_songs
LEFT JOIN songs ON pre_songs.song = songs.song
AND pre_songs.artists = songs.artists

SELECT
    song,
    artists,
    sum(1 / charts.position) AS p_score,
    sum(101 - charts.position) AS l_score,
    sum(1 / ln(charts.position + 1)) AS _score
FROM charts
GROUP BY
    ALL