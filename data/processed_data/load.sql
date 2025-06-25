CREATE temp
TABLE (
    SELECT
        song,
        artists,
        sum(1 / charts.position) AS pos_score,
        sum(101 - charts.position) AS long_score,
        sum(1 / ln(charts.position + 1)) AS overall_score,
        min(charts.date) AS min_date
    FROM charts
    GROUP BY
        ALL
) AS new_songs;

INSERT INTO
    songs
SELECT *
FROM new_songs ON conflict (song_title) do
UPDATE
SET
    pos_score = pos_score + excluded.pos_score,
    long_score = long_score + excluded.long_score,
    overall_score = overall_score + excluded.overall_score,
    chart_debut = strftime (
        min(
            strptime (chart_debut, '%B %-d, %Y'),
            excluded.min_date
        ),
        '%B %-d, %Y'
    );