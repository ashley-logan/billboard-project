-- CREATE temp
-- TABLE (
--     SELECT
--         song,
--         artists,
--         sum(1 / charts.position) AS pos_score,
--         sum(101 - charts.position) AS long_score,
--         sum(1 / ln(charts.position + 1)) AS overall_score,
--         min(charts.date) AS min_date
--     FROM charts
--     GROUP BY
--         ALL
-- ) AS new_songs;


create temp table staged_artists as (
with unnested_chart as (
    select 
    date,
    position,
    song,
    unnest(artists) as artist
    from charts
    ),

    artist_pairs as (
    select array_agg({date: date, position: position}) as _id,
    artist
    from unnested_chart
    group by artist
    )

select 
        array_agg(artist) as artists
from artist_pairs
group by _id
having len(artists) > 1
);

CREATE temp TABLE new_artists AS ()