from datetime import date
import polars as pl
import adbc_driver_sqlite.dbapi as sql_driver
import pyarrow
import sqlite3

pl.Config.set_tbl_cols(20)
pl.Config.set_tbl_rows(100)
# con = sql_driver.connect("./etl/billboard.db")
"""
TRANFORMATION OUTLINE:
CREATE TABLE records (
id INT PRIMARY KEY
date VARCHAR (ISO FORMAT) 
position INT
song_id INT FOREIGN KEY
)

CREATE TABLE song (
id INT PRIMARY KEY
name VARCHAR
power FLOAT
longevity FLOAT
earliest DATE
latest DATE
decade TEXT
)

CREATE TABLE artist (
id INT PRIMARY KEY
name VARCHAR
)

CREATE TABLE song-artist (
(artist_id INT, song_id INT) PRIMARY KEY
)

"""


def load():
    return (
        pl.read_json(
            source="./data/records06-14_15-34.json",
            schema={
                "position": pl.UInt8,
                "date": pl.String,
                "song": pl.String,
                "artist": pl.String,
            },
        )
        .cast({"date": pl.Date})
        .sort(by="date")
        .with_row_index("id")
        .select(["id", "date", "position", "song", "artist"])
        .lazy()
    )


def get_unique_frame(lf):
    return lf.unique(subset=["song", "artist"])


def create_table_song(lf) -> pl.LazyFrame:
    # score_calcs: dict =  {
    #     "pos_weighted": (1 / pl.col("position")).sum,
    #     "longevity_weighted": (101 - pl.col("position")).truediv(100).sum,
    #     "unweighted": (pl.lit(100).log1p() - pl.col("position").log1p()).sum,
    # }
    decade_cuts: range = range(1970, 2030, 10)
    decade_labels = [f"{decade}s" for decade in range(1960, 2030, 10)]
    return (
        lf.group_by(["song", "artist"])
        .agg(
            # position_score=score_calcs["pos_weighted"](),
            # longevity_score=score_calcs["longevity_weighted"](),
            # overall_score=score_calcs["unweighted"](),
            debut=pl.min("date"),
        )
        .sort(by="debut")
        .with_row_index("id")
        .with_columns(
            decade=(
                pl.col("debut")
                .dt.year()
                .cut(breaks=decade_cuts, labels=decade_labels, left_closed=True)
            )
        )
        .reverse()
        .select(
            [
                "id",
                "song",
                "artist",
                "debut",
                "decade",
            ]
        )
    )


def clean_artist_col(lf) -> pl.LazyFrame:
    edge_pat_1 = r"(?i)(\sa\s)?(duet with)"
    # matches any occurance of "duet with" (case insensitive)
    edge_pat_2 = r"(?i)\((feat\.*[a-z]*)|(&)|(with)"
    # matches any occurance of an opening parenthese immediately followed by a first class seperator (DEFINE CONST) or "&"
    edge_pat_3: str = r"(?i)&\s(the|his|her|original)(.*)"
    # matches any occurance of "& " followed by the, his, her, or original; captures the previous word and the rest of the string
    return (
        lf.unique(subset=["song", "artist"])
        .with_columns(
            # filter out duplicate song entries; include artist in the subset to avoid grouping songs with the same title together
            artist=pl.col("artist").replace(edge_pat_1, "&")
            # replace "duet with" with & since it practically acts as a second class seperator rather than a first class seperator
        )
        .with_columns(
            artist=pl.when(pl.col("artist").str.contains(edge_pat_2))
            .then(pl.col("artist").str.replace_all(r"[()]", ""))
            .otherwise(pl.col("artist"))
            # strip parentheses from entries that only use parenthese to include a feature since it complicated whitespace when regexing later
        )
        .select(
            artist=pl.col("artist").str.replace_all(edge_pat_3, r"and $1$2"),
            song="song",
            # entries matching edge_pat_3 contain band names and should not be seperated, so "&"" is replaced with "and" which is not recognized as a seperator
        )
    )


def split_features(lf) -> pl.LazyFrame:
    split_pattern: str = r"(?i)(\sfeat\.*[a-z]*\s)|(\swith\s)"
    roles = ["main", "featured"]

    mainlf: pl.LazyFrame = lf.with_columns(
        artist=pl.col("artist")
        .str.replace(split_pattern, "-")
        .str.split_exact("-", 1)
        .struct[0],
        role=pl.lit("main"),
    ).drop_nulls()

    featlf: pl.LazyFrame = lf.with_columns(
        artist=pl.col("artist")
        .str.replace(split_pattern, "-")
        .str.split_exact("-", 1)
        .struct[1],
        role=pl.lit("featured"),
    ).drop_nulls()

    return pl.concat([mainlf, featlf]).cast({"role": pl.Enum(roles)})


def split_artists(lf) -> pl.LazyFrame:
    seperator_sub: str = r"(?i)(\s*[&/+,]\s*)|(x\s)"
    return lf.with_columns(
        artist=pl.col("artist")
        .str.replace_all(seperator_sub, "!~!")
        .str.split("!~!")
        .list.eval(pl.element().str.strip_chars()),
    ).explode(["artist"])


def create_table_artist(lf) -> pl.LazyFrame:
    return (
        lf.pipe(clean_artist_col)
        .pipe(split_features)
        .pipe(split_artists)
        .unique(subset=["artist"])
        .with_row_index("id")
        .select(["id", "artist"])
    )


def create_table_junction(lf) -> pl.LazyFrame:
    return (
        lf.pipe(clean_artist_col)
        .pipe(split_features)
        .pipe(split_artists)
        .select(["artist", "song", "role"])
        .join(artist)
    )


def create_table_junction(lf, song_table, artist_table) -> pl.DataFrame:
    pass
    return (
        lf.unique(subset=["song", "artist"])
        .join(song_table.lazy(), on=["song", "artist"], how="inner")
        .select(pl.col("id").alias("song_id"), pl.col("song"), pl.col("artist"))
        .explode(["artist"])
        .explode(["artist"])
        .join(artist_table.lazy(), on="artist", how="inner")
        .rename({"id": "artist_id"})
        .select(["song_id", "song", "artist_id", "artist"])
        .collect()
    )


def main():
    base_table = load()
    song_table = base_table.pipe(create_table_song).collect()
    artist_table = base_table.pipe(create_table_artist).collect()

    print(artist_table.head(20))


if __name__ == "__main__":
    main()
