import polars as pl
from pathlib import Path

pl.Config.set_tbl_cols(20)
pl.Config.set_tbl_rows(100)
project_dir = Path(__file__).parent.parent
data_path = (project_dir / "data").resolve()

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


def load_data(file_name):
    file_path = data_path / file_name
    return (
        pl.read_json(
            source=file_path,
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


def normalize_artist_names(col: pl.Expr) -> pl.Expr:
    edge_pat_1 = r"(?i)(\sa\s)?(duet\swith)"
    # matches any occurance of "duet with" (case insensitive)
    edge_pat_2 = r"(?i)\((feat\.*[a-z]*|&|with)(.*?)\)(.*$)"
    # matches any occurance of an opening parenthese immediately followed by a first class seperator (DEFINE CONST) or "&"
    edge_pat_3: str = r"(?i)&\s(the|his|her|original)(.*$)"
    # matches any occurance of "& " followed by the, his, her, or original; captures the previous word and the rest of the string
    return (
        col.str.replace(edge_pat_1, "&")
        .str.replace(edge_pat_2, r"$1$2$3")
        .str.replace(edge_pat_3, r"and $1$2")
    )


def split_artist_names1(col: pl.Expr) -> pl.Expr:
    split_pattern: str = r"(?i)\sfeat\.*[a-z]*\s|\swith\s"
    sep_pattern: str = r"(?i)\s*[&/+,]\s*|\sx\s"

    return (
        (
            col.str.replace(split_pattern, "-")
            .str.split_exact("-", 1)
            .struct.rename_fields(["main", "featuring"])
        )
        .struct.with_fields(
            pl.field("*").str.strip_chars().str.replace_all(sep_pattern, "!~!")
        )
        .struct.unnest()
    )


def split_artist_names2(col: pl.Expr) -> pl.Expr:
    return col.str.split("!~!").list.eval(pl.element().str.strip_chars())


def get_song_table(lf) -> pl.LazyFrame:
    return (
        lf.with_columns(
            pl.col("artist").pipe(normalize_artist_names).pipe(split_artist_names1)
        )
        .select(["song", "main", "featuring"])
        .with_columns(
            pl.col("main").pipe(split_artist_names2),
            pl.col("featuring").pipe(split_artist_names2),
        )
        .unique(["song", "main", "featuring"])
        .with_row_index("id")
        .select(["id", "song"])
        .drop_nulls()
    )


def get_artist_table(lf) -> pl.LazyFrame:
    df = lf.with_columns(
        pl.col("artist").pipe(normalize_artist_names).pipe(split_artist_names1)
    ).select(["song", "main", "featuring"])
    return (
        pl.concat(
            [
                df.select(song="song", artist="main"),
                df.select(song="song", artist="featuring"),
            ]
        )
        .with_columns(pl.col("artist").pipe(split_artist_names2))
        .explode(["artist"])
        .unique(["artist"])
        .with_row_index("id")
        .select("id", "artist")
        .drop_nulls()
    )


def get_junction_table(lf, song_tbl, artist_tbl) -> pl.LazyFrame:
    return (
        lf.with_columns(
            pl.col("artist").pipe(normalize_artist_names).pipe(split_artist_names1)
        )
        .select(["song", "main", "featuring"])
        .unpivot(
            index=["song"],
            variable_name="role",
            value_name="artist",
        )
        .with_columns(pl.col("artist").pipe(split_artist_names2))
        .explode(["artist"])
        .join(song_tbl, on="song", how="left")
        .drop("song")
        .rename({"id": "song_id"})
        .join(artist_tbl, on="artist", how="left")
        .drop("artist")
        .rename({"id": "artist_id"})
        .select(["song_id", "artist_id", "role"])
        .drop_nulls()
    )


def transform(file_name):
    base_table = load_data(file_name)
    song_tbl = get_song_table(base_table)
    artist_tbl = get_artist_table(base_table)
    junc = get_junction_table(base_table, song_tbl, artist_tbl).collect()


if __name__ == "__main__":
    transform(file_name="records06-14_15-34.json")
