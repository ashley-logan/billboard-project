import polars as pl

pl.Config.set_tbl_cols(20)
pl.Config.set_tbl_rows(100)
pl.Config.set_tbl_width_chars(-1)
pl.Config.set_tbl_formatting(format="UTF8_FULL")

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


def load_data(load_path):
    return (
        pl.scan_parquet(
            source=load_path,
            schema={
                "position": pl.UInt8,
                "date": pl.Date,
                "song": pl.String,
                "artist": pl.String,
            },
        )
        .sort(by="date")
        .with_row_index("id")
        .select(["id", "date", "position", "song", "artist"])
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
        .select(["id", "song", "artist", "debut", "decade", "hash_id"])
    )


def handle_edge_cases(col: pl.Expr) -> pl.Expr:
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


def first_and_second_class_insert(artist_col: pl.Expr) -> pl.Expr:
    split_pattern: str = r"(?i)\sfeat\.*[a-z]*\s|\swith\s"
    sep_pattern: str = r"(?i)\s*[&/+,]\s*|\sx\s"

    return (
        artist_col.str.replace_all(split_pattern, "!-!")
        .str.split("!-!")
        .list.eval(
            pl.element()
            .str.strip_chars()
            .str.replace_all(sep_pattern, "!-!")
            .str.split("!-!")
            .list.sort()
            .list.join("!-!")
        )
        .list.sort()
        .list.join("!~!")
    )


def first_class_split(col: pl.Expr) -> pl.Expr:
    return (
        col.str.split_exact("!~!", n=1)
        .struct.rename_fields(["main", "featuring"])
        .struct.unnest()
    )


def second_class_split(col: pl.Expr) -> pl.Expr:
    return col.str.split("!-!").list.eval(pl.element().str.strip_chars())


def get_song_table(lf) -> pl.LazyFrame:
    decade_cuts: range = range(1970, 2030, 10)
    decade_labels = [f"{decade}s" for decade in range(1960, 2030, 10)]
    return (
        lf.with_columns(
            artist=pl.col("artist")
            .pipe(handle_edge_cases)
            .pipe(first_and_second_class_insert),
        )
        .with_columns(id=pl.concat_str(["song", "artist"], separator="|").hash())
        .group_by(["id", "song", "artist"])
        .agg(chart_debut=pl.col("date").min())
        .with_columns(
            decade=(
                pl.col("chart_debut")
                .dt.year()
                .cut(breaks=decade_cuts, labels=decade_labels, left_closed=True)
            )
        )
        .select(["id", "song", "artist", "chart_debut", "decade"])
        .drop_nulls()
        .sort(by="id", descending=True)
    )


def get_artist_table(lf) -> pl.LazyFrame:
    df = lf.with_columns(
        pl.col("artist")
        .pipe(handle_edge_cases)
        .pipe(first_and_second_class_insert)
        .pipe(first_class_split)
    ).select(["song", "main", "featuring"])
    return (
        pl.concat(
            [
                df.select(song="song", artist="main"),
                df.select(song="song", artist="featuring"),
            ]
        )
        .with_columns(pl.col("artist").pipe(second_class_split))
        .explode(["artist"])
        .unique(["artist"])
        .select("artist", id=pl.col("artist").hash())
        .drop_nulls()
    )


def get_junction_table(lf, song_tbl, artist_tbl) -> pl.LazyFrame:
    return (
        lf.with_columns(
            artist=pl.col("artist")
            .pipe(handle_edge_cases)
            .pipe(first_and_second_class_insert),
        )
        .with_columns(id_song=pl.concat_str(["song", "artist"], separator="|").hash())
        .with_columns(pl.col("artist").pipe(first_class_split))
        .select(["id_song", "main", "featuring"])
        .unpivot(
            index=["id_song"],
            variable_name="role",
            value_name="artist",
        )
        .with_columns(pl.col("artist").pipe(second_class_split))
        .explode(["artist"])
        .join(artist_tbl, on="artist", how="left")
        .drop("artist")
        .rename({"id": "id_artist"})
        .select(["id_song", "id_artist", "role"])
        .unique()
        .drop_nulls()
        .sort(by="id_song", descending=True)
    )


def clean_base_table(lf, song_tbl) -> pl.LazyFrame:
    return (
        lf.with_columns(
            date=pl.col("date").dt.to_string("iso"),
            artist=pl.col("artist")
            .pipe(handle_edge_cases)
            .pipe(first_and_second_class_insert),
        )
        .unique(subset=["date", "position"])
        .with_columns(id_song=pl.concat_str(["song", "artist"], separator="|").hash())
        .select(["id_song", "date", "position", "artist"])
        .sort(by="id_song", descending=True)
    )


def transform(load_path) -> dict[str, pl.DataFrame]:
    base_table: pl.LazyFrame = load_data(load_path)
    song_tbl: pl.LazyFrame = base_table.pipe(get_song_table)
    artist_tbl: pl.LazyFrame = base_table.pipe(get_artist_table)
    records_tbl = base_table.pipe(clean_base_table, song_tbl)
    junction_tbl: pl.LazyFrame = base_table.pipe(
        get_junction_table, song_tbl, artist_tbl
    )
    df_names: list[str] = ["songdf", "artistdf", "recordsdf", "junctiondf"]
    dfs: list[pl.DataFrame] = pl.collect_all(
        [song_tbl, artist_tbl, records_tbl, junction_tbl]
    )
    return dict(zip(df_names, dfs))


# if __name__ == "__main__":
#     tbls_dict = transform()
#     for df in tbls_dict.values():
#         print(df.head(20))
