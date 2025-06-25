import polars as pl
from pathlib import Path

pl.Config.set_tbl_cols(200)
pl.Config.set_tbl_rows(100)
pl.Config.set_tbl_width_chars(-1)
pl.Config.set_tbl_formatting(format="UTF8_FULL")
# if i mistakenly seperate a band name, they should have the exact same artist score and amount of chart entries
# total points for a song should be distributed evenly among all artists on a track


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
    # edge_pat_3: str = r"(?i)&\s(the|his|her|original|co\.)(.*$)"
    # matches any occurance of "& " followed by the, his, her, or original; captures the previous word and the rest of the string
    return (
        col.str.replace(edge_pat_1, "&").str.replace(edge_pat_2, r"$1$2$3")
        # .str.replace(edge_pat_3, r"and $1$2")
    )


def simple_str_clean(col: pl.Expr) -> pl.Expr:
    sep_pattern: str = r"(?i)\sfeat\.*[a-z]*\s|\swith\s|\s*[&/+,]\s*|\sx\s|\sand\s"

    return col.str.replace_all(sep_pattern, "--").str.split("--")


def first_and_second_class_insert(artist_col: pl.Expr) -> pl.Expr:
    split_pattern: str = r"(?i)\sfeat\.*[a-z]*\s|\swith\s"
    sep_pattern: str = r"\s*[&/+,]\s*|\s[xX]\s"

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
        .sort(by="id", descending=False)
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
        .select(id=pl.col("artist").hash(), artist=pl.col("artist"))
        .drop_nulls()
    )


def get_junction_table(lf, song_tbl, artist_tbl) -> pl.LazyFrame:
    return (
        lf.with_columns(
            artist=pl.col("artist")
            .pipe(handle_edge_cases)
            .pipe(first_and_second_class_insert),
        )
        .unique(subset=["song", "artist"])
        .join(song_tbl, on=["song", "artist"], how="left", suffix="_song")
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


def clean_base_table(lf) -> pl.LazyFrame:
    return (
        lf.with_columns(
            artists=pl.col("artist").pipe(handle_edge_cases).pipe(simple_str_clean),
        )
        .unique(subset=["date", "position"])
        .select(["date", "position", "song", "artists"])
        .sort(by="date", descending=True)
    )


def transform(load_path) -> dict[str, pl.DataFrame]:
    base_table: pl.LazyFrame = load_data(load_path)
    return base_table.pipe(clean_base_table).collect()


if __name__ == "__main__":
    ROOT_DIR: Path = Path(__file__).parent.parent
    path = ROOT_DIR / "data" / "raw_data" / "raw-data_06-22.parquet"
    tbls_dict = transform(load_path=path)
