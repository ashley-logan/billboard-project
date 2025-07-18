import duckdb
import dash_ag_grid as dag
import dash_mantine_components as dmc
from dash import Dash, html, Input, Output, callback, dcc

from hot100_pkg.utils import DB_PATH, OLDEST_RECORD_DATE, get_curr_date

CURR_DATE = get_curr_date()
db_conn = duckdb.connect(database=DB_PATH, read_only=True)

formulas: dict[str, str] = {
    "pop_rank": "SUM(1.0 / LN(position::FLOAT + 1.0))",
    "pow_rank": "SUM(1.0 / position::FLOAT)",
    "long_rank": "SUM(101 - position)",
}


app = Dash(__name__)

# app.db_conn = duckdb.connect(database=DB_PATH, read_only=True)

app.layout = dmc.MantineProvider(
    children=[
        dmc.Stack(
            children=[
                html.H1("Song Popularity Data"),
                dmc.DatePickerInput(
                    id="date1",
                    label="Oldest Chart",
                    description="No charts older than this will be included in the ranking",
                    allowDeselect=True,
                    placeholder="Oldest Chart",
                    value=OLDEST_RECORD_DATE,
                    minDate=OLDEST_RECORD_DATE,
                    maxDate=CURR_DATE,
                    closeOnChange=False,
                    clearable=True,
                ),
                dmc.DatePickerInput(
                    id="date2",
                    label="Newest Chart",
                    description="No charts newer than this will be included in the ranking",
                    allowDeselect=True,
                    placeholder="Newest Chart",
                    value=CURR_DATE,
                    minDate=OLDEST_RECORD_DATE,
                    maxDate=CURR_DATE,
                    closeOnChange=False,
                    clearable=True,
                    highlightToday=True,
                ),
                dmc.Tabs(
                    id="chart-tabs",
                    value="pop_rank",
                    variant="outline",
                    children=[
                        dmc.TabsList(
                            [
                                dmc.TabsTab("Popularity Ranking", value="pop_rank"),
                                dmc.TabsTab("Power Ranking", value="pow_rank"),
                                dmc.TabsTab("Longevity Ranking", value="long_rank"),
                            ]
                        ),
                        dmc.Stack(
                            children=[
                                dag.AgGrid(id="table1", rowData=[], columnDefs=[])
                            ]
                        ),
                    ],
                ),
            ]
        )
    ]
)


@callback(Output("date2", "minDate"), Output("date1", "value"), Input("date1", "value"))
def update_minDate(start_date):
    if start_date is None:
        return OLDEST_RECORD_DATE, OLDEST_RECORD_DATE
    return start_date, start_date


@callback(Output("date1", "maxDate"), Output("date2", "value"), Input("date2", "value"))
def update_maxDate(end_date):
    if end_date is None:
        return CURR_DATE, CURR_DATE
    return end_date, end_date


@callback(
    Output("table1", "rowData"),
    Output("table1", "columnDefs"),
    Input("chart-tabs", "value"),
    Input("date1", "value"),
    Input("date2", "value"),
)
def update_table1(rank_option, start_date, end_date, limit=50):
    query = f"""
        SELECT
        ROW_NUMBER() OVER(ORDER BY {formulas[rank_option]} DESC) AS rank,
        song AS title,
        artists,
        min(date) AS debut
        FROM charts
        WHERE date >= ? AND date <= ?
        GROUP BY song, artists
        ORDER BY rank
        LIMIT ?
        """
    df = db_conn.execute(query, [start_date, end_date, limit]).pl()
    rows = df.to_dicts()
    col_defs = [{"field": col} for col in df.columns]
    return rows, col_defs


if __name__ == "__main__":
    app.run(debug=True)
