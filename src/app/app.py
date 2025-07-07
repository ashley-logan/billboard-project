import duckdb
import dash_mantine_components as dmc
from dash import Dash, html, Input, Output, callback, dcc
from utils import DB_PATH, OLDEST_RECORD_DATE
from datetime import datetime

app = Dash(__name__)

# app.db_conn = duckdb.connect(database=DB_PATH, read_only=True)

app.layout = dmc.MantineProvider(
    children=[
        dcc.Store(id="store-1"),
        dmc.Container(
            fluid=True,
            children=[
                dmc.Stack(
                    [
                        dmc.DatePickerInput(
                            id="dpi-1",
                            value=OLDEST_RECORD_DATE,
                            valueFormat="MMMM DD, YYYY",
                            label="Earliest",
                            debounce=True,
                            allowDeselect=True,
                            clearable=True,
                            description="Leave empty for the oldest chart",
                            minDate=OLDEST_RECORD_DATE,
                            maxDate=datetime.now().date(),
                        ),
                        dmc.DatePickerInput(
                            id="dpi-2",
                            value=datetime.now().date(),
                            valueFormat="MMMM DD, YYYY",
                            label="Latest",
                            debounce=True,
                            allowDeselect=True,
                            clearable=True,
                            description="Leave empty for the newest chart",
                            minDate=OLDEST_RECORD_DATE,
                            maxDate=datetime.now().date(),
                        ),
                        dmc.Table(
                            id="table-1",
                        ),
                    ]
                )
            ],
        ),
    ]
)


@callback(Output("dpi-2", "minDate"), Output("dpi-1", "value"), Input("dpi-1", "value"))
def update_minDate(start_date):
    if start_date is None:
        return OLDEST_RECORD_DATE, OLDEST_RECORD_DATE
    return start_date, start_date


@callback(Output("dpi-1", "maxDate"), Output("dpi-2", "value"), Input("dpi-2", "value"))
def update_maxDate(end_date):
    if end_date is None:
        return datetime.now().now(), datetime.now().date()
    return end_date, end_date


if __name__ == "__main__":
    app.run(debug=True)
