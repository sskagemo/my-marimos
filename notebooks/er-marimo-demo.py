# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "marimo==0.13.15",
#     "polars==1.30.0",
#     "altair==4.2.0",
#     "pandas==2.3.0",
# ]
# ///

import marimo

__generated_with = "0.14.16"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    import polars as pl
    import altair as alt
    import pandas as pd

    file = mo.notebook_location() / "public" / "enheter_alle.parquet"


@app.cell(hide_code=True)
def _():
    mo.md(
        """
    # Palmer Penguins Analysis

    Analyzing the Palmer Penguins dataset using Polars and marimo.
    """
    )
    return


@app.cell
def _():
    # Les Enhetsregisteret (NB!! Utdatert versjon!!!) fra enheter_alle.parquet
    # Og vis de første linjene
    df = pl.read_parquet(str(file))
    df.head()
    return (df,)


@app.cell
def _(df):
    # Oppsummering av innholdet
    mo.md(f"""
    ### Oversikt over innholdet i datasettet

    - Antall enheter: {df.height}
    - Kolonner: {', '.join(df.columns)}

    ### Oppsummerende fakta/statistikk om de ulike kolonnene

    {mo.as_html(df.describe())}
    """)
    return


@app.cell
def _(df):
    # konvertere organisasjonsform fra str til Categorical
    df_categorical = df.rename(
        {"organisasjonsform.kode": "organisasjonsform_kode"}
    ).with_columns(pl.col("organisasjonsform_kode").cast(pl.Categorical))
    return (df_categorical,)


@app.cell
def _(df_categorical):
    df_categorical.describe()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""### Species Distribution""")
    return


@app.cell
def _(df_categorical):
    # Vis fordeling av organisasjonsformer
    organisasjonsform_chart = mo.ui.altair_chart(
        alt.Chart(df_categorical)
        .mark_bar()
        .encode(x=alt.X("organisasjonsform_kode:N", title="Organisasjonsform (kode)", sort="-y"), y="count():Q")
        .properties(title="Fordeling av organisasjonsformer"),
        chart_selection=None,
    )

    organisasjonsform_chart
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""### Bill Dimensions Analysis""")
    return


@app.cell
def _(df):
    # Scatter plot of bill dimensions
    scatter = mo.ui.altair_chart(
        alt.Chart(df)
        .mark_point()
        .encode(
            x="bill_length_mm",
            y="bill_depth_mm",
            color="species",
            tooltip=["species", "bill_length_mm", "bill_depth_mm"],
        )
        .properties(title="Bill Length vs Depth by Species"),
        chart_selection=None,
    )

    scatter
    return


if __name__ == "__main__":
    app.run()
