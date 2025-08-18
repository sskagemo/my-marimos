import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import polars as pl
    import pyarrow.parquet as pq
    return mo, pd, pl, pq


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Feilsøking av parquet-fil

    Status: Har en marimo-basert demo som leser enheter_alle.parquet ved hjelp av pl.read_parquet(), og det fungerer fint når jeg kjører en tradisjonell python, dvs marimo server lokalt på min maskin.

    Forsøker det samme med python i nettleseren (pyodide), og da feiler det. Det er en kjent begrensning i polars, tydeligvis.

    Fant en workaround ved bruk av pyarrow.parquet, som forutsetter at man først bruker requests til å hente fila og skrive den til et lokalt filobjekt. Men da får jeg feilmelding om at det kanskje ikke er en ekte parquet-fil (pyarrow), eller at det ikke er en gz-fil (pandas), eller "no magic byte" (duckdb)

    Planen nå er først å prøve å lese fila som en lokal fil fra filsystemet, med de tre overnneevnte verktøyene. Deretter forsøke å lese den fra Github.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Resultater

    ### Lokal fil - /public/enheter_alle.parquet

    - REFERANSE: Polars direkte: Fungerer. Bevarer kategorier
        - Størrelse: 250 mb
        - Tidsbruk 500 ms
    - Polars via Pyarrow: Fungerer. Bevarer kategorier
        - Størrelse: 172 mb (overraskende at den er mindre en polars direkte)
        - Tidsbruk: 900 ms
    - Pandas: Fungerer, forutsatt pyarrow som dtype_backend. Ser ut til å bevare kategorier (som "dict")
        - Størrelse på df er ca 390 MB
        - tidsbruk: 730 ms
    - duckdb. Bevarer *ikke* kategorier. Resultatet er en polars df
        - Størrelse: 213 mb
        - Tidsbruk:  3,2 sekunder (skyldes muligens at det var første duckdb-kommando)

    ### http: fra github pages

    Fungerte tilsvarende de over, med unntak av polars via pyarrow; der fungerte det ikke å lese direkte fra http, så isteden måtte jeg bruke requests for å laste ned og skrive en lokal fil, og deretter lese den.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Lokal fil""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### REFERANSE: Polars direkte (metoden som ikke fungerer på pyodide)""")
    return


@app.cell
def _(pl):
    df_polars_direkte = pl.read_parquet('notebooks/public/enheter_alle.parquet')
    return (df_polars_direkte,)


@app.cell
def _(df_polars_direkte):
    df_polars_direkte.estimated_size(unit="mb")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Polars via pyarrow.parquet""")
    return


@app.cell
def _(pl, pq):
    df_polars = pl.from_arrow(pq.read_table('notebooks/public/enheter_alle.parquet'))
    return (df_polars,)


@app.cell
def _(df_polars):
    df_polars.estimated_size(unit='mb')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Pandas""")
    return


@app.cell
def _(pd):
    df_pandas = pd.read_parquet('notebooks/public/enheter_alle.parquet', dtype_backend='pyarrow')
    return (df_pandas,)


@app.cell
def _(df_pandas):
    df_pandas.info(memory_usage="mb")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### duckdb""")
    return


@app.cell
def _(mo):
    df_duckdb = mo.sql(
        f"""
        select * from read_parquet('notebooks/public/enheter_alle.parquet')
        """
    )
    return (df_duckdb,)


@app.cell
def _(df_duckdb):
    df_duckdb.estimated_size(unit="mb")
    return


@app.cell
def _(df_duckdb):
    type(df_duckdb)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Fil via http (GH pages)""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Polars direkte""")
    return


@app.cell
def _(pl):
    df_polars_direkte_http = pl.read_parquet('http://sskagemo.github.io/my-marimos/notebooks/public/enheter_alle.parquet')
    return (df_polars_direkte_http,)


@app.cell
def _(df_polars_direkte_http):
    df_polars_direkte_http.estimated_size(unit='mb')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Polars via pyarrow""")
    return


@app.cell
def _():
    import requests
    return (requests,)


@app.cell
def _(df_polars_http, file, os, pl, pq, requests):
    if "http" in str(file)[:4]:
        # filnavnet starter med http, m.a.o. kjører demoen i nettelseren,
        # og vi må laste ned filen vha requests før den kan leses

        # men først må vi anvende pyodide-prosjektets "lapping" av requeests
        import pyodide_http
        pyodide_http.patch_all()

        r = requests.get(str(file),
                         allow_redirects=True)
        r.raise_for_status()
        with open('data.parquet', 'wb') as f:
            f.write(r.content)  # lagrer fila i nettleserens virtuelle filsystem

        df = pl.from_arrow(pq.read_table('data.parquet'))

        # for å redusere minnebruken, fjerner vi den lokale filen når vi har lest inn dataene:
        try:
            os.remove('data.parquet')
            print("Filen ble slettet.")
        except FileNotFoundError:
            print("Filen finnes ikke.")
    else:
        # dette er den normale måten å gjøre det på med polars, og 
        # vil antagelig også fungere i fremtiden når python i nettleseren
        # blir like normalt som å ha python installert på egen maskin
        df = pl.read_parquet(str(file))

    df_polars_http.head() # Viser de første fem postene i datasettet
    return


@app.cell
def _(pl, pq, requests):
    r = requests.get('http://sskagemo.github.io/my-marimos/notebooks/public/enheter_alle.parquet', allow_redirects=True)
    r.raise_for_status()
    with open('data.parquet', 'wb') as f:
        f.write(r.content)

    # table = pq.read_table('data.parquet')
    # df = pl.from_arrow(table)

    df_polars_http = pl.from_arrow(pq.read_table('data.parquet'))
    return (df_polars_http,)


@app.cell
def _(df_polars_http):
    df_polars_http.estimated_size(unit='mb')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Pandas""")
    return


@app.cell
def _(pd):
    df_pandas_http = pd.read_parquet('http://sskagemo.github.io/my-marimos/notebooks/public/enheter_alle.parquet', dtype_backend='pyarrow')
    return (df_pandas_http,)


@app.cell
def _(df_pandas_http):
    df_pandas_http.info(memory_usage="mb")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### duckdb""")
    return


@app.cell
def _(mo):
    df_duckdb_http = mo.sql(
        f"""
        select * from read_parquet('http://sskagemo.github.io/my-marimos/notebooks/public/enheter_alle.parquet')
        """
    )
    return


if __name__ == "__main__":
    app.run()
