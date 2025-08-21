import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    import pandas as pd
    import pyarrow  # pandas bruker pyarrow for å kunne lese parquet riktig
    import pyarrow.parquet as pq
    from pyodide.http import pyfetch  # pyfetch og io brukes for å laste ned fila
    import io


    file = mo.notebook_location() / 'public' / 'enheter_alle.parquet'
    return file, io, pq, pyfetch


@app.cell
async def _(file, io, pq, pyfetch):
    # Leser innholdet i parquet-fila i en pandas dataframe
    # Du finner resultatet under "Explore Data Sources" i menyen til venstre
    df = pq.read_table(
        io.BytesIO(await (await pyfetch(str(file))).bytes())
        ).to_pandas()
    return (df,)


@app.cell
def _(df):
    # Pandas lager sin egen indeks ved innlesning
    # Siden organisasjonsnummer er unikt, endrer vi til å bruke organisasjonsnummer som indeks
    df.set_index('organisasjonsnummer', inplace=True)
    return


@app.cell
def _(df):
    # Viser fem tilfeldig utvalgte rader fra dataframen
    # Kjør denne cellen flere ganger for å få frem andre eksempler
    df.sample(5)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Analyse med pandas

    Nedenfor er to enkle eksempler på bruk av pandas (laget ved hjelp av Gemini).

    Det første eksempelet bruker en innebygget funksjon som henter indeksen (dvs organisasjonsnummeret) til den forekomsten med høyest verdi (idmax()). Det pakkes inn i en funksjon for å hente ut en rad basert på indeks (loc).

    Det andre eksempelet viser hvor mange enheter som har mindre enn fem ansatte og samtidig er registrert i MVA-registeret. Det siste indikerer at de driver næringsvirksomhet.
    """
    )
    return


@app.cell
def _(df):
    # Hvilken enhet har flest ansatte?
    df.loc[df['antallAnsatte'].idxmax()]
    return


@app.cell
def _(df):
    # Hvor mange enheter med mindre enn fem ansatte er samtidig registrert i MVA-registeret?

    resultat = df[(df['antallAnsatte'] < 5) & (df['registrertIMvaregisteret'])].shape[0]

    print(f'Det er {resultat} antall enheter som har færre enn fem ansatte, og samtidig er registrert i MVA-registeret')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    Forklaring på pandas-koden i eksempelet over (laget med hjelp fra Gemini):

    1. df['antallAnsatte'] < 5: Denne delen lager en True/False maske for rader der antallet ansatte er under 5.
    2. df['registrertIMvaregisteret']: Denne delen lager en True/False maske for rader som er registrert i MVA-registeret.
    3. &: Symbolet & brukes til å kombinere de to boolean-maskene. Det returnerer True kun der begge maskene er True.
    4. df[...]: Masken brukes til å filtrere DataFrame-en og velge ut kun de radene som oppfyller begge kriteriene.
    5. .shape[0]: Til slutt brukes .shape[0] til å telle antall rader (enheter) i den filtrerte DataFrame-en. Du kan også bruke len(...) for å få det samme resultatet.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Analyse med SQL
    I tillegg til å bearbeide og analysere dataene ved hjelp av pandas, kan du også bruke SQL. Nedenfor er to eksempler.

    Det første lister alle enheter med mer enn 10000 ansatte, og viser hva slags type organisasjoner det er og hvilken kommune de har postadresse.

    Det andre eksempelet lister alle organisasjonsformene, og antallet av hver, sortert synkende.

    For mer informasjon om SQL-syntaksen, se [https://duckdb.org/docs/stable/sql/dialect/friendly_sql.html](https://duckdb.org/docs/stable/sql/dialect/friendly_sql.html)
    """
    )
    return


@app.cell
def _(df, mo):
    _df = mo.sql(
        f"""
        SELECT navn, antallAnsatte, organisasjonsform_beskrivelse, postadresse_kommune
            FROM df 
            WHERE antallAnsatte > 10000 
            ORDER BY antallAnsatte DESC
        """
    )
    return


@app.cell
def _(df, mo):
    _df = mo.sql(
        f"""
        SELECT 
            organisasjonsform_beskrivelse,
            COUNT(*) AS antall
        FROM 
            df
        GROUP BY 
            organisasjonsform_beskrivelse
        ORDER BY antall DESC;
        """
    )
    return


if __name__ == "__main__":
    app.run()
