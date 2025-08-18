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

__generated_with = "0.14.17"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    import polars as pl
    import altair as alt
    import pyarrow.parquet as pq
    import requests
    import os

    file = mo.notebook_location() / "public" / "enheter_alle.parquet"


@app.cell(hide_code=True)
def _():
    mo.md(
        """
    # Demo - Analyse av Enhetsregisteret i nettleseren

    Denne demoen leser inn en dump av Enhetsregisteret (NB! En gammel versjon, fra 2023), og viser hvordan Python-baserte verktøy kan brukes direkte i nettleseren, uten å måtte installere Python på maskinen.

    Kort om de forskjellige komponentene:

    * enheter_alle.parquet: En fil som inneholder tilsvarer innholdet som kan lastes ned fra data.brreg.no for Enhetsregisteret (hovedenheter). Laget ved å konvertere json- eller CSV-fila (husker ikke hvilken ...) til parquet-formatet.
    * marimo - selve "notatboken"; mange bruker Python i form av Jupyter Notebook, som lar brukeren mikse kode og tekst (markdown), som del av en analysejobb. Marimo er et forsøk på å lagre en bedre versjon ved å løse noen av utfordringene Jupyter har, som bl.a. bedre tilpasset versjonshåndtering med Git, mindre fare for inkonsistens mellom celler som endrer data, mer innebygget funksjonalitet for å få oversikt over dataene og bruke KI
    * polars - en utfordrer til et av de mest brukte verktøyene for dataanalyse, pandas. Polars har også "dataframe" som grunnleggende datastruktur, men er raskere, har bedre håndtering av kolonner med fritekst, og bruker funksjonalitet fra relasjonsdatabaser til å optimalisere rekkefølgen på spørringer før de utføres
    * duckdb - en database som fokuserer på dataanalyse, og som samhandler effektivt rundt "dataframes"-konseptet til pandas og polars
    * Altair er et bibliotek for å lage grafiske fremstillinger, som et alternativ til det mer kjente matplotlib

    Løsningen er tilgjengeliggjort ved hjelp av Github Pages, som en "statisk" side. Det vil si at all interaksjon, og all python-kode som kjøres enten den er skrevet på forhånd eller legges til av brukeren, kjøres i nettleseren. En måte å se det på er at moderne nettlesere tilbyr virtuelle maskiner, som er "sandboxed", og typisk har en grense på 2 GB arbeidsminne.
    """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    ## Les inn og få oversikt over dataene

    Første steg er å bruke polars funksjon for å lese data fra parquet. Etter innlesning brukes polars "head"-funksjon for å vise de første fem linjene. Skroll til høyre for å se alle kolonnene.

    Vi bruker variabelnavnet df (forkorteelse for "dataframe") for dataene. Det er vanlig praksis ved bruk av pandas og polars, selv om det ikke sier så mye om hvilke data det er der ...

    Merk at resultatet av kjøring av kode, presenteres _over_ koden. Det er kanskje ikke helt intuitivt ...
    """
    )
    return


@app.cell
def _():
    # Les Enhetsregisteret (NB!! Utdatert versjon av registeret!!!), og vis de første linjene
    # 
    # Dette er normalt bare to linjer kode, for polars kan lese parquet-filer direkte. Men akkurat 
    # funksjonen for å lese parquet-filer via http fungerer ikke når polars kjører i nettleseren,
    # derfor må vi bruke noen ekstra kodelinjer for først å finne ut om denne notatboken kjører 
    # direkte i nettleseren eller ei. Hvis den kjører i nettleseren må vi laste ned fila vi skal lese
    # til nettleserens virtuelle filsystem, og bruke pyarrow til å lese fila, før polars kan overta

    if "http" in str(file)[:4]:
        # filnavnet starter med http, m.a.o. kjører demoen i nettelseren,
        # og vi må laste ned filen vha requests før den kan leses

        # men først må vi anvende pyodide-prosjektets "lapping" av requests
        import pyodide_http
        pyodide_http.patch_all()
    
        r = requests.get(str(file), allow_redirects=True)
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

    df.head() # Viser de første fem postene i datasettet
    return (df,)


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    ## Oppsummerende informasjon om de ulike kolonnene

    Neste steg er å få vite litt mer om innholdet i hver kolonne.
    """
    )
    return


@app.cell
def _(df):
    mo.md(
        f"""
    ### Oversikt over innholdet i datasettet

    - Antall enheter (dvs antall rader): {df.height}
    - Antall kolonner: {len(df.columns)}

    ### Oppsummerende fakta/statistikk om de ulike kolonnene

    Disse oppsummeringene gir mest mening for kolonner som har tallverdier, f.eks. antallAnsatte.

    {mo.as_html(df.describe())}
    """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    ### Oversikt via marimos innebyggete verktøy

    Etter at dataene er lest inn er det også mulig å se mer detaljer om datasettet i menyen til venstre. Velg databasesymbolet og få opp liste over datakilder hvilke datakilder, dvs dataframes, som er tilgjengelig. Du skal bare se den ene, "df".

    Ved å klikke på den får du opp en liste over alle kolonnenen. For hver kolonne kan du se hvilken datatype kolonnen har, og ved å klikke på kolonnenavnet, får du i tillegg informasjon om antall verdier, tomme (null), og fordeling av verdier, f.eks. antall true og false, eller antall unike verdier.
    """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    ### Grafisk visning av fordeling av organisasjonsformer

    Nedenfor er en enkel visualisering som viser antall enheter fordelt på de ulike organisasjonsformene, sortert synkende.
    """
    )
    return


@app.cell
def _(df):
    # Vis fordeling av organisasjonsformer
    organisasjonsform_chart = mo.ui.altair_chart(
        alt.Chart(df)
        .mark_bar()
        .encode(x=alt.X("organisasjonsform_kode:N", title="Organisasjonsform (kode)", sort="-y"), y="count():Q")
        .properties(title="Fordeling av organisasjonsformer"),
        chart_selection=None,
    )

    organisasjonsform_chart
    return


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    ## Analyse med bruk av SQL

    Cellene i marimo kan være av tre ulike typer; markdown (som denne) for å skrive tekst. Python for ren python-kode. Eller SQL for å skrive rene sql-spørringer. Det siste krever at man også installerer duckdb som database. For de som har erfaring med databaser og sql er det et godt alternativ til å lære seg polars/pandas.

    I eksempelet nedenfor er det en spørring for å finne alle enheter med antall ansatte lik 5.
    """
    )
    return


@app.cell
def _(df):
    _df = mo.sql(
        f"""
        SELECT * FROM df where antallAnsatte = 5
        """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    ### Minneforbruk og filstørrelser

    Versjonen av Enhetesregisteret i parquet-filen er optimalisert for å lagre data på en kompakt måte, bl.a. ved at alle kolonner som har et begrenset antall mulige verdier (organisasjonsform, institusjonell kode, landkode etc) er endret fra standardtypen fritekst (str) til en datatype for kategorier (Categorical). Det reduserer lagringsbehovet, og gjør også analyser raskere. For å få et esitmat over hvor stor plass dataframen med ER tar i minnet, kan vi bruke polars estimated_size:
    """
    )
    return


@app.cell
def _(df):
    mo.md(f"""Det estimerte minneforbruket for dataframen df er {df.estimated_size(unit="gb")} gb""")
    return


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    Filstørrelser

    Nedenfor er oversikt over størrelse på filene for nedlasting, pr 13. august 2025:

    * Zippet json: ca 17O mb
    * Zippet CSV: ca 130 mb
    * Excel: ca 370 mb

    Til sammenligning er parquet-fila (ferdig optimalisert for analyse) på ca 73 mb (NB! Basert på nedlastede data fra 2023), altså nesten bare halvparten så stor som det minste alternativet (CSV).
    """
    )
    return


if __name__ == "__main__":
    app.run()
