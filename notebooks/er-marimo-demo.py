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
    import pandas as pd # kan kanskje fjerne denne, hvis jeg ikke bruker pandas?

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


@app.cell
def _():
    mo.md(
        r"""
    ## Les inn, få oversikt over, og tilpasse dataene

    Første steg er å bruke polars funksjon for å lese data fra parquet. Etter innlesning brukes polars "head"-funksjon for å vise de første fem linjene. Skroll til høyre for å se alle kolonnene.

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

    mo.md(f"""
    ### Oversikt over innholdet i datasettet

    - Antall enheter: {df.height}
    - Kolonner: {', '.join(df.columns)}

    ### Oppsummerende fakta/statistikk om de ulike kolonnene

    {mo.as_html(df.describe())}
    """)


    return


@app.cell
def _():
    mo.md(
        r"""
    ### Oversikt via marimos innebyggete verktøy
    
    Etter at dataene er lest inn er det også mulig å se mer detaljer om datasettet i menyen til venstre. Velg databasesymbolet og få opp liste over datakilder, dvs dataframes. For hver kolonne kan du se hvilken datatype kolonnen har, og ved å klikke på kolonnenavnet, får du i tillegg informasjon om antall verdier, tomme (null), og fordeling av verdier, f.eks. antall true og false, eller antall unike verdier.
    """
    )
    return


@app.cell
def _():
    mo.md(
        r"""
    ## Optimalisere datasettet for analyse

    Det er to hovedsvakheter ved måten dataene er representert nå. For det første inkluderer mange kolonnenavn punktum, og det skaper erfaringsmessig problemer. Derfor bør alle kolonnene med punktum døpes om, og punktum erstattes med _.

    Det andre problemet er at veldig mange kolonnner har datatypen str, dvs fritekst. Samtidig er det i mange tilfeller bare et lite antall mulige verdier, f.eks. institusjoneell sektorkode (ulike verdier), organisasjonsform (41 ulike verdier), næringskode (805 ulike verdier). For disse er det bedre å gjøre om til datatypen "Categorical". Det samme gjelder antageligvis også for postnummer/poststed. Det beste ville være å gå systematisk gjennom og se om minnebruken reduseres ved endring av en kolonne.

    Vi kan bruke polars estimated_size() for å informasjon om minnebruken før vi gjør noe.
    """
    )
    return


@app.cell
def _(df):
    df.estimated_size(unit="gb")
    return


@app.cell
def _():
    mo.md(
        r"""
    ### Endre fra punktum til understrek

    Vi bruker klassisk python til å lage en mappingtabell som vi deretter kan bruke for å endre navn.
    """
    )
    return


@app.cell
def _(df):
    mapping_kolonnenavn = {kolonnenavn: kolonnenavn.replace('.','_') for kolonnenavn in df.columns if '.' in kolonnenavn }
    return (mapping_kolonnenavn,)


@app.cell
def _():
    mo.md(r"""I samme omgang som vi endrer navnene, vil vi også endre til datatypen Categorical for relevante kolonner. Listen over disse er laget manuelt.""")
    return


@app.cell
def _():
    categorical_kolonner = ['organisasjonsform.kode', 'organisasjonsform.beskrivelse', 'naeringskode1.kode', 'naeringskode1.beskrivelse', 'naeringskode2.kode', 'naeringskode2.beskrivelse','naeringskode3.kode', 'naeringskode3.beskrivelse', 'postadresse.poststed', 'postadresse.postnummer', 'postadresse.kommune', 'postadresse.kommunenummer', 'postadresse.land', 'forretningsadresse.landkode','forretningsadresse.poststed', 'forretningsadresse.postnummer', 'forretningsadresse.kommune', 'forretningsadresse.kommunenummer', 'forretningsadresse.land', 'forretningsadresse.landkode', 'institusjonellSektorkode.kode','institusjonellSektorkode.beskrivelse', 'maalform']
    return (categorical_kolonner,)


@app.cell
def _(categorical_kolonner, df, mapping_kolonnenavn):
    # endre til categorical og fjern punktum
    df_forbedret = df.with_columns(pl.col(categorical_kolonner)
        .cast(pl.Categorical)).rename(mapping_kolonnenavn)
    return (df_forbedret,)


@app.cell
def _(df_forbedret):
    df_forbedret.estimated_size(unit='gb')
    return


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
