import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(
        r"""
    # Enheter inkludert roller

    Koden på denne siden lager en pandas dataframe som slår sammen data fra enhetsregisteret (hovedenheter) med data om roller for de samme enhetene. Hver type rolle (DAGL, MEDL) er en egen kolonne i den nye dataframen.

    I de skjulte cellene nedenfor er funksjoner som gjør selve jobben. Klikk på dem for å se koden.

    **NB! Når resultatet vises mangler noen kolonner i forhåndsvisningen i Marimoj!** For å se alle dataene, velg "Download"-linken, og velg et av alternativene, f.eks. kopier til utklippstavle og lim inn i Excel.
    """
    )
    return


@app.cell
def _():
    from typing import Any, Dict, List
    import requests
    import pandas as pd
    return Any, Dict, List, pd, requests


@app.cell
def _():
    # Nødvendig kode for at dette skal fungere med pyodide/wasm
    from pyodide_http import patch_all
    patch_all()
    return


@app.cell(hide_code=True)
def _(Any, Dict, requests):
    def hent_enheter_fra_brreg(hjemmeside: str) -> list[Dict[str, Any]]:
        """
        Henter enheter fra Brønnøysundregistrenes API.

        Funksjonen søker etter enheter basert på en spesifisert hjemmeside.
        Fjerner de delene av svaret som ikke er data om enheter.

        Args:
            hjemmeside: Hjemmesiden (domenet) det skal søkes etter.

        Returns:
            En liste med en dictionary med data for hver enhet som blir funnet.
        """
        # Basis-URL for enhetsregisterets API.
        base_url: str = 'https://data.brreg.no/enhetsregisteret/api/enheter'
        url: str = f'{base_url}?hjemmeside={hjemmeside}'

        try:
            # Sender en HTTP GET-forespørsel.
            response = requests.get(url)
            # Sjekker for feil i HTTP-statuskoden (f.eks. 404, 500).
            response.raise_for_status()
            # Konverterer svaret fra JSON til en Python dictionary.
            data: Dict[str, Any] = response.json()

            # Henter listen over enheter fra responsen.
            # .get('_embedded', {}) returnerer en tom dictionary hvis '_embedded' ikke finnes.
            # .get('enheter', []) returnerer en tom liste hvis 'enheter' ikke finnes.
            # Dette er en trygg måte å hente data på uten å risikere en KeyError.
            enheter_liste: list = data.get('_embedded', {}).get('enheter', [])
            return enheter_liste

        except requests.exceptions.RequestException as e:
            # Håndterer nettverksfeil eller andre problemer med forespørselen.
            return {"feilmelding": f"Kunne ikke hente data for '{hjemmeside}': {e}"}


    return (hent_enheter_fra_brreg,)


@app.cell(hide_code=True)
def _(Any, Dict, requests):
    def hent_roller_for_enhet(organisasjonsnummer: str) -> Dict[str, Any]:
        """
        Henter rollegrupper (f.eks. styre, daglig leder) for en spesifikk enhet
        fra Brønnøysundregistrenes API.

        Funksjonen bygger en URL spesifikt for å hente roller basert på et
        organisasjonsnummer, sender en HTTP GET-forespørsel, og returnerer
        resultatet. Den inkluderer robust feilhåndtering.

        Args:
            organisasjonsnummer: Organisasjonsnummeret til enheten det skal
                                 hentes roller for.

        Returns:
            En dictionary som inneholder API-responsen med rollegrupper,
            eller en dictionary med en feilmelding hvis noe gikk galt.
        """
        # Basis-URL for enhetsregisterets API.
        base_url: str = 'https://data.brreg.no/enhetsregisteret/api/enheter'

        # Vi bygger den fullstendige URL-en ved å legge til organisasjonsnummeret
        # og endepunktet '/roller'.
        url: str = f'{base_url}/{organisasjonsnummer}/roller'

        try:
            # Sender en HTTP GET-forespørsel til den konstruerte URL-en.
            response = requests.get(url)

            # Kaster en exception hvis statuskoden indikerer en feil (f.eks. 404 Not Found).
            response.raise_for_status()

            # Hvis alt gikk bra, konverterer vi JSON-svaret til en Python dictionary
            # og returnerer det.
            return response.json()

        except requests.exceptions.HTTPError as e:
            # Håndterer spesifikt HTTP-feil. For eksempel, hvis org.nr. ikke finnes,
            # vil API-et returnere statuskode 404, som fanges opp her.
            if e.response.status_code == 404:
                return {"feilmelding": f"Fant ingen enhet eller roller for organisasjonsnummer '{organisasjonsnummer}'. Sjekk om nummeret er korrekt."}
            else:
                return {"feilmelding": f"En HTTP-feil oppstod for '{organisasjonsnummer}': {e}"}

        except requests.exceptions.RequestException as e:
            # Håndterer andre nettverksrelaterte feil, som f.eks. manglende internettforbindelse.
            return {"feilmelding": f"Kunne ikke koble til API-et for å hente roller for '{organisasjonsnummer}': {e}"}

    return (hent_roller_for_enhet,)


@app.cell(hide_code=True)
def _(Any, Dict, List):
    def forenkle_roller_dict(orgnr: str, roller_data) -> list:
        """
        Forenkler kompleks/dypt nøsted dict som gjenspeiler JSON-dataene vi får fra 
        API-et via fra hent_roller_for_enhet(), til en enklere dict med organisasjonsnummer,
        rolleetype, navn (på person).

        Utelater roller der rollen er knyttet til en annen enhet

        Organisasjonsnummer må inngå i kallet siden det ikke er inkludert i dataene om roller
        (bortsett fra som del av URL-en for 'enhet')

        Args:
            orgnr: Organisasjonsnummeret til enheten
            roller_data: Dictionary med rolleinformasjon, som returnert fra hent_roller_for_enhet()

        Returns:
            Liste med dicts med organisasjonsnummere, rolletype og navn (på person)"""

        # Steg 1: Forbered en liste til å holde på de formaterte dataene.
        formaterte_roller: List[Dict[str, str]] = []

        # Steg 4: Iterer gjennom den komplekse datastrukturen for å trekke ut informasjonen vi trenger.
        # Strukturen er: rollegrupper -> roller -> person -> navn.
        # Vi bruker .get() med en tom liste som standardverdi for å unngå feil hvis en nøkkel mangler.
        for rollegruppe in roller_data.get("rollegrupper", []):
            for rolle in rollegruppe.get("roller", []):
                # Vi er kun interessert i roller som er knyttet til en person.
                # Noen roller kan være knyttet til en annen 'enhet', disse hopper vi over.
                if "person" in rolle:
                    person_data: Dict[str, Any] = rolle["person"]
                    navn_data: Dict[str, str] = person_data.get("navn", {})

                    # Bygg sammen fullt navn fra fornavn, mellomnavn og etternavn.
                    fornavn: str = navn_data.get("fornavn", "")
                    mellomnavn: str = navn_data.get("mellomnavn", "")
                    etternavn: str = navn_data.get("etternavn", "")

                    # Slår sammen navnedelene til én enkelt streng.
                    # filter(None, ...) fjerner tomme strenger (f.eks. hvis mellomnavn mangler),
                    # og ' '.join(...) setter mellomrom mellom de gjenværende delene.
                    fullt_navn: str = ' '.join(filter(None, [fornavn, mellomnavn, etternavn]))

                    # Hent beskrivelsen av rollen (f.eks. "Styrets leder").
                    rolletype: str = rolle.get("type", {}).get("beskrivelse", "Ukjent rolle")

                    # Legg til en dictionary med den formaterte informasjonen i listen vår.
                    formaterte_roller.append({
                        "organisasjonsnummer": orgnr,
                        "rolletype": rolletype,
                        "navn": fullt_navn
                    })

        return formaterte_roller


    return (forenkle_roller_dict,)


@app.cell(hide_code=True)
def _(Any, Dict, forenkle_roller_dict, hent_roller_for_enhet, pd):
    def lag_rolletabell_for_enheter(organisasjonsnumre: list) -> pd.DataFrame:
        """
        Henter roller for et gitt organisasjonsnummer og konverterer den komplekse
        JSON-strukturen til en flat pandas DataFrame.

        Funksjonen bruker `hent_roller_for_enhet` for å hente rådata, og
        deretter forenkle_roller_dict for å transformere dataen til en tabell
        med kolonnene:
        'organisasjonsnummer', 'rolletype', og 'navn'.

        Args:
            organisasjonsnummer: Organisasjonsnummeret til enheten.

        Returns:
            En pandas DataFrame med roller, eller en tom DataFrame hvis ingen
            roller ble funnet eller en feil oppstod.
        """
        roller_liste = []
        for orgnr in organisasjonsnumre:

            # Steg 1: Hent rådata fra API-et ved å kalle den eksisterende funksjonen.
            roller_data: Dict[str, Any] = hent_roller_for_enhet(orgnr)

            # Steg 2: Sjekk for feilmeldinger fra API-kallet. Hvis det er en feil,
            # skriver vi ut en melding og returnerer en tom DataFrame for å unngå feil senere.
            if "feilmelding" in roller_data:
                print(f"Feil ved henting av roller for {orgnr}: {roller_data['feilmelding']}")
                return pd.DataFrame()

            # Steg 3: Bruk forenkle_roller_dict til å trekke ut de relvante dataene av roller_data
            # og utvid roller_liste med de nye rollene
            roller_liste.extend(forenkle_roller_dict(orgnr, roller_data))

        # Steg 5: Konverter listen med dictionaries til en pandas DataFrame.
        df_roller_flat: pd.DataFrame = pd.DataFrame(roller_liste)

        return df_roller_flat
    return (lag_rolletabell_for_enheter,)


@app.cell(hide_code=True)
def _(pd):
    def utvid_enheter_med_roller(enheter_df: pd.DataFrame, roller_df: pd.DataFrame) -> pd.DataFrame:
        """
        Kombinerer en DataFrame med enhetsinformasjon med en DataFrame med rolleinformasjon.

        Funksjonen pivoterer rolle-dataframen slik at hver rolletype blir en egen
        kolonne, og slår deretter denne informasjonen sammen med den opprinnelige
        enhets-dataframen basert på organisasjonsnummer. Dette er nyttig for å få
        en flat tabell med nøkkelpersoner direkte knyttet til hver enhet.

        Args:
            enheter_df: En DataFrame med grunnleggende informasjon om enheter,
                        må inneholde en 'organisasjonsnummer'-kolonne.
            roller_df: En DataFrame med roller, må inneholde kolonnene
                       'organisasjonsnummer', 'rolletype', og 'navn'.

        Returns:
            En ny, utvidet DataFrame hvor enhetsinformasjonen er beriket med
            kolonner for hver unike rolletype.
        """
        # Sjekker om rolle-tabellen er tom. Hvis den er det, er det ingenting å
        # slå sammen, så vi returnerer den opprinnelige enhets-tabellen.
        if roller_df.empty:
            return enheter_df.copy()

        # Steg 1: Pivotere roller_df.
        # Vi omformer tabellen slik at hver rad fortsatt representerer ett unikt
        # organisasjonsnummer, men hver unike 'rolletype' blir en ny kolonne.
        # Verdien i de nye kolonnene blir navnet på personen med den rollen.
        # Siden det kan være flere personer med samme rolle (f.eks. flere styremedlemmer),
        # bruker vi `aggfunc` til å slå sammen navnene til en enkelt streng, separert med komma.
        roller_pivotert: pd.DataFrame = roller_df.pivot_table(
            index='organisasjonsnummer',
            columns='rolletype',
            values='navn',
            aggfunc=', '.join
        ).reset_index() # Gjør 'organisasjonsnummer' om fra en indeks til en vanlig kolonne.

        # Steg 2: Slå sammen (merge) den opprinnelige enhets-dataframen med den pivoterte rolle-dataframen.
        # Vi bruker en 'left' merge for å sikre at vi beholder alle enhetene fra
        # den opprinnelige dataframen, selv om de ikke har noen roller registrert.
        # 'on="organisasjonsnummer"' forteller pandas hvilken kolonne som skal brukes for å koble dem sammen.
        df_utvidet: pd.DataFrame = pd.merge(
            enheter_df,
            roller_pivotert,
            on='organisasjonsnummer',
            how='left'
        )

        return df_utvidet
    return (utvid_enheter_med_roller,)


@app.cell
def _():
    domener = [ # de domenene det skal søkes etter
        'www.nho.no',
        'www.brreg.no',
        'www.dagbladet.no'
    ]
    return (domener,)


@app.cell
def _(
    Any,
    Dict,
    domener,
    hent_enheter_fra_brreg,
    lag_rolletabell_for_enheter,
    pd,
    utvid_enheter_med_roller,
):
    # Steg 1: Starter med å lage en liste som skal lagre alle enhetene vi finner
    enheter: list[Dict[str, Any]] = []

    # Steg 2: For hvert domene i listen domener (se cellen over), henter vi enheter:
    for domene in domener:
        enheter.extend(hent_enheter_fra_brreg(domene))

    # Steg 3: Lager en dataframe av listen med enheter, og bruker funksjonalitet 
    # i pandas for å "forflate" dype JSON-strukturer (json_normalize) 
    df_enheter = pd.DataFrame.from_dict(pd.json_normalize(enheter, sep='_'))

    # Steg 4: Hente ut innholdet i kolonnen organisasjonsnummer til
    # bruk for å hente roller i neste steg
    organisasjonsnumre = list(df_enheter['organisasjonsnummer'])

    # Steg 5: Hente roller for organisasjonsnumrene og lagre i en egen dataframe
    df_roller = lag_rolletabell_for_enheter(organisasjonsnumre)

    # Deretter kaller vi funksjonen med enhets-dataframen og rolle-tabellen.
    df_enheter_og_roller = utvid_enheter_med_roller(df_enheter, df_roller)

    # Til slutt viser vi den nye, komplette DataFrame-en.
    # Nå inneholder den både den opprinnelige informasjonen og nye kolonner for roller.
    df_enheter_og_roller
    return


if __name__ == "__main__":
    app.run()
