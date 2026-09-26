#!/usr/bin/env python3
"""
Récupère les vols récents d'un avion (par défaut le Cirrus SR22 N155HR,
icao24 = a0dd81) via l'API OpenSky Network, et dépose un fichier CSV
dans un dossier Google Drive.

Conçu pour tourner comme une tâche planifiée GitHub Actions (voir
.github/workflows/fetch-flights.yml), sans état persistant.

L'API OpenSky partitionne ses données par jour calendaire UTC et
refuse toute requête qui chevauche plus de 2 partitions (jours). On
interroge donc un jour calendaire UTC complet à la fois, en remontant
sur plusieurs jours à chaque exécution (recouvrement volontaire avec
les exécutions précédentes, pour ne jamais rater un vol même en cas
de run manqué). Les doublons éventuels entre deux fichiers successifs
sont dédupliqués en aval (par Claude, lors du traitement du dossier
Drive), sur la base du couple (icao24, firstSeen).

Variables d'environnement attendues :
- OPENSKY_CLIENT_ID, OPENSKY_CLIENT_SECRET : identifiants API OpenSky
  (générés depuis la page "API Client" de ton compte OpenSky)
- GDRIVE_SA_KEY : contenu JSON complet de la clé de compte de service
  Google Cloud (Drive API activée)
- GDRIVE_FOLDER_ID : ID du dossier Google Drive cible (partagé en
  "Éditeur" avec l'adresse e-mail du compte de service)
- AIRCRAFT_ICAO24 (optionnel) : adresse icao24 en minuscules,
  par défaut "a0dd81" (N155HR)
- DAYS_BACK (optionnel) : nombre de jours calendaires UTC à interroger
  en remontant depuis aujourd'hui, par défaut 3 (aujourd'hui, hier,
  avant-hier)
"""

import os
import sys
import tempfile
import time
import csv
from datetime import datetime, timezone, timedelta

import requests

OPENSKY_TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network"
    "/protocol/openid-connect/token"
)
OPENSKY_API_BASE = "https://opensky-network.org/api"


def get_opensky_token(client_id: str, client_secret: str) -> str:
    resp = requests.post(
        OPENSKY_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def fetch_flights(token: str, icao24: str, begin: int, end: int, max_retries: int = 6) -> list:
    """Interroge l'API OpenSky pour un aircraft sur une fenêtre donnée.

    Réessaie automatiquement en cas de 429 (trop de requêtes), avec une
    attente croissante, en respectant l'en-tête Retry-After si présent.
    """
    attempt = 0
    while True:
        resp = requests.get(
            f"{OPENSKY_API_BASE}/flights/aircraft",
            params={"icao24": icao24, "begin": begin, "end": end},
            headers={"Authorization": f"Bearer {token}"},
            timeout=60,
        )
        if resp.status_code == 404:
            # OpenSky renvoie 404 quand aucun vol n'est trouvé sur la période
            return []

        if resp.status_code == 429:
            attempt += 1
            if attempt > max_retries:
                print(f"Réponse OpenSky 429 persistante après {max_retries} tentatives, "
                      f"abandon pour cette journée.", file=sys.stderr)
                resp.raise_for_status()
            retry_after = resp.headers.get("Retry-After")
            wait_s = float(retry_after) if retry_after else min(10 * attempt, 90)
            print(f"Réponse OpenSky 429 (trop de requêtes), nouvelle tentative dans "
                  f"{wait_s:.0f}s (essai {attempt}/{max_retries})...", file=sys.stderr)
            time.sleep(wait_s)
            continue

        if not resp.ok:
            print(f"Réponse OpenSky {resp.status_code} : {resp.text}", file=sys.stderr)
        resp.raise_for_status()
        return resp.json()


def to_iso(ts):
    if ts is None:
        return ""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def day_bounds_utc(date_obj):
    """Retourne (begin_epoch, end_epoch) pour une journée calendaire UTC complète."""
    start = datetime(date_obj.year, date_obj.month, date_obj.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return int(start.timestamp()), int(end.timestamp())


def write_csv(flights: list, icao24: str, out_path: str):
    fieldnames = [
        "icao24",
        "callsign",
        "firstSeen_epoch",
        "firstSeen_utc",
        "lastSeen_epoch",
        "lastSeen_utc",
        "duree_minutes",
        "depart_icao_estime",
        "arrivee_icao_estime",
        "depart_distance_horiz_m",
        "arrivee_distance_horiz_m",
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for fl in flights:
            first_seen = fl.get("firstSeen")
            last_seen = fl.get("lastSeen")
            duree = None
            if first_seen is not None and last_seen is not None:
                duree = round((last_seen - first_seen) / 60, 1)
            writer.writerow(
                {
                    "icao24": fl.get("icao24", icao24),
                    "callsign": (fl.get("callsign") or "").strip(),
                    "firstSeen_epoch": first_seen,
                    "firstSeen_utc": to_iso(first_seen),
                    "lastSeen_epoch": last_seen,
                    "lastSeen_utc": to_iso(last_seen),
                    "duree_minutes": duree,
                    "depart_icao_estime": fl.get("estDepartureAirport") or "",
                    "arrivee_icao_estime": fl.get("estArrivalAirport") or "",
                    "depart_distance_horiz_m": fl.get("estDepartureAirportHorizDistance"),
                    "arrivee_distance_horiz_m": fl.get("estArrivalAirportHorizDistance"),
                }
            )


def upload_to_drive(local_path: str, filename: str, folder_id: str, sa_key_json: str):
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tmp:
        tmp.write(sa_key_json)
        key_path = tmp.name

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/drive.file"]
    )
    service = build("drive", "v3", credentials=credentials)

    file_metadata = {"name": filename, "parents": [folder_id]}
    media = MediaFileUpload(local_path, mimetype="text/csv")
    uploaded = (
        service.files()
        .create(body=file_metadata, media_body=media, fields="id, name, webViewLink")
        .execute()
    )
    return uploaded


def main():
    client_id = os.environ.get("OPENSKY_CLIENT_ID")
    client_secret = os.environ.get("OPENSKY_CLIENT_SECRET")
    sa_key_json = os.environ.get("GDRIVE_SA_KEY")
    folder_id = os.environ.get("GDRIVE_FOLDER_ID")
    icao24 = os.environ.get("AIRCRAFT_ICAO24", "a0dd81").lower()
    days_back = int(os.environ.get("DAYS_BACK", "3"))

    missing = [
        name
        for name, val in [
            ("OPENSKY_CLIENT_ID", client_id),
            ("OPENSKY_CLIENT_SECRET", client_secret),
            ("GDRIVE_SA_KEY", sa_key_json),
            ("GDRIVE_FOLDER_ID", folder_id),
        ]
        if not val
    ]
    if missing:
        print(f"Variables d'environnement manquantes : {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    token = get_opensky_token(client_id, client_secret)

    now = int(time.time())
    today = datetime.now(timezone.utc).date()

    all_flights = []
    seen_keys = set()
    failed_days = []

    for i in range(days_back):
        day = today - timedelta(days=i)
        begin, end = day_bounds_utc(day)
        if begin > now:
            continue
        end = min(end, now)
        if end <= begin:
            continue

        print(f"[{i+1}/{days_back}] Récupération des vols pour icao24={icao24} le "
              f"{day.isoformat()} UTC ({to_iso(begin)} -> {to_iso(end)})...")
        try:
            day_flights = fetch_flights(token, icao24, begin, end)
        except requests.exceptions.HTTPError as exc:
            print(f"  -> échec définitif pour le {day.isoformat()} ({exc}), "
                  f"on continue avec les jours suivants.", file=sys.stderr)
            failed_days.append(day.isoformat())
            continue

        if day_flights:
            print(f"  -> {len(day_flights)} vol(s) trouvé(s) ce jour-là.")

        for fl in day_flights:
            key = (fl.get("icao24"), fl.get("firstSeen"))
            if key not in seen_keys:
                seen_keys.add(key)
                all_flights.append(fl)

        # Pause entre les requêtes pour rester correct vis-à-vis des limites
        # de débit de l'API, surtout utile sur un grand backfill.
        time.sleep(1.5)

    print(f"{len(all_flights)} vol(s) au total sur la période interrogée.")
    if failed_days:
        print(f"Attention : {len(failed_days)} jour(s) n'ont pas pu être récupérés "
              f"malgré les tentatives : {', '.join(failed_days)}. Relance le workflow "
              f"plus tard avec un DAYS_BACK ciblé pour compléter ces jours-là.",
              file=sys.stderr)

    if not all_flights:
        print("Aucun vol à exporter, fin du script.")
        return

    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"vols_{icao24}_{run_stamp}.csv"
    local_path = os.path.join(tempfile.gettempdir(), filename)
    write_csv(all_flights, icao24, local_path)

    uploaded = upload_to_drive(local_path, filename, folder_id, sa_key_json)
    print(f"Fichier déposé sur Google Drive : {uploaded.get('name')} "
          f"(id={uploaded.get('id')})")


if __name__ == "__main__":
    main()
