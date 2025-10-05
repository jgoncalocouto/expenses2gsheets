from __future__ import annotations

import os, sys, json, logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

# TOML parsers
try:
    import tomllib  # py311+
except Exception:
    tomllib = None
try:
    import toml
except Exception:
    toml = None

import requests
import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from oauthlib.oauth2.rfc6749.errors import MismatchingStateError

SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/spreadsheets",
]

SECRETS_PATH_DEFAULT = os.path.join(".streamlit", "secrets.toml")
LOG_FILE = "test_oauth_gsheets.log"

# *** DEDICATED LOOPBACK REDIRECT (must be in Google Cloud Authorized redirects) ***
HOST = "127.0.0.1"
PORT = 8081
REDIRECT = f"http://{HOST}:{PORT}/"

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("oauth_test")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")
    ch = logging.StreamHandler(stream=sys.stdout); ch.setLevel(logging.DEBUG); ch.setFormatter(fmt)
    fh = RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=3, encoding="utf-8")
    fh.setLevel(logging.DEBUG); fh.setFormatter(fmt)
    logger.addHandler(ch); logger.addHandler(fh)
    return logger

log = setup_logging()

def load_secrets(path: str) -> dict:
    if not os.path.isfile(path):
        log.error("❌ Secrets TOML not found: %s", path); sys.exit(1)
    try:
        if tomllib:
            with open(path, "rb") as f: return tomllib.load(f)
        elif toml:
            with open(path, "r", encoding="utf-8") as f: return toml.load(f)
        else:
            log.error("❌ No TOML parser available. Use Python 3.11+ or `pip install toml`."); sys.exit(1)
    except Exception as e:
        log.exception("Failed to parse secrets TOML: %s", e); sys.exit(1)

def build_client_config(oauth_client: dict) -> dict:
    # Accept nested or flat secrets
    if "web" in oauth_client and isinstance(oauth_client["web"], dict):
        web = dict(oauth_client["web"])
    else:
        web = {k.split("web.", 1)[1]: v for k, v in oauth_client.items()
               if isinstance(k, str) and k.startswith("web.")}
    req = ["client_id","client_secret","auth_uri","token_uri","auth_provider_x509_cert_url"]
    missing = [k for k in req if k not in web]
    if missing: log.error("❌ Missing keys under [oauth_client.web]: %s", missing); sys.exit(1)

    # Trim strings
    for k in req:
        v = web[k]
        if not isinstance(v, str) or not v.strip():
            log.error("❌ oauth_client.web.%s must be a non-empty string.", k); sys.exit(1)
        web[k] = v.strip()

    # *** Force only our dedicated redirect ***
    web["redirect_uris"] = [REDIRECT]

    client_config = {"web": web}
    log.debug("Client config used:\n%s", json.dumps(client_config, indent=2))
    return client_config

def run_oauth(client_config: dict) -> Credentials:
    log.info("Starting OAuth on %s ...", REDIRECT)
    flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
    try:
        creds: Credentials = flow.run_local_server(
            host=HOST,
            port=PORT,
            authorization_prompt_message="Opening browser for Google login...",
            success_message="✅ Auth complete. You can close this tab.",
            open_browser=True,
        )
    except MismatchingStateError:
        log.error("❌ MismatchingStateError (state mismatch). Likely multiple tabs/callbacks or wrong redirect.")
        log.error("   Close all consent tabs, use one incognito window, ensure ONLY %s is authorized.", REDIRECT)
        sys.exit(1)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    log.info("OAuth finished OK.")
    return creds

def whoami(creds: Credentials) -> dict:
    r = requests.get("https://www.googleapis.com/oauth2/v3/userinfo",
                     headers={"Authorization": f"Bearer {creds.token}"}, timeout=30)
    r.raise_for_status()
    info = r.json()
    log.info("Signed in as: %s (%s)", info.get("email"), info.get("name"))
    return info

def prepend_a1(creds: Credentials, spreadsheet_id: str | None, spreadsheet_name: str | None, sheet: str):
    client = gspread.authorize(creds)
    sh = client.open_by_key(spreadsheet_id) if spreadsheet_id else client.open(spreadsheet_name)
    try:
        ws = sh.worksheet(sheet)
    except gspread.WorksheetNotFound:
        log.warning("Worksheet %s not found. Creating.", sheet)
        ws = sh.add_worksheet(title=sheet, rows=1000, cols=20)

    # Read current A1
    current = ""
    try:
        current = ws.acell("A1").value or ""
    except Exception:
        pass

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_val = f"{now}\n{current}" if current else now

    # ✅ Use either update_acell OR 2D list
    ws.update_acell("A1", new_val)
    # or: ws.update("A1", [[new_val]])

    sid = spreadsheet_id or sh.id
    url = f"https://docs.google.com/spreadsheets/d/{sid}/edit"
    log.info("Prepended timestamp to A1: %s", now)
    print(f"\nOpen your sheet: {url}\n")

def main():
    secrets_path = sys.argv[1] if len(sys.argv) > 1 else SECRETS_PATH_DEFAULT
    log.info("Using secrets at: %s", secrets_path)
    secrets = load_secrets(secrets_path)
    if "oauth_client" not in secrets or "app" not in secrets:
        log.error("❌ secrets.toml must have [oauth_client] and [app] blocks"); sys.exit(1)

    client_config = build_client_config(secrets["oauth_client"])
    app_cfg = secrets["app"]
    spreadsheet_id = app_cfg.get("spreadsheet_id")
    spreadsheet_name = app_cfg.get("spreadsheet_name")
    worksheet = app_cfg.get("worksheet_name", "Expenses")
    if not spreadsheet_id and not spreadsheet_name:
        log.error("❌ Provide [app].spreadsheet_id OR [app].spreadsheet_name"); sys.exit(1)

    creds = run_oauth(client_config)
    whoami(creds)
    prepend_a1(creds, spreadsheet_id, spreadsheet_name, worksheet)
    log.info("✅ Done.")

if __name__ == "__main__":
    main()
