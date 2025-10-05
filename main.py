from __future__ import annotations

import json
from datetime import date, datetime
from collections.abc import Mapping

import gspread
import pandas as pd
import requests
import streamlit as st
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from oauthlib.oauth2.rfc6749.errors import MismatchingStateError

# =========================================
# CONFIG
# =========================================
SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/spreadsheets",
]

HOST = "127.0.0.1"
PORT = 8081
REDIRECT = f"http://{HOST}:{PORT}/"

st.set_page_config(page_title="Expense Logger (OAuth)", layout="wide")
st.title("Expense Logger - Per-User Google Login (OAuth)")

EXPECTED_HEADERS = [
    "timestamp_utc", "date", "year", "month", "description",
    "amount", "category", "payment_method", "notes"
]

# =========================================
# SECRETS HELPERS
# =========================================
REQ_WEB_KEYS = [
    "client_id", "client_secret", "auth_uri", "token_uri", "auth_provider_x509_cert_url",
]

def _copy_mapping_like(obj) -> dict:
    if isinstance(obj, Mapping):
        return {str(k): obj[k] for k in obj.keys()}
    try:
        return json.loads(json.dumps(obj))
    except Exception:
        return {}

def _load_web_from_secrets() -> dict:
    if "oauth_client" not in st.secrets:
        st.error("Missing [oauth_client] in .streamlit/secrets.toml")
        st.stop()
    oc = st.secrets["oauth_client"]
    if isinstance(oc, Mapping) and "web" in oc:
        web = _copy_mapping_like(oc["web"])
    else:
        web = {k.split("web.", 1)[1]: v for k, v in oc.items()
               if isinstance(k, str) and k.startswith("web.")}
    missing = [k for k in REQ_WEB_KEYS if k not in web]
    if missing:
        st.error("[oauth_client.web] missing keys: " + ", ".join(missing))
        st.stop()
    for k in REQ_WEB_KEYS:
        v = web[k]
        if not isinstance(v, str) or not v.strip():
            st.error(f"oauth_client.web.{k} must be a non-empty string.")
            st.stop()
        web[k] = v.strip()
    web["redirect_uris"] = [REDIRECT]
    return web

def _get_app_cfg() -> dict:
    if "app" not in st.secrets:
        st.error("Missing [app] in .streamlit/secrets.toml")
        st.stop()
    app_cfg = _copy_mapping_like(st.secrets["app"])
    if not app_cfg.get("spreadsheet_id") and not app_cfg.get("spreadsheet_name"):
        st.error("Provide [app].spreadsheet_id OR [app].spreadsheet_name in secrets.toml")
        st.stop()
    app_cfg.setdefault("worksheet_name", "Expenses")
    return app_cfg

# =========================================
# OAUTH (InstalledAppFlow on loopback)
# =========================================
def run_user_oauth() -> None:
    if st.session_state.get("_auth_in_progress"):
        st.warning("Authentication already in progress. If stuck, reload the page.")
        return
    st.session_state["_auth_in_progress"] = True
    try:
        web = _load_web_from_secrets()
        client_config = {
            "web": {
                "client_id": web["client_id"],
                "client_secret": web["client_secret"],
                "auth_uri": web["auth_uri"],
                "token_uri": web["token_uri"],
                "auth_provider_x509_cert_url": web["auth_provider_x509_cert_url"],
                "redirect_uris": web["redirect_uris"],
            }
        }
        flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
        try:
            creds = flow.run_local_server(
                host=HOST, port=PORT,
                authorization_prompt_message="Opening browser for Google login...",
                success_message="Authentication complete. You can close this tab.",
                open_browser=True,
            )
        except MismatchingStateError:
            st.error(
                "Login failed due to a state mismatch.\n"
                f"- Ensure ONLY {REDIRECT} is authorized in Google Cloud.\n"
                "- Close previous consent tabs and try again (one click only).\n"
                "- Use a private/incognito window.\n"
                "- Make sure only one Streamlit tab is open."
            )
            return
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
        st.session_state.creds = creds
        st.session_state.user = get_userinfo(creds)
    finally:
        st.session_state["_auth_in_progress"] = False

def ensure_fresh_credentials(creds: Credentials) -> Credentials:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return creds

def get_userinfo(creds: Credentials) -> dict:
    r = requests.get(
        "https://www.googleapis.com/oauth2/v3/userinfo",
        headers={"Authorization": f"Bearer {creds.token}"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()

def logout():
    for k in ("creds", "user", "_auth_in_progress"):
        st.session_state.pop(k, None)
    st.success("Logged out. Refresh the page if needed.")

# =========================================
# AUTH UI
# =========================================
auth_col1, _ = st.columns([1, 3])
with auth_col1:
    if "creds" not in st.session_state:
        if st.button("Sign in with Google", use_container_width=True):
            run_user_oauth()
            st.rerun()
    else:
        u = st.session_state.get("user", {})
        st.success(f"Signed in as {u.get('name') or u.get('email')}")
        if st.button("Log out", use_container_width=True):
            logout()
            st.rerun()

if "creds" not in st.session_state:
    st.info("Please sign in with Google to continue.")
    st.stop()

# =========================================
# SHEET HELPERS
# =========================================
def ensure_headers(ws):
    try:
        first_row = ws.row_values(1)
    except Exception:
        first_row = []
    if first_row == EXPECTED_HEADERS:
        return
    if not first_row:
        ws.update("A1:I1", [EXPECTED_HEADERS])
        return
    ws.insert_row(EXPECTED_HEADERS, index=1)

# =========================================
# DATA LAYER
# =========================================
def get_gspread_client() -> gspread.Client:
    creds: Credentials = st.session_state.get("creds")
    if not creds:
        st.stop()
    creds = ensure_fresh_credentials(creds)
    st.session_state.creds = creds
    return gspread.authorize(creds)

@st.cache_resource
def get_worksheet():
    app_cfg = _get_app_cfg()
    ws_name = app_cfg.get("worksheet_name", "Expenses")
    client = get_gspread_client()
    if "spreadsheet_id" in app_cfg and app_cfg["spreadsheet_id"]:
        sh = client.open_by_key(app_cfg["spreadsheet_id"])
    else:
        sh = client.open(app_cfg["spreadsheet_name"])
    try:
        ws = sh.worksheet(ws_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=ws_name, rows=1000, cols=20)
        ws.update("A1:I1", [EXPECTED_HEADERS])
    ensure_headers(ws)
    return ws

@st.cache_data(ttl=15)
def load_data() -> pd.DataFrame:
    ws = get_worksheet()
    values = ws.get_all_values()
    if not values or len(values) == 1:
        return pd.DataFrame(columns=EXPECTED_HEADERS)
    df = pd.DataFrame(values[1:], columns=values[0])
    for col in EXPECTED_HEADERS:
        if col not in df.columns:
            df[col] = pd.NA
    if "date" in df:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    if "amount" in df:
        df["amount"] = pd.to_numeric(df["amount"].astype(str).str.replace(",", "."), errors="coerce")
    for c in ("year", "month"):
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df[EXPECTED_HEADERS]

def append_row(payload: dict):
    ws = get_worksheet()
    ensure_headers(ws)
    ws.append_row([
        payload.get("timestamp_utc"),
        payload.get("date_str"),
        payload.get("year"),
        payload.get("month"),
        payload.get("description"),
        payload.get("amount"),
        payload.get("category", ""),
        payload.get("payment_method"),
        payload.get("notes", ""),
    ], value_input_option="USER_ENTERED")

# =========================================
# UI
# =========================================
def month_name(m: int) -> str:
    return date(2000, m, 1).strftime("%B")

def month_year_selector(df: pd.DataFrame):
    if df.empty or "year" not in df.columns or df["year"].dropna().empty:
        years = [date.today().year]
    else:
        years = sorted([int(y) for y in df["year"].dropna().unique().tolist()], reverse=True)
    sel_year = st.selectbox("Year", years, index=0, key="filter_year")

    if df.empty or "month" not in df.columns:
        months_sorted = list(range(1, 13))
    else:
        months_present = df.loc[df["year"].eq(sel_year), "month"].dropna().unique().tolist()
        months_sorted = sorted([int(m) for m in months_present]) if months_present else list(range(1, 13))

    month_labels = [f"{m:02d} - {month_name(m)}" for m in months_sorted]
    sel_month_label = st.selectbox(
        "Month", month_labels, index=min(len(month_labels)-1, date.today().month-1), key="filter_month"
    )
    sel_month = int(sel_month_label.split(" - ")[0])
    return sel_year, sel_month

def add_expense_form():
    st.subheader("Add Expense")
    with st.form("add_expense_date_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            exp_date = st.date_input("Date", value=date.today(), format="YYYY-MM-DD")
            amount = st.number_input("Amount", min_value=0.0, step=0.01, format="%.2f")
            description = st.text_input("Description")
        with c2:
            pay_method = st.text_input("Payment Method", value="Cash")
        submitted = st.form_submit_button("Add expense")
        if submitted:
            if amount and description and exp_date:
                payload = {
                    "timestamp_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    "date_str": exp_date.strftime("%Y-%m-%d"),
                    "year": exp_date.year,
                    "month": exp_date.month,
                    "description": description.strip(),
                    "amount": amount,
                    "category": "",
                    "payment_method": (pay_method or "Cash").strip() or "Cash",
                    "notes": "",
                }
                append_row(payload)
                st.success("Expense added.")
                st.cache_data.clear()
            else:
                st.warning("Please provide Date, Amount, and Description.")

def month_view(df: pd.DataFrame):
    st.subheader("Browse by Month")
    fy, fm = month_year_selector(df)
    filt = (df["year"].astype("Int64") == fy) & (df["month"].astype("Int64") == fm)
    month_df = df.loc[filt].copy()

    left, mid, right = st.columns(3)
    total = float(month_df["amount"].sum()) if not month_df.empty else 0.0
    count = int(month_df.shape[0])
    avg = float(month_df["amount"].mean()) if count else 0.0
    with left: st.metric("Total", f"{total:,.2f}")
    with mid: st.metric("# Expenses", f"{count}")
    with right: st.metric("Average / expense", f"{avg:,.2f}")

    if not month_df.empty:
        display_cols = ["date", "description", "amount", "payment_method"]
        st.markdown("Expenses")
        st.dataframe(
            month_df[display_cols].sort_values(by=["date", "description"]),
            use_container_width=True, hide_index=True
        )
    else:
        st.info("No expenses found for the selected month.")

# =========================================
# MAIN
# =========================================
if "creds" in st.session_state:
    st.caption(f"Signed in as: {st.session_state.get('user', {}).get('email')}")

df = load_data() if "creds" in st.session_state else pd.DataFrame(columns=EXPECTED_HEADERS)
if "creds" in st.session_state:
    add_expense_form()
    st.divider()
    month_view(df)
