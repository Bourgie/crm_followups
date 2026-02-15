# app/google_oauth.py
import os
import json
import re
from pathlib import Path
from datetime import datetime, timezone

from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials

# OJO: usás /userinfo para obtener el email => necesitás estos scopes.
# (Evita el warning de "Scope has changed" y problemas raros de refresh.)
SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/calendar.events",
]

APP_DIR = Path(__file__).resolve().parent.parent
CREDS_DIR = APP_DIR / "credentials"
TOKENS_DIR = CREDS_DIR / "tokens"
CLIENT_SECRET_PATH = CREDS_DIR / "client_secret.json"


# -----------------------------
# Client secret (local file o env en Render)
# -----------------------------
def _ensure_client_secret_file() -> None:
    """
    En local podés tener credentials/client_secret.json.
    En Render NO lo subimos al repo: lo armamos desde GOOGLE_CLIENT_SECRET_JSON.
    """
    if CLIENT_SECRET_PATH.exists():
        return

    raw = (os.getenv("GOOGLE_CLIENT_SECRET_JSON") or "").strip()
    if not raw:
        raise RuntimeError(
            "Falta GOOGLE_CLIENT_SECRET_JSON (env) y no existe credentials/client_secret.json"
        )

    CREDS_DIR.mkdir(parents=True, exist_ok=True)

    # Acepta JSON puro o JSON "escapado"
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = json.loads(raw.encode("utf-8").decode("unicode_escape"))

    CLIENT_SECRET_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _base_url() -> str:
    # ej: http://localhost:8000 o https://crm-followups.onrender.com
    return os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/")


# -----------------------------
# Expiry helpers (FIX: naive vs aware)
# -----------------------------
def _serialize_expiry(dt: datetime | None) -> str | None:
    """Guardamos expiry como ISO UTC con Z (string)."""
    if not dt:
        return None
    if dt.tzinfo is None:
        # si vino naive, asumimos UTC
        dt_utc = dt.replace(tzinfo=timezone.utc)
    else:
        dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.isoformat().replace("+00:00", "Z")


def _parse_expiry(value):
    """
    Devuelve expiry como datetime NAIVE en UTC (sin tzinfo).
    Evita error: naive vs aware en google-auth.
    """
    if not value:
        return None

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        v = value.strip()
        try:
            if v.endswith("Z"):
                dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
            else:
                dt = datetime.fromisoformat(v)
        except Exception:
            return None
    else:
        return None

    # Normalizamos a UTC y lo dejamos NAIVE
    if dt.tzinfo is None:
        # si vino naive, asumimos UTC
        dt_utc_naive = dt
    else:
        dt_utc_naive = dt.astimezone(timezone.utc).replace(tzinfo=None)

    return dt_utc_naive


# -----------------------------
# Token paths (vendor + email)
# -----------------------------
def _safe_filename(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9._-]+", "_", s)
    return s


def _token_path(vendor_id: str) -> Path:
    return TOKENS_DIR / f"{_safe_filename(vendor_id)}.json"


def token_path_for_email(email: str) -> Path:
    return TOKENS_DIR / f"{_safe_filename(email)}.json"


# -----------------------------
# OAuth URLs / exchange
# -----------------------------
def get_auth_url(vendor_id: str) -> str:
    """
    Devuelve URL de consentimiento.
    state = vendor_id (para tokens por vendedor o login).
    """
    _ensure_client_secret_file()
    redirect_uri = f"{_base_url()}/auth/callback"

    flow = Flow.from_client_secrets_file(
        str(CLIENT_SECRET_PATH),
        scopes=SCOPES,
        redirect_uri=redirect_uri,
    )

    auth_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
        state=vendor_id,
    )
    return auth_url


def exchange_code_for_creds(code: str, vendor_id: str) -> Credentials:
    """
    Intercambia el code por credenciales y guarda token por vendor_id.
    """
    _ensure_client_secret_file()
    redirect_uri = f"{_base_url()}/auth/callback"

    flow = Flow.from_client_secrets_file(
        str(CLIENT_SECRET_PATH),
        scopes=SCOPES,
        redirect_uri=redirect_uri,
    )
    flow.fetch_token(code=code)

    creds = flow.credentials
    save_creds_for_vendor(vendor_id, creds)
    return creds


# -----------------------------
# Save / Load creds (vendor)
# -----------------------------
def save_creds_for_vendor(vendor_id: str, creds: Credentials) -> None:
    TOKENS_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": creds.scopes or SCOPES,
        "expiry": _serialize_expiry(getattr(creds, "expiry", None)),
    }
    _token_path(vendor_id).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_creds_for_vendor(vendor_id: str) -> Credentials | None:
    path = _token_path(vendor_id)
    if not path.exists():
        return None

    data = json.loads(path.read_text(encoding="utf-8"))
    if "expiry" in data:
        data["expiry"] = _parse_expiry(data.get("expiry"))

    # from_authorized_user_info es más tolerante
    try:
        return Credentials.from_authorized_user_info(data, scopes=SCOPES)
    except Exception:
        return Credentials(**data)


# -----------------------------
# Save / Load creds (email) - usado por main.py
# -----------------------------
def save_creds_for_email(email: str, creds: Credentials) -> None:
    TOKENS_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": creds.scopes or SCOPES,
        "expiry": _serialize_expiry(getattr(creds, "expiry", None)),
    }
    token_path_for_email(email).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_creds_for_email(email: str) -> Credentials | None:
    path = token_path_for_email(email)
    if not path.exists():
        return None

    data = json.loads(path.read_text(encoding="utf-8"))
    if "expiry" in data:
        data["expiry"] = _parse_expiry(data.get("expiry"))

    try:
        return Credentials.from_authorized_user_info(data, scopes=SCOPES)
    except Exception:
        return Credentials(**data)
