import os
import json
from pathlib import Path
from datetime import datetime, timezone

from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials

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


def _ensure_client_secret_file() -> None:
    """
    En deploy (Render), no subimos client_secret.json al repo.
    Lo creamos desde la env GOOGLE_CLIENT_SECRET_JSON.
    """
    if CLIENT_SECRET_PATH.exists():
        return

    raw = os.getenv("GOOGLE_CLIENT_SECRET_JSON", "").strip()
    if not raw:
        raise RuntimeError(
            "Falta GOOGLE_CLIENT_SECRET_JSON (env) y no existe credentials/client_secret.json"
        )

    CREDS_DIR.mkdir(parents=True, exist_ok=True)

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # por si lo pegaste escapado
        data = json.loads(raw.encode("utf-8").decode("unicode_escape"))

    CLIENT_SECRET_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _base_url() -> str:
    return os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/")


def _safe_key(s: str) -> str:
    """
    Para nombre de archivo (email/vendor_id), evita caracteres raros.
    """
    out = []
    for ch in (s or "").strip():
        if ch.isalnum():
            out.append(ch)
        else:
            out.append("_")
    return "".join(out) or "unknown"


# -----------------------------
# Token paths (por email / vendor)
# -----------------------------
def token_path_for_email(email: str) -> Path:
    return TOKENS_DIR / f"{_safe_key(email)}.json"


def token_path_for_vendor(vendor_id: str) -> Path:
    return TOKENS_DIR / f"{_safe_key(vendor_id)}.json"


# -----------------------------
# OAuth flow
# -----------------------------
def get_auth_url(vendor_id: str) -> str:
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
    _ensure_client_secret_file()
    redirect_uri = f"{_base_url()}/auth/callback"

    flow = Flow.from_client_secrets_file(
        str(CLIENT_SECRET_PATH),
        scopes=SCOPES,
        redirect_uri=redirect_uri,
    )
    flow.fetch_token(code=code)

    creds = flow.credentials

    # Guardamos por vendor_id (si lo estás usando) y listo
    save_creds_for_vendor(vendor_id, creds)
    return creds


# -----------------------------
# Save / Load
# -----------------------------
def _parse_expiry(value):
    """
    Convierte expiry string -> datetime aware.
    Acepta ISO con Z o con offset.
    """
    if not value or not isinstance(value, str):
        return value
    v = value.strip()
    try:
        # Ej: 2026-02-14T12:34:56Z
        if v.endswith("Z"):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        # Ej: 2026-02-14T12:34:56+00:00
        dt = datetime.fromisoformat(v)
        # si vino naive, lo asumimos UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def save_creds_for_vendor(vendor_id: str, creds: Credentials) -> None:
    TOKENS_DIR.mkdir(parents=True, exist_ok=True)
    token_path_for_vendor(vendor_id).write_text(creds.to_json(), encoding="utf-8")


def load_creds_for_vendor(vendor_id: str) -> Credentials | None:
    path = token_path_for_vendor(vendor_id)
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))

    # ✅ FIX: expiry viene como str en muchos casos
    if isinstance(data.get("expiry"), str):
        data["expiry"] = _parse_expiry(data["expiry"])

    # Constructor correcto que tolera campos extra
    try:
        return Credentials.from_authorized_user_info(data, scopes=SCOPES)
    except Exception:
        # fallback
        return Credentials(**data)


def save_creds_for_email(email: str, creds: Credentials) -> None:
    TOKENS_DIR.mkdir(parents=True, exist_ok=True)
    token_path_for_email(email).write_text(creds.to_json(), encoding="utf-8")


def load_creds_for_email(email: str) -> Credentials | None:
    path = token_path_for_email(email)
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))

    # ✅ FIX: expiry str -> datetime
    if isinstance(data.get("expiry"), str):
        data["expiry"] = _parse_expiry(data["expiry"])

    try:
        return Credentials.from_authorized_user_info(data, scopes=SCOPES)
    except Exception:
        return Credentials(**data)
