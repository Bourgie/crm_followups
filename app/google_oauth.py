import os
import json
from pathlib import Path
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials

SCOPES = ["https://www.googleapis.com/auth/calendar.events"]

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
            "Falta GOOGLE_CLIENT_SECRET_JSON (Render env) y no existe credentials/client_secret.json"
        )

    CREDS_DIR.mkdir(parents=True, exist_ok=True)

    # Acepta JSON puro o JSON escapado
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # por si lo pegaste escapado tipo "\"{...}\""
        data = json.loads(raw.encode("utf-8").decode("unicode_escape"))

    CLIENT_SECRET_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _base_url() -> str:
    return os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/")


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
    save_creds_for_vendor(vendor_id, creds)
    return creds


def _token_path(vendor_id: str) -> Path:
    return TOKENS_DIR / f"{vendor_id}.json"


def save_creds_for_vendor(vendor_id: str, creds: Credentials) -> None:
    TOKENS_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": creds.scopes,
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
    return Credentials(**data)

