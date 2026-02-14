import os
import re
from pathlib import Path

from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials

# ✅ Scopes: Calendar + Login (email/profile)
SCOPES = [
    "https://www.googleapis.com/auth/calendar.events",
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
]

APP_DIR = Path(__file__).resolve().parent.parent
CREDS_DIR = APP_DIR / "credentials"
TOKENS_DIR = CREDS_DIR / "tokens"
CLIENT_SECRET_PATH = CREDS_DIR / "client_secret.json"


def _base_url() -> str:
    # ej: http://localhost:8000
    return os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/")


def sanitize_email(email: str) -> str:
    # file-safe: juan.perez@gmail.com -> juan_perez_gmail_com
    return re.sub(r"[^a-zA-Z0-9]+", "_", email).strip("_").lower()


def token_path_for_email(email: str) -> Path:
    TOKENS_DIR.mkdir(parents=True, exist_ok=True)
    return TOKENS_DIR / f"{sanitize_email(email)}.json"


def get_auth_url(vendor_id: str) -> str:
    """
    Devuelve la URL de Google para consentir.
    state = vendor_id (en modo login usamos 'login' como placeholder)
    """
    redirect_uri = f"{_base_url()}/auth/callback"

    flow = Flow.from_client_secrets_file(
        str(CLIENT_SECRET_PATH),
        scopes=SCOPES,
        redirect_uri=redirect_uri,
    )

    auth_url, _state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
        state=vendor_id,
    )
    return auth_url


def exchange_code_for_creds(code: str, vendor_id: str) -> Credentials:
    """
    Intercambia el 'code' por credenciales.
    Ya NO guardamos por vendor_id acá, porque el id real es el email.
    El guardado por email lo hace main.py (cuando ya conoce el email).
    """
    redirect_uri = f"{_base_url()}/auth/callback"

    flow = Flow.from_client_secrets_file(
        str(CLIENT_SECRET_PATH),
        scopes=SCOPES,
        redirect_uri=redirect_uri,
    )

    flow.fetch_token(code=code)
    return flow.credentials


def load_creds_for_email(email: str) -> Credentials | None:
    """
    Carga credenciales guardadas por email.
    """
    path = token_path_for_email(email)
    if not path.exists():
        return None
    return Credentials.from_authorized_user_file(str(path), SCOPES)
