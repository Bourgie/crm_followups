import os
import json
from pathlib import Path
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials

SCOPES = ["https://www.googleapis.com/auth/calendar.events"]

APP_DIR = Path(__file__).resolve().parent.parent
CREDS_DIR = APP_DIR / "credentials"
TOKENS_DIR = CREDS_DIR / "tokens"


def _base_url() -> str:
    # ej: http://localhost:8000  |  https://tu-app.onrender.com
    return os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/")


def _get_client_config() -> dict:
    """
    En deploy (Render) NO hay credentials/client_secret.json.
    Leemos el JSON desde la env GOOGLE_CLIENT_SECRET_JSON.

    Acepta:
    - JSON normal: {"web": {...}} o {"installed": {...}}
    - JSON escapado (copiado como string con \")
    """
    raw = (os.getenv("GOOGLE_CLIENT_SECRET_JSON") or "").strip()
    if not raw:
        raise RuntimeError(
            "Falta GOOGLE_CLIENT_SECRET_JSON en variables de entorno."
        )

    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    except json.JSONDecodeError:
        pass

    # fallback por si lo pegaste escapado tipo "\"{...}\"" o con \n, \"
    try:
        unescaped = raw.encode("utf-8").decode("unicode_escape")
        data = json.loads(unescaped)
        if isinstance(data, dict):
            return data
    except Exception as e:
        raise RuntimeError(
            "GOOGLE_CLIENT_SECRET_JSON no es un JSON válido (ni normal ni escapado)."
        ) from e

    raise RuntimeError("GOOGLE_CLIENT_SECRET_JSON inválido.")


def get_auth_url(vendor_id: str) -> str:
    """
    Devuelve la URL de Google para consentir.
    state = vendor_id (para guardar token por vendedor).
    """
    redirect_uri = f"{_base_url()}/auth/callback"

    flow = Flow.from_client_config(
        _get_client_config(),
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
    Intercambia el 'code' por credenciales y las guarda en disco (tokens por vendedor).
    """
    redirect_uri = f"{_base_url()}/auth/callback"

    flow = Flow.from_client_config(
        _get_client_config(),
        scopes=SCOPES,
        redirect_uri=redirect_uri,
    )

    flow.fetch_token(code=code)

    creds = flow.credentials
    save_creds_for_vendor(vendor_id, creds)
    return creds


def _token_path(vendor_id: str) -> Path:
    # guardamos tokens en disco por vendedor (OK para local;
    # en Render el FS es efímero: sirve para demo, pero no para prod serio)
    safe = vendor_id.replace("@", "_").replace(".", "_")
    return TOKENS_DIR / f"{safe}.json"


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

def token_path_for_email(email: str) -> Path:
    """
    Compatibilidad con main.py (alias).
    Antes se guardaba por email. Ahora usamos vendor_id.
    """
    return _token_path(email)


def load_creds_for_email(email: str) -> Credentials | None:
    """
    Compatibilidad con main.py (alias).
    """
    return load_creds_for_vendor(email)
