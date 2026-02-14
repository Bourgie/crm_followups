# CRM Followups (MVP)

Este proyecto:
1) recibe un PDF de cotización
2) extrae datos (nro, fecha, vendedor, cliente, total)
3) crea 2 eventos en Google Calendar (48h y 72h) en el calendario personal del vendedor

## Requisitos
- Python 3.11+ instalado
- Un proyecto en Google Cloud con Calendar API habilitada
- Un OAuth Client "Web application"
- Descargar el archivo `client_secret.json` y guardarlo en `credentials/client_secret.json`

## Instalación
```bash
cd crm_followups
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Mac/Linux:
source .venv/bin/activate

pip install -r requirements.txt
```

## Variables .env
Crea un archivo `.env` en la raíz con:
```
BASE_URL=http://localhost:8000
```

## Ejecutar
```bash
uvicorn app.main:app --reload --port 8000
```

Luego:
1) Abrí `http://localhost:8000/connect?vendor_id=1`
2) Aceptá permisos de Google Calendar
3) Subí un PDF en `http://localhost:8000/docs` (endpoint `/upload`)

