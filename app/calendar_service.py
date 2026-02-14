from datetime import datetime, timedelta
from googleapiclient.discovery import build

def create_followup_events(creds, quote_data: dict) -> list[dict]:
    """
    Crea 2 eventos (48h y 72h) en el calendar 'primary'.
    Devuelve una lista con ids/links básicos.
    """
    service = build("calendar", "v3", credentials=creds)

    now = datetime.now().astimezone()
    results = []

    for hours, tag in [(48, "48h"), (72, "72h")]:
        start = now + timedelta(hours=hours)
        end = start + timedelta(minutes=10)

        summary = f"Seguimiento {tag} - Cotización {quote_data.get('quote_number','S/N')} - {quote_data.get('client_name','Cliente')}"
        description = (
            f"Cliente: {quote_data.get('client_name','')}\n"
            f"Vendedor: {quote_data.get('seller','')}\n"
            f"Fecha emisión: {quote_data.get('issue_date','')}\n"
            f"Total: {quote_data.get('total','')}\n\n"
            "Acción: contactar y avanzar cierre."
        )

        event = {
            "summary": summary,
            "description": description,
            "start": {"dateTime": start.isoformat()},
            "end": {"dateTime": end.isoformat()},
            "reminders": {
                "useDefault": False,
                "overrides": [{"method": "popup", "minutes": 0}],
            },
        }

        created = service.events().insert(calendarId="primary", body=event).execute()
        results.append({
            "tag": tag,
            "event_id": created.get("id"),
            "htmlLink": created.get("htmlLink"),
            "start": created.get("start"),
        })

    return results

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def delete_events(creds, event_ids: list[str], calendar_id: str = "primary") -> dict:
    service = build("calendar", "v3", credentials=creds)

    deleted = []
    failed = []

    for eid in event_ids:
        try:
            service.events().delete(calendarId=calendar_id, eventId=eid).execute()
            deleted.append(eid)
        except HttpError as e:
            # status real en googleapiclient:
            status = getattr(getattr(e, "resp", None), "status", None)

            # 404 = ya no existe -> lo tomamos como "ok"
            if status == 404:
                deleted.append(eid)
            else:
                failed.append({"event_id": eid, "status": status, "error": str(e)})
        except Exception as e:
            failed.append({"event_id": eid, "status": None, "error": str(e)})

    return {"deleted": deleted, "failed": failed}


def create_postventa_event(creds, data: dict, calendar_id: str = "primary") -> dict:
    """
    Crea un evento único de postventa en Google Calendar.
    data: client_name, phone, sale_date, postventa_date, type, notes
    """
    service = build("calendar", "v3", credentials=creds)

    title = f"Postventa: {data.get('client_name','Cliente')}"
    type_ = data.get("type") or "postventa"
    phone = data.get("phone") or ""
    sale_date = data.get("sale_date") or ""
    notes = data.get("notes") or ""

    desc_lines = [
        f"Tipo: {type_}",
        f"Cliente: {data.get('client_name','')}",
        f"Tel: {phone}",
        f"Fecha venta/instalación: {sale_date}",
        "",
        "Notas:",
        notes,
    ]
    description = "\n".join(desc_lines).strip()

    # postventa_date tiene formato YYYY-MM-DD
    day = data["postventa_date"]

    body = {
        "summary": title,
        "description": description,
        "start": {"date": day},
        "end": {"date": day},
        "reminders": {
            "useDefault": False,
            "overrides": [
                {"method": "popup", "minutes": 9 * 60},  # 9hs
            ],
        },
    }

    created = service.events().insert(calendarId=calendar_id, body=body).execute()

    return {
        "event_id": created.get("id"),
        "htmlLink": created.get("htmlLink"),
        "calendar_id": calendar_id,
    }
