import sqlite3
from pathlib import Path
from datetime import datetime
import json

# DB en la raíz del proyecto (crm_followups/data.sqlite)
APP_DIR = Path(__file__).resolve().parent.parent
DB_PATH = APP_DIR / "data.sqlite"


# -----------------------------
# Conexión
# -----------------------------
def get_conn():
    # check_same_thread False ayuda si luego metés threads (uvicorn / fastapi)
    return sqlite3.connect(DB_PATH, check_same_thread=False)


# -----------------------------
# Helpers migración (ALTER TABLE)
# -----------------------------
def _add_column_if_missing(conn, table: str, column: str, coldef: str):
    # SQLite: no hay IF NOT EXISTS en ALTER TABLE ADD COLUMN
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coldef}")
    except sqlite3.OperationalError:
        # ya existe o no se puede alterar en ese estado
        pass


def _iso_range(date_from: str | None, date_to: str | None):
    """
    Convierte fechas YYYY-MM-DD a rangos ISO para comparar con created_at ISO.
    """
    df = (date_from or "").strip()
    dt = (date_to or "").strip()

    df_iso = f"{df}T00:00:00" if df else None
    dt_iso = f"{dt}T23:59:59" if dt else None
    return df_iso, dt_iso


# -----------------------------
# Init DB + migraciones suaves
# -----------------------------
def init_db():
    with get_conn() as conn:
        # --- QUOTES ---
        conn.execute("""
        CREATE TABLE IF NOT EXISTS quotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vendor_id TEXT NOT NULL,
            quote_number TEXT NOT NULL,
            pdf_sha256 TEXT NOT NULL,
            extracted_json TEXT,
            events_json TEXT,
            created_at TEXT NOT NULL,

            -- columnas UI / seguimiento
            summary TEXT,
            notes TEXT,
            status TEXT DEFAULT 'pendiente',
            updated_at TEXT,

            UNIQUE(vendor_id, quote_number),
            UNIQUE(vendor_id, pdf_sha256)
        )
        """)

        # Migración suave por si la tabla se creó antes sin estas columnas
        _add_column_if_missing(conn, "quotes", "summary", "TEXT")
        _add_column_if_missing(conn, "quotes", "notes", "TEXT")
        _add_column_if_missing(conn, "quotes", "status", "TEXT")
        _add_column_if_missing(conn, "quotes", "updated_at", "TEXT")

        # --- POSTVENTAS ---
        conn.execute("""
        CREATE TABLE IF NOT EXISTS postventas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vendor_id TEXT NOT NULL,
            client_name TEXT NOT NULL,
            phone TEXT,
            sale_date TEXT,
            postventa_date TEXT NOT NULL,
            type TEXT,
            notes TEXT,
            status TEXT DEFAULT 'pendiente',
            event_id TEXT,
            htmlLink TEXT,
            created_at TEXT NOT NULL,

            -- vínculo opcional a cotización
            quote_number TEXT
        )
        """)

        # Migración suave: si la tabla ya existía sin quote_number
        _add_column_if_missing(conn, "postventas", "quote_number", "TEXT")

        conn.commit()


# -----------------------------
# QUOTES (cotizaciones)
# -----------------------------
def find_existing(vendor_id: str, quote_number: str, pdf_sha256: str):
    """
    Devuelve el registro si ya existe por quote_number o por pdf hash.
    """
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT quote_number, pdf_sha256, extracted_json, events_json, created_at
            FROM quotes
            WHERE vendor_id = ?
              AND (quote_number = ? OR pdf_sha256 = ?)
            ORDER BY id DESC
            LIMIT 1
            """,
            (vendor_id, quote_number, pdf_sha256)
        )
        row = cur.fetchone()
        if not row:
            return None

        return {
            "quote_number": row[0],
            "pdf_sha256": row[1],
            "extracted": json.loads(row[2]) if row[2] else None,
            "events_created": json.loads(row[3]) if row[3] else None,
            "created_at": row[4],
        }


def insert_quote(vendor_id: str, quote_number: str, pdf_sha256: str, extracted: dict, events_created: list):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO quotes (vendor_id, quote_number, pdf_sha256, extracted_json, events_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                vendor_id,
                quote_number,
                pdf_sha256,
                json.dumps(extracted, ensure_ascii=False),
                json.dumps(events_created, ensure_ascii=False),
                datetime.now().isoformat()
            )
        )
        conn.commit()


def update_notes(vendor_id: str, quote_number: str, summary: str, notes: str, status: str):
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE quotes
            SET summary = ?,
                notes = ?,
                status = ?,
                updated_at = ?
            WHERE vendor_id = ? AND quote_number = ?
            """,
            (summary, notes, status, datetime.now().isoformat(), vendor_id, quote_number)
        )
        conn.commit()


def list_quotes(vendor_id: str):
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT quote_number, created_at, COALESCE(status,'pendiente') as status, summary
            FROM quotes
            WHERE vendor_id = ?
            ORDER BY created_at DESC
            LIMIT 300
            """,
            (vendor_id,)
        )
        rows = cur.fetchall()

    return [
        {"quote_number": r[0], "created_at": r[1], "status": r[2], "summary": r[3]}
        for r in rows
    ]


def get_quote_detail(vendor_id: str, quote_number: str):
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT
              quote_number,
              extracted_json,
              events_json,
              summary,
              notes,
              COALESCE(status,'pendiente') as status,
              created_at,
              updated_at
            FROM quotes
            WHERE vendor_id = ?
              AND quote_number = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (vendor_id, quote_number)
        )
        row = cur.fetchone()
        if not row:
            return None

        return {
            "quote_number": row[0],
            "extracted": json.loads(row[1]) if row[1] else None,
            "events_created": json.loads(row[2]) if row[2] else [],
            "summary": row[3] or "",
            "notes": row[4] or "",
            "status": row[5] or "pendiente",
            "created_at": row[6],
            "updated_at": row[7],
        }


def get_event_ids(vendor_id: str, quote_number: str) -> list[str]:
    """
    Devuelve los event_id del ÚLTIMO registro para esa cotización.
    """
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT events_json
            FROM quotes
            WHERE vendor_id = ? AND quote_number = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (vendor_id, quote_number)
        )
        row = cur.fetchone()
        if not row or not row[0]:
            return []

        try:
            events = json.loads(row[0])
        except Exception:
            return []

        ids = []
        for e in events:
            if isinstance(e, dict):
                eid = e.get("event_id") or e.get("id")
                if eid:
                    ids.append(eid)
        return ids


def clear_events(vendor_id: str, quote_number: str):
    """
    Limpia events_json para esa cotización.
    """
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE quotes
            SET events_json = ?
            WHERE vendor_id = ? AND quote_number = ?
            """,
            ("[]", vendor_id, quote_number)
        )
        conn.commit()


# -----------------------------
# POSTVENTAS
# -----------------------------
def insert_postventa(
    vendor_id: str,
    client_name: str,
    phone: str | None,
    sale_date: str | None,
    postventa_date: str,
    type_: str | None,
    notes: str | None,
    event: dict | None,
    quote_number: str | None = None,
) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO postventas (
                vendor_id, client_name, phone, sale_date, postventa_date, type, notes,
                status, event_id, htmlLink, created_at, quote_number
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                vendor_id,
                client_name,
                phone,
                sale_date,
                postventa_date,
                type_ or "",
                notes or "",
                "pendiente",
                (event or {}).get("event_id"),
                (event or {}).get("htmlLink"),
                datetime.now().isoformat(),
                quote_number,
            )
        )
        conn.commit()
        return int(cur.lastrowid)


def list_postventas(vendor_id: str):
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT id, client_name, phone, postventa_date, type, COALESCE(status,'pendiente') as status,
                   htmlLink, created_at
            FROM postventas
            WHERE vendor_id = ?
            ORDER BY id DESC
            LIMIT 200
            """,
            (vendor_id,)
        )
        rows = cur.fetchall()

    items = []
    for r in rows:
        items.append({
            "id": r[0],
            "client_name": r[1],
            "phone": r[2],
            "postventa_date": r[3],
            "type": r[4],
            "status": r[5],
            "htmlLink": r[6],
            "created_at": r[7],
        })
    return items


def get_postventa_detail(vendor_id: str, postventa_id: int):
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT id, client_name, phone, sale_date, postventa_date, type, notes,
                   COALESCE(status,'pendiente') as status,
                   event_id, htmlLink, created_at, quote_number
            FROM postventas
            WHERE vendor_id = ? AND id = ?
            """,
            (vendor_id, postventa_id)
        )
        r = cur.fetchone()

    if not r:
        return None

    return {
        "id": r[0],
        "client_name": r[1],
        "phone": r[2],
        "sale_date": r[3],
        "postventa_date": r[4],
        "type": r[5],
        "notes": r[6],
        "status": r[7],
        "event_id": r[8],
        "htmlLink": r[9],
        "created_at": r[10],
        "quote_number": r[11],
    }


def update_postventa_status(vendor_id: str, postventa_id: int, status: str):
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE postventas
            SET status = ?
            WHERE vendor_id = ? AND id = ?
            """,
            (status, vendor_id, postventa_id)
        )
        conn.commit()


def clear_postventa_event(vendor_id: str, postventa_id: int):
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE postventas
            SET event_id = NULL, htmlLink = NULL
            WHERE vendor_id = ? AND id = ?
            """,
            (vendor_id, postventa_id)
        )
        conn.commit()


# -----------------------------
# ADMIN (encargado)
# -----------------------------
def list_vendors():
    with get_conn() as conn:
        cur = conn.execute("""
            SELECT DISTINCT vendor_id FROM quotes
            UNION
            SELECT DISTINCT vendor_id FROM postventas
            ORDER BY vendor_id
        """)
        rows = cur.fetchall()
    return [r[0] for r in rows if r and r[0]]


def list_quotes_admin(
    vendor_id: str | None = None,
    status: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 500,
):
    df_iso, dt_iso = _iso_range(date_from, date_to)

    sql = """
        SELECT
            quote_number,
            vendor_id,
            created_at,
            extracted_json,
            events_json,
            summary,
            COALESCE(status,'pendiente') as status,
            updated_at
        FROM quotes
        WHERE 1=1
    """
    params = []

    if vendor_id:
        sql += " AND vendor_id = ?"
        params.append(vendor_id)

    if status:
        sql += " AND COALESCE(status,'pendiente') = ?"
        params.append(status)

    if df_iso:
        sql += " AND created_at >= ?"
        params.append(df_iso)

    if dt_iso:
        sql += " AND created_at <= ?"
        params.append(dt_iso)

    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)

    with get_conn() as conn:
        cur = conn.execute(sql, params)
        rows = cur.fetchall()

    out = []
    for r in rows:
        extracted = json.loads(r[3]) if r[3] else None
        events = json.loads(r[4]) if r[4] else None
        out.append({
            "quote_number": r[0],
            "vendor_id": r[1],
            "created_at": r[2],
            "extracted": extracted,
            "events_created": events,
            "summary": r[5] or "",
            "status": r[6] or "pendiente",
            "updated_at": r[7],
        })
    return out


def list_postventas_admin(
    vendor_id: str | None = None,
    status: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 500,
):
    df_iso, dt_iso = _iso_range(date_from, date_to)

    sql = """
        SELECT
            id,
            vendor_id,
            client_name,
            postventa_date,
            COALESCE(status,'pendiente') as status,
            type,
            quote_number,
            created_at,
            htmlLink
        FROM postventas
        WHERE 1=1
    """
    params = []

    if vendor_id:
        sql += " AND vendor_id = ?"
        params.append(vendor_id)

    if status:
        sql += " AND COALESCE(status,'pendiente') = ?"
        params.append(status)

    if df_iso:
        sql += " AND created_at >= ?"
        params.append(df_iso)

    if dt_iso:
        sql += " AND created_at <= ?"
        params.append(dt_iso)

    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)

    with get_conn() as conn:
        cur = conn.execute(sql, params)
        rows = cur.fetchall()

    out = []
    for r in rows:
        out.append({
            "id": r[0],
            "vendor_id": r[1],
            "client_name": r[2],
            "postventa_date": r[3],
            "status": r[4] or "pendiente",
            "type": r[5] or "",
            "quote_number": r[6] or "",
            "created_at": r[7],
            "htmlLink": r[8] or "",
        })
    return out

def list_admin_items(
    vendor_id: str | None = None,
    status: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    kind: str | None = None,   # "quote" | "postventa" | None
    limit: int = 500,
):
    items = []

    if kind in (None, "", "all", "quote"):
        qs = list_quotes_admin(vendor_id=vendor_id, status=status, date_from=date_from, date_to=date_to, limit=limit)
        for q in qs:
            ex = q.get("extracted") or {}
            items.append({
                "kind": "cotizacion",
                "date": q.get("created_at"),
                "vendor_id": q.get("vendor_id"),
                "client_name": ex.get("client_name") or "",
                "ref": q.get("quote_number"),
                "status": q.get("status") or "pendiente",
                "total": ex.get("total") or "",
                "summary": q.get("summary") or "",
            })

    if kind in (None, "", "all", "postventa"):
        ps = list_postventas_admin(vendor_id=vendor_id, status=status, date_from=date_from, date_to=date_to, limit=limit)
        for p in ps:
            items.append({
                "kind": "postventa",
                "date": p.get("created_at"),
                "vendor_id": p.get("vendor_id"),
                "client_name": p.get("client_name") or "",
                "ref": f"PV-{p.get('id')}",
                "status": p.get("status") or "pendiente",
                "total": "",
                "summary": p.get("type") or "",
            })

    # ordenar por fecha desc (string ISO, ordena bien)
    items.sort(key=lambda x: x.get("date") or "", reverse=True)

    return items[:limit]

