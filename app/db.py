import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path


# ============================================
# DB path (crm_followups/data.sqlite)
# ============================================
APP_DIR = Path(__file__).resolve().parent.parent
DB_PATH = APP_DIR / "data.sqlite"


# ============================================
# Conexión
# ============================================
def get_conn() -> sqlite3.Connection:
    # check_same_thread=False ayuda si luego metés threads (uvicorn/fastapi)
    return sqlite3.connect(DB_PATH, check_same_thread=False)


# ============================================
# Helpers migración (ALTER TABLE)
# ============================================
def _add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, coldef: str) -> None:
    # SQLite no tiene IF NOT EXISTS en ALTER TABLE ADD COLUMN
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coldef}")
    except sqlite3.OperationalError:
        # ya existe o no se puede alterar en ese estado
        pass


def _iso_range(date_from: str | None, date_to: str | None) -> tuple[str | None, str | None]:
    """
    Convierte fechas YYYY-MM-DD a rangos ISO para comparar con created_at ISO.
    """
    df = (date_from or "").strip()
    dt = (date_to or "").strip()

    df_iso = f"{df}T00:00:00" if df else None
    dt_iso = f"{dt}T23:59:59" if dt else None
    return df_iso, dt_iso


# ============================================
# Init DB + migraciones suaves
# ============================================
def init_db() -> None:
    with get_conn() as conn:
        # --- QUOTES ---
        conn.execute(
            """
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
            """
        )

        # Migración suave por si la tabla se creó antes sin estas columnas
        _add_column_if_missing(conn, "quotes", "summary", "TEXT")
        _add_column_if_missing(conn, "quotes", "notes", "TEXT")
        _add_column_if_missing(conn, "quotes", "status", "TEXT")
        _add_column_if_missing(conn, "quotes", "updated_at", "TEXT")

        # --- POSTVENTAS ---
        conn.execute(
            """
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
            """
        )

        # Migración suave: si la tabla ya existía sin quote_number
        _add_column_if_missing(conn, "postventas", "quote_number", "TEXT")

        conn.commit()


# ============================================
# QUOTES (cotizaciones)
# ============================================
def find_existing(vendor_id: str, quote_number: str, pdf_sha256: str) -> dict | None:
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
            (vendor_id, quote_number, pdf_sha256),
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


def insert_quote(vendor_id: str, quote_number: str, pdf_sha256: str, extracted: dict, events_created: list) -> None:
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
                datetime.now().isoformat(),
            ),
        )
        conn.commit()


def update_notes(vendor_id: str, quote_number: str, summary: str, notes: str, status: str) -> None:
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
            (summary, notes, status, datetime.now().isoformat(), vendor_id, quote_number),
        )
        conn.commit()


def list_quotes(vendor_id: str) -> list[dict]:
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT quote_number, created_at, COALESCE(status,'pendiente') as status, summary
            FROM quotes
            WHERE vendor_id = ?
            ORDER BY created_at DESC
            LIMIT 300
            """,
            (vendor_id,),
        )
        rows = cur.fetchall()

    return [{"quote_number": r[0], "created_at": r[1], "status": r[2], "summary": r[3]} for r in rows]


def get_quote_detail(vendor_id: str, quote_number: str) -> dict | None:
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
            (vendor_id, quote_number),
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
            (vendor_id, quote_number),
        )
        row = cur.fetchone()

    if not row or not row[0]:
        return []

    try:
        events = json.loads(row[0])
    except Exception:
        return []

    ids: list[str] = []
    for e in events:
        if isinstance(e, dict):
            eid = e.get("event_id") or e.get("id")
            if eid:
                ids.append(eid)
    return ids


def clear_events(vendor_id: str, quote_number: str) -> None:
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
            ("[]", vendor_id, quote_number),
        )
        conn.commit()


# ============================================
# POSTVENTAS
# ============================================
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
            ),
        )
        conn.commit()
        return int(cur.lastrowid)


def list_postventas(vendor_id: str) -> list[dict]:
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
            (vendor_id,),
        )
        rows = cur.fetchall()

    items: list[dict] = []
    for r in rows:
        items.append(
            {
                "id": r[0],
                "client_name": r[1],
                "phone": r[2],
                "postventa_date": r[3],
                "type": r[4],
                "status": r[5],
                "htmlLink": r[6],
                "created_at": r[7],
            }
        )
    return items


def get_postventa_detail(vendor_id: str, postventa_id: int) -> dict | None:
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT id, client_name, phone, sale_date, postventa_date, type, notes,
                   COALESCE(status,'pendiente') as status,
                   event_id, htmlLink, created_at, quote_number
            FROM postventas
            WHERE vendor_id = ? AND id = ?
            """,
            (vendor_id, postventa_id),
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


def update_postventa_status(vendor_id: str, postventa_id: int, status: str) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE postventas
            SET status = ?
            WHERE vendor_id = ? AND id = ?
            """,
            (status, vendor_id, postventa_id),
        )
        conn.commit()


def clear_postventa_event(vendor_id: str, postventa_id: int) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE postventas
            SET event_id = NULL, htmlLink = NULL
            WHERE vendor_id = ? AND id = ?
            """,
            (vendor_id, postventa_id),
        )
        conn.commit()


# ============================================
# ADMIN (encargado)
# ============================================
def list_vendors() -> list[str]:
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT DISTINCT vendor_id FROM quotes
            UNION
            SELECT DISTINCT vendor_id FROM postventas
            ORDER BY vendor_id
            """
        )
        rows = cur.fetchall()
    return [r[0] for r in rows if r and r[0]]


def list_quotes_admin(
    vendor_id: str | None = None,
    status: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 500,
) -> list[dict]:
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
    params: list = []

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

    out: list[dict] = []
    for r in rows:
        extracted = json.loads(r[3]) if r[3] else None
        events = json.loads(r[4]) if r[4] else None
        out.append(
            {
                "quote_number": r[0],
                "vendor_id": r[1],
                "created_at": r[2],
                "extracted": extracted,
                "events_created": events,
                "summary": r[5] or "",
                "status": r[6] or "pendiente",
                "updated_at": r[7],
            }
        )
    return out


def list_postventas_admin(
    vendor_id: str | None = None,
    status: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 500,
) -> list[dict]:
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
    params: list = []

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

    out: list[dict] = []
    for r in rows:
        out.append(
            {
                "id": r[0],
                "vendor_id": r[1],
                "client_name": r[2],
                "postventa_date": r[3],
                "status": r[4] or "pendiente",
                "type": r[5] or "",
                "quote_number": r[6] or "",
                "created_at": r[7],
                "htmlLink": r[8] or "",
            }
        )
    return out


def list_admin_items(
    vendor_id: str | None = None,
    status: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    kind: str | None = None,  # "quote" | "postventa" | None
    limit: int = 500,
) -> list[dict]:
    items: list[dict] = []

    if kind in (None, "", "all", "quote"):
        qs = list_quotes_admin(vendor_id=vendor_id, status=status, date_from=date_from, date_to=date_to, limit=limit)
        for q in qs:
            ex = q.get("extracted") or {}
            items.append(
                {
                    "kind": "cotizacion",
                    "date": q.get("created_at"),
                    "vendor_id": q.get("vendor_id"),
                    "client_name": ex.get("client_name") or "",
                    "ref": q.get("quote_number"),
                    "status": q.get("status") or "pendiente",
                    "total": ex.get("total") or "",
                    "summary": q.get("summary") or "",
                }
            )

    if kind in (None, "", "all", "postventa"):
        ps = list_postventas_admin(vendor_id=vendor_id, status=status, date_from=date_from, date_to=date_to, limit=limit)
        for p in ps:
            items.append(
                {
                    "kind": "postventa",
                    "date": p.get("created_at"),
                    "vendor_id": p.get("vendor_id"),
                    "client_name": p.get("client_name") or "",
                    "ref": f"PV-{p.get('id')}",
                    "status": p.get("status") or "pendiente",
                    "total": "",
                    "summary": p.get("type") or "",
                }
            )

    # Ordenar por fecha desc (string ISO ordena bien)
    items.sort(key=lambda x: x.get("date") or "", reverse=True)
    return items[:limit]


# ============================================
# KPIs simples (vendedor logueado) - opcional
# ============================================
def get_vendor_kpis(vendor_id: str) -> dict:
    """
    KPIs simples para el vendedor logueado.
    - Se basan en status actual (quotes.status y postventas.status)
    - % cierre = cerradas / (cerradas + perdidas) si hay datos
    """
    with get_conn() as conn:
        # --- Quotes KPIs ---
        q = conn.execute(
            """
            SELECT
              COUNT(*) as total,
              SUM(CASE WHEN COALESCE(status,'pendiente')='pendiente' THEN 1 ELSE 0 END) as pendiente,
              SUM(CASE WHEN status='contactado' THEN 1 ELSE 0 END) as contactado,
              SUM(CASE WHEN status='interesado' THEN 1 ELSE 0 END) as interesado,
              SUM(CASE WHEN status='cerrada' THEN 1 ELSE 0 END) as cerrada,
              SUM(CASE WHEN status='perdida' THEN 1 ELSE 0 END) as perdida
            FROM quotes
            WHERE vendor_id = ?
            """,
            (vendor_id,),
        ).fetchone()

        quotes = {
            "total": q[0] or 0,
            "pendiente": q[1] or 0,
            "contactado": q[2] or 0,
            "interesado": q[3] or 0,
            "cerrada": q[4] or 0,
            "perdida": q[5] or 0,
        }

        # --- Postventas KPIs ---
        p = conn.execute(
            """
            SELECT
              COUNT(*) as total,
              SUM(CASE WHEN COALESCE(status,'pendiente')='pendiente' THEN 1 ELSE 0 END) as pendiente,
              SUM(CASE WHEN status='realizada' THEN 1 ELSE 0 END) as realizada,
              SUM(CASE WHEN status='cancelada' THEN 1 ELSE 0 END) as cancelada
            FROM postventas
            WHERE vendor_id = ?
            """,
            (vendor_id,),
        ).fetchone()

        postventas = {
            "total": p[0] or 0,
            "pendiente": p[1] or 0,
            "realizada": p[2] or 0,
            "cancelada": p[3] or 0,
        }

    closed_base = quotes["cerrada"] + quotes["perdida"]
    close_rate = round((quotes["cerrada"] / closed_base) * 100, 1) if closed_base else None

    return {
        "quotes": quotes,
        "postventas": postventas,
        "close_rate": close_rate,
    }


# ============================================
# KPIs completos + Ranking mensual
# ============================================
def _first_day_of_month(d: datetime) -> datetime:
    return d.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _next_month(d: datetime) -> datetime:
    # d = first day of month
    if d.month == 12:
        return d.replace(year=d.year + 1, month=1)
    return d.replace(month=d.month + 1)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _safe_div(num: int, den: int) -> float | None:
    if den <= 0:
        return None
    return (num / den) * 100.0


def _count_quotes(conn: sqlite3.Connection, vendor_id: str | None, date_from_iso: str | None, date_to_iso: str | None) -> dict:
    sql = """
        SELECT COALESCE(status,'pendiente') as st, COUNT(*)
        FROM quotes
        WHERE 1=1
    """
    params: list = []
    if vendor_id:
        sql += " AND vendor_id = ?"
        params.append(vendor_id)
    if date_from_iso:
        sql += " AND created_at >= ?"
        params.append(date_from_iso)
    if date_to_iso:
        sql += " AND created_at < ?"
        params.append(date_to_iso)

    sql += " GROUP BY COALESCE(status,'pendiente')"
    rows = conn.execute(sql, params).fetchall()

    out = {"total": 0, "pendiente": 0, "contactado": 0, "interesado": 0, "cerrada": 0, "perdida": 0}
    for st, c in rows:
        st = (st or "pendiente").strip().lower()
        if st not in out:
            out[st] = 0
        out[st] += int(c)
        out["total"] += int(c)
    return out


def _count_postventas(conn: sqlite3.Connection, vendor_id: str | None, date_from_iso: str | None, date_to_iso: str | None) -> dict:
    sql = """
        SELECT COALESCE(status,'pendiente') as st, COUNT(*)
        FROM postventas
        WHERE 1=1
    """
    params: list = []
    if vendor_id:
        sql += " AND vendor_id = ?"
        params.append(vendor_id)
    if date_from_iso:
        sql += " AND created_at >= ?"
        params.append(date_from_iso)
    if date_to_iso:
        sql += " AND created_at < ?"
        params.append(date_to_iso)

    sql += " GROUP BY COALESCE(status,'pendiente')"
    rows = conn.execute(sql, params).fetchall()

    out = {"total": 0, "pendiente": 0, "realizada": 0, "cancelada": 0}
    for st, c in rows:
        st = (st or "pendiente").strip().lower()
        if st not in out:
            out[st] = 0
        out[st] += int(c)
        out["total"] += int(c)
    return out


def _count_old_open_quotes(conn: sqlite3.Connection, vendor_id: str | None, older_than_days: int = 7) -> int:
    cutoff = (datetime.now() - timedelta(days=older_than_days)).isoformat()
    sql = """
        SELECT COUNT(*)
        FROM quotes
        WHERE 1=1
          AND created_at <= ?
          AND COALESCE(status,'pendiente') NOT IN ('cerrada','perdida')
    """
    params: list = [cutoff]
    if vendor_id:
        sql += " AND vendor_id = ?"
        params.append(vendor_id)
    return int(conn.execute(sql, params).fetchone()[0])


def get_kpis(vendor_id: str | None = None, older_than_days: int = 7) -> dict:
    """
    KPIs completos:
    - lifetime
    - mes actual (MTD)
    - mes anterior
    - comparación
    - alertas (cotizaciones abiertas hace +X días)
    """
    now = datetime.now()
    m0 = _first_day_of_month(now)
    m1 = _next_month(m0)

    prev0 = _first_day_of_month(m0 - timedelta(days=1))  # primer día mes anterior
    prev1 = m0

    with get_conn() as conn:
        # lifetime
        life_quotes = _count_quotes(conn, vendor_id, None, None)
        life_postv = _count_postventas(conn, vendor_id, None, None)

        # mes actual
        cur_quotes = _count_quotes(conn, vendor_id, _iso(m0), _iso(m1))
        cur_postv = _count_postventas(conn, vendor_id, _iso(m0), _iso(m1))

        # mes anterior
        prev_quotes = _count_quotes(conn, vendor_id, _iso(prev0), _iso(prev1))
        prev_postv = _count_postventas(conn, vendor_id, _iso(prev0), _iso(prev1))

        # close rate (cerrada / (cerrada + perdida))
        cur_den = cur_quotes.get("cerrada", 0) + cur_quotes.get("perdida", 0)
        prev_den = prev_quotes.get("cerrada", 0) + prev_quotes.get("perdida", 0)

        cur_close_rate = _safe_div(cur_quotes.get("cerrada", 0), cur_den)
        prev_close_rate = _safe_div(prev_quotes.get("cerrada", 0), prev_den)

        old_open = _count_old_open_quotes(conn, vendor_id, older_than_days=older_than_days)

    # deltas (mes vs anterior)
    def delta(a: int, b: int) -> int:
        return int(a) - int(b)

    def delta_pct(a: int, b: int) -> float | None:
        return None if b == 0 else ((a - b) / b) * 100.0

    compare = {
        "quotes_total_delta": delta(cur_quotes["total"], prev_quotes["total"]),
        "quotes_total_delta_pct": delta_pct(cur_quotes["total"], prev_quotes["total"]),
        "quotes_cerrada_delta": delta(cur_quotes.get("cerrada", 0), prev_quotes.get("cerrada", 0)),
        "quotes_cerrada_delta_pct": delta_pct(cur_quotes.get("cerrada", 0), prev_quotes.get("cerrada", 0)),
        "postventas_total_delta": delta(cur_postv["total"], prev_postv["total"]),
        "postventas_total_delta_pct": delta_pct(cur_postv["total"], prev_postv["total"]),
        "close_rate_delta": None
        if (cur_close_rate is None or prev_close_rate is None)
        else (cur_close_rate - prev_close_rate),
    }

    return {
        "lifetime": {"quotes": life_quotes, "postventas": life_postv},
        "month": {
            "from": m0.strftime("%Y-%m-%d"),
            "to": (m1 - timedelta(days=1)).strftime("%Y-%m-%d"),
            "quotes": cur_quotes,
            "postventas": cur_postv,
            "close_rate": None if cur_close_rate is None else round(cur_close_rate, 1),
        },
        "prev_month": {
            "from": prev0.strftime("%Y-%m-%d"),
            "to": (prev1 - timedelta(days=1)).strftime("%Y-%m-%d"),
            "quotes": prev_quotes,
            "postventas": prev_postv,
            "close_rate": None if prev_close_rate is None else round(prev_close_rate, 1),
        },
        "compare": compare,
        "alerts": {"old_open_quotes": old_open, "older_than_days": older_than_days},
    }


def _month_range(ref: datetime) -> tuple[str, str]:
    m0 = _first_day_of_month(ref)
    m1 = _next_month(m0)
    return _iso(m0), _iso(m1)


def list_vendor_kpis_month(date_ref: datetime | None = None) -> list[dict]:
    """
    Ranking por vendedor del MES (cotizaciones, cerradas, perdidas, %cierre, postventas).
    """
    ref = date_ref or datetime.now()
    df_iso, dt_iso = _month_range(ref)

    vendors = list_vendors()
    out: list[dict] = []

    with get_conn() as conn:
        for v in vendors:
            q = _count_quotes(conn, v, df_iso, dt_iso)
            p = _count_postventas(conn, v, df_iso, dt_iso)

            den = q.get("cerrada", 0) + q.get("perdida", 0)
            close_rate = _safe_div(q.get("cerrada", 0), den)

            out.append(
                {
                    "vendor_id": v,
                    "quotes_total": q.get("total", 0),
                    "quotes_cerrada": q.get("cerrada", 0),
                    "quotes_perdida": q.get("perdida", 0),
                    "quotes_pendiente": q.get("pendiente", 0),
                    "close_rate": None if close_rate is None else round(close_rate, 1),
                    "postventas_total": p.get("total", 0),
                    "postventas_pendiente": p.get("pendiente", 0),
                    "postventas_realizada": p.get("realizada", 0),
                    "postventas_cancelada": p.get("cancelada", 0),
                }
            )

    # orden: más cerradas, luego más cotizaciones
    out.sort(key=lambda x: (x["quotes_cerrada"], x["quotes_total"]), reverse=True)
    return out
