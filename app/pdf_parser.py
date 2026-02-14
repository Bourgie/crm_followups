import re
import pdfplumber

def parse_budget_pdf(pdf_path: str) -> dict:
    """
    Parser para el formato de presupuesto/cotización del CRM.
    Ajustable, pero con PDF fijo suele funcionar bien.
    """
    with pdfplumber.open(pdf_path) as pdf:
        text = "\n".join((p.extract_text() or "") for p in pdf.pages)

    def m(pattern: str, flags=0):
        match = re.search(pattern, text, flags)
        return match.group(1).strip() if match else None

    quote_number = m(r"Número\s+([0-9]{4}-[0-9]{8})")
    issue_date   = m(r"Fecha de Emisión\s+([0-9]{2}/[0-9]{2}/[0-9]{4})")
    seller       = m(r"Vendedor:\s*(.+)")
    total        = m(r"\bTOTAL\b\s*([\d\.,]+)")

    # Cliente: suele quedar en mayúsculas al final de esa línea
    client_name  = m(r"Apellido y Nombre / Razón Social:.*\s([A-ZÁÉÍÓÚÑ0-9]+)\s*$", flags=re.MULTILINE)

    return {
        "quote_number": quote_number or "S/N",
        "issue_date": issue_date,
        "seller": seller,
        "client_name": client_name or "Cliente",
        "total": total,
    }
