from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import sys
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

INVENTORY_CSV = DATA_DIR / "inventory.csv"
INVENTORY_MERCADO_CSV = DATA_DIR / "inventory_mercado.csv"
SALES_CSV = DATA_DIR / "sales.csv"
SALES_MERCADO_CSV = DATA_DIR / "sales_mercado.csv"
PROCESSED_EVENTS_CSV = DATA_DIR / "processed_events.csv"


@dataclass
class ParsedPayload:
    issue_number: str
    issue_title: str
    issue_body: str
    issue_author: str
    labels: list[str]
    raw: dict[str, Any]
    payload_type: str
    fecha: str
    metodo_pago: str
    txn_id: str
    issue_ref: str
    accion: str
    modo: str
    valor: str
    sku: str
    descripcion: str
    razon: str
    detalle: str
    notas: str
    items: list[dict[str, Any]]


def log(msg: str) -> None:
    print(msg, flush=True)


def normalize_payment(value: str) -> str:
    v = str(value or "").strip().lower()
    if v in {"tarjeta", "transferencia", "merma"}:
        return v
    return "efectivo"


def normalize_type(value: str) -> str:
    v = str(value or "").strip().lower().replace(" ", "_").replace("-", "_")
    aliases = {
        "venta": "venta",
        "venta_mkt": "venta_mkt",
        "merma": "merma",
        "merma_mkt": "merma_mkt",
        "regalada": "merma",
        "regaladas": "merma",
    }
    return aliases.get(v, v)


def to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value).strip() or default))
    except Exception:
        return default


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value).strip() or default)
    except Exception:
        return default


def sha1_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def csv_read(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return [{str(k).strip(): (v if v is not None else "") for k, v in raw.items()} for raw in reader]


def csv_write(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = rows or []
    if fieldnames is None:
        fieldnames = []
        seen: set[str] = set()
        for row in rows:
            for k in row.keys():
                if k not in seen:
                    seen.add(k)
                    fieldnames.append(k)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n", extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def append_csv_row(path: Path, row: dict[str, Any], preferred_fieldnames: list[str] | None = None) -> None:
    rows = csv_read(path)
    rows.append({k: str(v) if v is not None else "" for k, v in row.items()})
    csv_write(path, rows, preferred_fieldnames)


def ensure_csv_if_missing(path: Path, fieldnames: list[str]) -> None:
    if not path.exists():
        csv_write(path, [], fieldnames)


def ensure_core_files() -> None:
    ensure_csv_if_missing(INVENTORY_CSV, ["item", "descripcion", "stock", "precio", "product_id"])
    ensure_csv_if_missing(INVENTORY_MERCADO_CSV, ["item", "descripcion", "stock", "precio", "product_id"])
    sales_fields = ["txn_id", "fecha", "item", "cantidad", "precio_unit", "importe", "issue", "metodo_pago", "source_id", "descripcion", "status", "correction_ref", "notas"]
    ensure_csv_if_missing(SALES_CSV, sales_fields)
    ensure_csv_if_missing(SALES_MERCADO_CSV, sales_fields)
    ensure_csv_if_missing(PROCESSED_EVENTS_CSV, ["issue_number", "event_hash", "payload_type", "fecha", "status"])


def inventory_find_row(rows: list[dict[str, str]], sku: str) -> dict[str, str] | None:
    sku = str(sku).strip()
    for row in rows:
        if str(row.get("item", "")).strip() == sku:
            return row
    return None


def inventory_get_desc(rows: list[dict[str, str]], sku: str, fallback: str = "") -> str:
    row = inventory_find_row(rows, sku)
    return str(row.get("descripcion", "")).strip() if row else fallback


def inventory_get_price(rows: list[dict[str, str]], sku: str, fallback: float = 0.0) -> float:
    row = inventory_find_row(rows, sku)
    return to_float(row.get("precio", ""), fallback) if row else fallback


def inventory_adjust(path: Path, sku: str, descripcion: str, mode: str, value: int) -> None:
    rows = csv_read(path)
    row = inventory_find_row(rows, sku)
    if row is None:
        row = {"item": sku, "descripcion": descripcion, "stock": "0", "precio": "0", "product_id": ""}
        rows.append(row)
    current_stock = to_int(row.get("stock", 0), 0)
    row["stock"] = str(value if mode == "set" else current_stock + value)
    if descripcion and not str(row.get("descripcion", "")).strip():
        row["descripcion"] = descripcion
    csv_write(path, rows, ["item", "descripcion", "stock", "precio", "product_id"])


def parse_issue_event() -> dict[str, Any]:
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path:
        raise RuntimeError("Falta GITHUB_EVENT_PATH")
    with open(event_path, "r", encoding="utf-8") as f:
        return json.load(f)


def strip_conflict_lines(text: str) -> str:
    out = []
    for line in str(text or "").splitlines():
        if line.startswith(("<<<<<<<", "=======", ">>>>>>>")) or line.strip() in {"Updated upstream", "Stashed changes"}:
            continue
        out.append(line)
    return "\n".join(out).strip()


def extract_simple_fields(body: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for raw in strip_conflict_lines(body).splitlines():
        line = raw.strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        k = key.strip().lower().replace(" ", "_")
        if k in {"tipo", "type", "fecha", "txn_id", "client_txn_id", "issue_ref", "accion", "metodo_pago", "modo", "valor", "sku", "descripcion", "notas", "items"}:
            fields[k] = value.strip()
    return fields


def parse_markdown_table_items(text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or "|" not in line:
            continue
        if line.startswith("|"):
            line = line[1:]
        if line.endswith("|"):
            line = line[:-1]
        if not line or ("---" in line and "|" in line):
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 2:
            continue
        sku = parts[0]
        if not sku or sku.lower() in {"sku", "item", "producto"}:
            continue
        rows.append({
            "item": sku,
            "cantidad": to_int(parts[1], 0),
            "precio": to_float(parts[2], 0.0) if len(parts) >= 3 else 0.0,
            "descripcion": parts[3] if len(parts) >= 4 else "",
        })
    return [r for r in rows if r["item"] and to_int(r["cantidad"], 0) != 0]


def parse_json_body(body: str) -> dict[str, Any] | None:
    text = strip_conflict_lines(body).strip()
    if not text.startswith("{") or not text.endswith("}"):
        return None
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def guess_type_from_title(title: str) -> str:
    t = str(title or "").strip().lower().replace(" ", "_").replace("-", "_")
    if "venta_mkt" in t or "venta_mercado" in t:
        return "venta_mkt"
    if "merma_mkt" in t:
        return "merma_mkt"
    if "merma" in t or "regalada" in t or "regaladas" in t:
        return "merma"
    if "venta" in t:
        return "venta"
    return ""


def guess_type_from_labels(labels: list[str]) -> str:
    normalized = [normalize_type(x) for x in labels]
    for p in ["venta_mkt", "merma_mkt", "merma", "venta"]:
        if p in normalized:
            return p
    return ""


def is_probable_merma(title: str, labels: list[str], items: list[dict[str, Any]]) -> bool:
    title_norm = str(title or "").strip().lower()
    if "movimiento" not in title_norm:
        return False
    if labels:
        return False
    if not items:
        return False
    return all(to_float(item.get("precio", 0), 0.0) == 0.0 for item in items)


def parse_items_from_any(body: str, json_body: dict[str, Any] | None, fields: dict[str, str]) -> list[dict[str, Any]]:
    if json_body and isinstance(json_body.get("items"), list):
        out = []
        for item in json_body["items"]:
            if isinstance(item, dict):
                out.append({
                    "item": str(item.get("item", "")).strip(),
                    "cantidad": to_int(item.get("cantidad", 0), 0),
                    "precio": to_float(item.get("precio", 0), 0.0),
                    "descripcion": str(item.get("descripcion", "")).strip(),
                })
        out = [r for r in out if r["item"] and to_int(r["cantidad"], 0) != 0]
        if out:
            return out

    raw_items = str(fields.get("items", "")).strip()
    if raw_items:
        try:
            parsed = json.loads(raw_items)
            if isinstance(parsed, list):
                out = []
                for item in parsed:
                    if isinstance(item, dict):
                        out.append({
                            "item": str(item.get("item", "")).strip(),
                            "cantidad": to_int(item.get("cantidad", 0), 0),
                            "precio": to_float(item.get("precio", 0), 0.0),
                            "descripcion": str(item.get("descripcion", "")).strip(),
                        })
                out = [r for r in out if r["item"] and to_int(r["cantidad"], 0) != 0]
                if out:
                    return out
        except Exception:
            pass

    return parse_markdown_table_items(strip_conflict_lines(body))


def parse_issue_payload(event: dict[str, Any]) -> ParsedPayload:
    issue = event.get("issue", {}) or {}
    title = str(issue.get("title", "")).strip()
    body = str(issue.get("body", "")).strip()
    author = str((issue.get("user") or {}).get("login", "")).strip()
    number = str(issue.get("number", "")).strip()
    labels = [str((x or {}).get("name", "")).strip() for x in (issue.get("labels") or [])]

    json_body = parse_json_body(body)
    fields = extract_simple_fields(body)
    items = parse_items_from_any(body, json_body, fields)

    payload_type = normalize_type(
        (json_body or {}).get("type")
        or fields.get("type")
        or fields.get("tipo")
        or guess_type_from_title(title)
        or guess_type_from_labels(labels)
    )
    if not payload_type and is_probable_merma(title, labels, items):
        payload_type = "merma"

    fecha = str((json_body or {}).get("fecha", "")).strip() or fields.get("fecha", "").strip() or ""
    metodo_pago = normalize_payment(str((json_body or {}).get("metodo_pago", "")).strip() or fields.get("metodo_pago", "").strip())
    if payload_type == "merma":
        metodo_pago = "merma"

    txn_id = str((json_body or {}).get("txn_id", "")).strip() or str((json_body or {}).get("client_txn_id", "")).strip() or fields.get("txn_id", "").strip() or fields.get("client_txn_id", "").strip()
    issue_ref = str((json_body or {}).get("issue_ref", "")).strip() or fields.get("issue_ref", "").strip()
    accion = str((json_body or {}).get("accion", "")).strip().lower() or fields.get("accion", "").strip().lower()
    modo = str((json_body or {}).get("modo", "")).strip().lower() or fields.get("modo", "").strip().lower()
    valor = str((json_body or {}).get("valor", "")).strip() or fields.get("valor", "").strip()
    sku = str((json_body or {}).get("sku", "")).strip() or fields.get("sku", "").strip()
    descripcion = str((json_body or {}).get("descripcion", "")).strip() or fields.get("descripcion", "").strip()
    razon = str((json_body or {}).get("razon", "")).strip()
    detalle = str((json_body or {}).get("detalle", "")).strip()
    notas = str((json_body or {}).get("notas", "")).strip() or fields.get("notas", "").strip()

    if not txn_id and payload_type in {"venta", "venta_mkt", "merma", "merma_mkt"}:
        txn_id = f"txn-issue-{number}"

    return ParsedPayload(number, title, body, author, labels, event, payload_type, fecha, metodo_pago, txn_id, issue_ref, accion, modo, valor, sku, descripcion, razon, detalle, notas, items)


def processed_event_exists(issue_number: str, event_hash: str) -> bool:
    for row in csv_read(PROCESSED_EVENTS_CSV):
        if str(row.get("issue_number", "")).strip() == issue_number and str(row.get("event_hash", "")).strip() == event_hash:
            return True
    return False


def mark_processed(issue_number: str, event_hash: str, payload_type: str, fecha: str, status: str) -> None:
    append_csv_row(PROCESSED_EVENTS_CSV, {"issue_number": issue_number, "event_hash": event_hash, "payload_type": payload_type, "fecha": fecha, "status": status}, ["issue_number", "event_hash", "payload_type", "fecha", "status"])


def add_github_comment(issue_number: str, body: str) -> None:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    repo = os.environ.get("GITHUB_REPOSITORY", "").strip()
    if not token or not repo or not issue_number:
        return
    req = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/issues/{issue_number}/comments",
        data=json.dumps({"body": body}).encode("utf-8"),
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json", "Content-Type": "application/json", "User-Agent": "inventory-bot"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=20).read()
    except Exception as exc:
        log(f"No se pudo comentar en issue #{issue_number}: {exc}")


def close_github_issue(issue_number: str) -> None:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    repo = os.environ.get("GITHUB_REPOSITORY", "").strip()
    if not token or not repo or not issue_number:
        return
    req = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/issues/{issue_number}",
        data=json.dumps({"state": "closed"}).encode("utf-8"),
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json", "Content-Type": "application/json", "User-Agent": "inventory-bot"},
        method="PATCH",
    )
    try:
        urllib.request.urlopen(req, timeout=20).read()
    except Exception as exc:
        log(f"No se pudo cerrar issue #{issue_number}: {exc}")


def slack_token() -> str:
    return str(os.environ.get("SLACK_BOT_TOKEN") or os.environ.get("SLACK_API_TOKEN") or "").strip()


def slack_channel_for_scope(scope: str) -> str:
    if scope == "mercado":
        return str(os.environ.get("SLACK_CHANNEL_VENTAS_BAZAR") or os.environ.get("VENTAS_BAZAR_SLACK_CHANNEL") or "").strip()
    return str(os.environ.get("SLACK_CHANNEL_VENTAS") or os.environ.get("VENTAS_SLACK_CHANNEL") or "").strip()


def post_slack_message(text: str, scope: str) -> None:
    token = slack_token()
    channel = slack_channel_for_scope(scope)
    if not token or not channel:
        log(f"Slack omitido: faltan token/canal para scope={scope}")
        return

    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=json.dumps({"channel": channel, "text": text}).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
            "User-Agent": "inventory-bot",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="ignore"))
            if not data.get("ok", False):
                log(f"Slack respondió error: {data}")
    except Exception as exc:
        log(f"No se pudo enviar mensaje a Slack: {exc}")


def format_items_for_slack(items: list[dict[str, Any]]) -> str:
    if not items:
        return "Sin items"
    return "\n".join(
        f"• {str(it.get('item','')).strip()} x{to_int(it.get('cantidad',0),0)} (${to_float(it.get('precio',0),0.0):.2f})"
        for it in items
    )


def slack_summary(payload: ParsedPayload, result: str) -> tuple[str, str]:
    scope = "mercado" if payload.payload_type in {"venta_mkt", "merma_mkt"} else "normal"
    title_map = {
        "venta": "🧾 Venta",
        "venta_mkt": "🧾 Venta mercado",
        "merma": "🎁 Merma / regaladas",
        "merma_mkt": "🎁 Merma mercado",
    }
    lines = [
        title_map.get(payload.payload_type, payload.payload_type),
        f"Fecha: {str(payload.fecha or '').strip() or datetime.now().strftime('%Y-%m-%d')}",
        f"Issue: #{payload.issue_number}",
    ]
    if payload.txn_id:
        lines.append(f"Txn: {payload.txn_id}")
    lines.append(f"Método: {payload.metodo_pago}")
    if payload.notas:
        lines.append(f"Notas: {payload.notas}")
    lines.append("Items:")
    lines.append(format_items_for_slack(payload.items))
    lines.append(f"Resultado: {result}")
    return ("\n".join(lines), scope)


def resolve_items_with_inventory(items: list[dict[str, Any]], inventory_path: Path, zero_price: bool = False) -> list[dict[str, Any]]:
    inventory_rows = csv_read(inventory_path)
    resolved = []
    for item in items:
        sku = str(item.get("item", "")).strip()
        qty = to_int(item.get("cantidad", 0), 0)
        if not sku or qty == 0:
            continue
        desc = str(item.get("descripcion", "")).strip() or inventory_get_desc(inventory_rows, sku, "")
        price = 0.0 if zero_price else to_float(item.get("precio", 0), 0.0)
        if not zero_price and price == 0:
            price = inventory_get_price(inventory_rows, sku, 0.0)
        resolved.append({"item": sku, "cantidad": qty, "precio": price, "descripcion": desc})
    return resolved


def register_sale(payload: ParsedPayload, mercado: bool, zero_price: bool = False) -> str:
    inventory_path = INVENTORY_MERCADO_CSV if mercado else INVENTORY_CSV
    sales_path = SALES_MERCADO_CSV if mercado else SALES_CSV
    txn_id = payload.txn_id or f"txn-issue-{payload.issue_number}"
    fecha = str(payload.fecha or "").strip() or datetime.now().strftime("%Y-%m-%d")
    items = resolve_items_with_inventory(payload.items, inventory_path, zero_price=zero_price)
    if not items:
        raise ValueError("La operación no trae items válidos")

    for item in items:
        qty = to_int(item["cantidad"], 0)
        price = 0.0 if zero_price else to_float(item["precio"], 0.0)
        append_csv_row(
            sales_path,
            {
                "txn_id": txn_id,
                "fecha": fecha,
                "item": item["item"],
                "cantidad": qty,
                "precio_unit": f"{price:.2f}",
                "importe": f"{qty * price:.2f}",
                "issue": payload.issue_number,
                "metodo_pago": "merma" if zero_price else normalize_payment(payload.metodo_pago),
                "source_id": payload.issue_number,
                "descripcion": item["descripcion"] or item["item"],
                "status": "activa",
                "correction_ref": "",
                "notas": payload.notas,
            },
            ["txn_id", "fecha", "item", "cantidad", "precio_unit", "importe", "issue", "metodo_pago", "source_id", "descripcion", "status", "correction_ref", "notas"],
        )
        inventory_adjust(inventory_path, item["item"], item["descripcion"] or item["item"], "delta", -qty)
    return txn_id


def build_event_hash(payload: ParsedPayload) -> str:
    return sha1_text(json.dumps({
        "type": payload.payload_type, "fecha": payload.fecha, "metodo_pago": payload.metodo_pago, "txn_id": payload.txn_id,
        "issue_ref": payload.issue_ref, "accion": payload.accion, "modo": payload.modo, "valor": payload.valor,
        "sku": payload.sku, "descripcion": payload.descripcion, "razon": payload.razon, "detalle": payload.detalle,
        "notas": payload.notas, "items": payload.items, "body": payload.issue_body,
    }, ensure_ascii=False, sort_keys=True))


def process_payload(payload: ParsedPayload) -> str:
    ptype = payload.payload_type
    if not ptype:
        raise ValueError("No pude detectar el tipo de operación")
    if ptype == "venta":
        return f"Venta normal registrada. txn={register_sale(payload, mercado=False)}"
    if ptype == "venta_mkt":
        return f"Venta mercado registrada. txn={register_sale(payload, mercado=True)}"
    if ptype == "merma":
        return f"Merma/regaladas normal registrada. txn={register_sale(payload, mercado=False, zero_price=True)}"
    if ptype == "merma_mkt":
        return f"Merma/regaladas mercado registrada. txn={register_sale(payload, mercado=True, zero_price=True)}"
    raise ValueError(f"Tipo no soportado: {ptype}")


def main() -> None:
    ensure_core_files()
    payload = parse_issue_payload(parse_issue_event())
    if not payload.payload_type:
        log("No hay tipo de operación en este evento. Se omite sin error.")
        return

    event_hash = build_event_hash(payload)
    if processed_event_exists(payload.issue_number, event_hash):
        log(f"Issue #{payload.issue_number} ya estaba procesado. No hago nada.")
        return

    try:
        result = process_payload(payload)
        fecha_out = str(payload.fecha or "").strip() or datetime.now().strftime("%Y-%m-%d")
        mark_processed(payload.issue_number, event_hash, payload.payload_type, fecha_out, "ok")
        log(result)
        add_github_comment(payload.issue_number, f"✅ Procesado correctamente.\n\n{result}")
        slack_text, scope = slack_summary(payload, result)
        post_slack_message(slack_text, scope)
        close_github_issue(payload.issue_number)
    except Exception as exc:
        err = f"❌ Error procesando issue #{payload.issue_number}: {exc}"
        log(err)
        fecha_out = str(payload.fecha or "").strip() or datetime.now().strftime("%Y-%m-%d")
        mark_processed(payload.issue_number, event_hash, payload.payload_type, fecha_out, "error")
        add_github_comment(payload.issue_number, err)
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        if "No pude detectar el tipo de operación" in str(exc):
            log(f"Sin operación para procesar: {exc}")
            sys.exit(0)
        log(f"Fallo fatal: {exc}")
        sys.exit(1)
