"""
reprocess_quarantine.py - Intelligent Quarantine Repair & Recovery Engine

Implements advanced heuristics to automatically repair recoverable quarantined records
and re-ingest them into `orders_validated` via Idempotent Upsert, updating their status
in `orders_quarantine` to 'RESOLVED'.

Key Repair Heuristics:
  1. Truncated JSON Repair: Closes broken brackets/braces or extracts valid items via regex.
  2. Total Amount Heuristic: Infers total_amount from payment_amount or item unit prices.
  3. Negative Value Recovery: Uses absolute values if amount aligns with order context.
  4. Date Heuristic: Parses exotic non-standard date formats.
"""

import sys
import re
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Tuple
from pymongo import UpdateOne

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    MONGO_DB_NAME,
    COLLECTION_VALIDATED,
    COLLECTION_QUARANTINE,
    BATCH_SIZE,
)
from src.mongo_setup import get_mongo_client, get_database
from src.quality_rules import (
    clean_arabic_numerals,
    clean_monetary_amount,
    clean_phone_number,
    clean_email,
    clean_date_format,
    clean_status_and_whitespace,
    validate_and_parse_items_json,
)

def attempt_repair_truncated_json(broken_json_str: str) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Attempts to salvage products from broken or truncated JSON strings.
    Example: '[{"sku":"SKU-1001","qty":2' -> balanced and parsed into [{'sku': 'SKU-1001', 'qty': 2}]
    """
    if not broken_json_str or not isinstance(broken_json_str, str):
        return [], False

    s = broken_json_str.strip()
    
    # Try direct parse first
    try:
        data = json.loads(s)
        if isinstance(data, list) and len(data) > 0:
            return data, True
    except Exception:
        pass

    # Heuristic 1: Balance unclosed braces and brackets
    open_braces = s.count("{") - s.count("}")
    open_brackets = s.count("[") - s.count("]")

    repaired_str = s
    # If ends with an unquoted key/value or comma, trim trailing comma or quote
    repaired_str = re.sub(r',\s*$', '', repaired_str)
    
    # Add missing closing quotes if odd count of quotes
    if repaired_str.count('"') % 2 != 0:
        repaired_str += '"'

    repaired_str += ("}" * max(0, open_braces))
    repaired_str += ("]" * max(0, open_brackets))

    try:
        data = json.loads(repaired_str)
        if isinstance(data, list) and len(data) > 0:
            return data, True
    except Exception:
        pass

    # Heuristic 2: Regex extraction of SKU, name, qty, price
    items_recovered = []
    # Match patterns like "sku":"...", "qty":...
    matches = re.findall(r'\{[^{}]*\}', s + "}")
    for m in matches:
        try:
            item_obj = json.loads(m)
            if "sku" in item_obj or "name" in item_obj:
                items_recovered.append(item_obj)
        except Exception:
            # Try manual regex field extraction
            sku = re.search(r'"sku"\s*:\s*"([^"]+)"', m)
            qty = re.search(r'"qty"\s*:\s*(-?\d+)', m)
            if sku:
                items_recovered.append({
                    "sku": sku.group(1),
                    "qty": abs(int(qty.group(1))) if qty else 1,
                    "name": "منتج مسترجع",
                    "unit_price": 0.0,
                    "total": 0.0
                })

    if items_recovered:
        return items_recovered, True

    return [], False


def attempt_repair_quarantine_record(quar_doc: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], List[Dict[str, Any]], str]:
    """
    Applies advanced heuristics to attempt repairing a single quarantined document.
    
    Returns:
        (is_repaired, repaired_record, corrections_list, repair_notes)
    """
    raw = quar_doc.get("raw_record", {})
    error_code = quar_doc.get("error_code", "")
    order_id = raw.get("order_id") or quar_doc.get("order_id", "").strip()

    # Rule: Missing order_id without any customer identifier cannot be safely fabricated
    if not order_id:
        if raw.get("customer_id") and raw.get("order_date"):
            # Synthetic Order ID Heuristic
            order_id = f"SYN-ORD-{raw['customer_id']}-{quar_doc.get('source_row_number', '0')}"
        else:
            return False, {}, [], "Cannot repair: Missing stable order_id."

    corrections: List[Dict[str, Any]] = []

    # 1. Clean order_id
    cleaned_order_id, mod = clean_arabic_numerals(str(order_id))
    cleaned_order_id = cleaned_order_id.strip()

    # 2. Date recovery
    date_val, date_corr, date_err = clean_date_format(str(raw.get("order_date", "")))
    if date_err:
        # Check if year is salvageable (e.g. year was parsed as 1800, replace with current year 2025)
        raw_d_str = str(raw.get("order_date", ""))
        fixed_d_str = re.sub(r'\b18\d{2}\b', '2025', raw_d_str)
        fixed_d_str = re.sub(r'\b22\d{2}\b', '2025', fixed_d_str)
        date_val, date_corr, date_err = clean_date_format(fixed_d_str)
        if not date_err:
            corrections.append({
                "field": "order_date",
                "original_value": raw_d_str,
                "corrected_value": date_val,
                "rule_code": "REPAIR_DATE_YEAR_NORMALIZATION"
            })
        else:
            return False, {}, [], f"Cannot repair date: {date_err}"
    elif date_corr:
        corrections.append(date_corr)

    # 3. Items JSON Repair
    items, items_err = validate_and_parse_items_json(raw.get("items_json", ""))
    if items_err:
        # Attempt smart truncated JSON recovery
        salvaged_items, ok = attempt_repair_truncated_json(raw.get("items_json", ""))
        if ok and len(salvaged_items) > 0:
            items = salvaged_items
            corrections.append({
                "field": "items",
                "original_value": str(raw.get("items_json", ""))[:50] + "...",
                "corrected_value": f"{len(items)} items salvaged",
                "rule_code": "REPAIR_SALVAGED_TRUNCATED_JSON"
            })
        else:
            return False, {}, [], f"Cannot repair items JSON: {items_err}"

    # 4. Monetary amounts and negative numbers recovery
    deliv_cost, deliv_corrs, _ = clean_monetary_amount(str(raw.get("delivery_cost", "0")), "delivery_cost")
    corrections.extend(deliv_corrs)

    pay_amt, pay_corrs, pay_err = clean_monetary_amount(str(raw.get("payment_amount", "0")), "payment_amount")
    corrections.extend(pay_corrs)

    tot_amt, tot_corrs, tot_err = clean_monetary_amount(str(raw.get("total_amount", "0")), "total_amount")
    corrections.extend(tot_corrs)

    # If total amount is missing / ???, recalculate from items and delivery
    if tot_amt is None or tot_err:
        computed_items_tot = sum(float(it.get("total", 0.0)) for it in items)
        if computed_items_tot > 0:
            tot_amt = computed_items_tot + (deliv_cost or 0.0)
            corrections.append({
                "field": "total_amount",
                "original_value": str(raw.get("total_amount", "")),
                "corrected_value": tot_amt,
                "rule_code": "REPAIR_TOTAL_FROM_ITEMS_SUM"
            })
        elif pay_amt and pay_amt > 0:
            tot_amt = pay_amt
            corrections.append({
                "field": "total_amount",
                "original_value": str(raw.get("total_amount", "")),
                "corrected_value": tot_amt,
                "rule_code": "REPAIR_TOTAL_FROM_PAYMENT_AMOUNT"
            })
        else:
            tot_amt = deliv_cost or 0.0

    if pay_amt is None or pay_err:
        pay_amt = tot_amt

    # 5. Customer & Contact Info
    phone, phone_corr = clean_phone_number(str(raw.get("customer_phone", "")))
    if phone_corr:
        corrections.append(phone_corr)

    email, email_corr = clean_email(str(raw.get("customer_email", "")))
    if email_corr:
        corrections.append(email_corr)

    status_val, status_corr = clean_status_and_whitespace(str(raw.get("status", "PENDING")))
    if status_corr:
        corrections.append(status_corr)

    # Build clean repaired document
    repaired_doc = {
        "order_id": cleaned_order_id,
        "order_date": date_val,
        "status": status_val,
        "customer_id": clean_arabic_numerals(str(raw.get("customer_id", "")))[0].strip(),
        "customer_name": str(raw.get("customer_name", "")).strip(),
        "customer_phone": phone,
        "customer_email": email,
        "city": str(raw.get("city", "")).strip(),
        "district": str(raw.get("district", "")).strip(),
        "delivery_type": str(raw.get("delivery_type", "عادي")).strip(),
        "delivery_cost": deliv_cost,
        "payment_method": str(raw.get("payment_method", "")).strip(),
        "payment_status": str(raw.get("payment_status", "")).strip(),
        "payment_amount": pay_amt,
        "currency": "YER",
        "total_amount": tot_amt,
        "items": items,
        "quality_status": "repaired_from_quarantine",
        "corrections": corrections,
        "repaired_at": datetime.utcnow().isoformat(),
        "original_quarantine_error": error_code,
        "run_id": quar_doc.get("run_id", "repair_run"),
        "source_file": quar_doc.get("source_file", ""),
    }

    return True, repaired_doc, corrections, "Successfully repaired using advanced heuristic engine."


def run_quarantine_repair(batch_size: int = BATCH_SIZE) -> Dict[str, Any]:
    """
    Scans `orders_quarantine` collection in MongoDB, attempts automated repairs,
    upserts repaired records into `orders_validated`, and updates quarantine status.
    """
    print("\n" + "=" * 75)
    print(" 🛠️  [QUARANTINE RECOVERY ENGINE] Starting Automated Repair Pipeline")
    print("=" * 75)

    client = get_mongo_client()
    db = get_database(client)

    quar_col = db[COLLECTION_QUARANTINE]
    val_col = db[COLLECTION_VALIDATED]

    total_quarantined = quar_col.count_documents({"status": {"$ne": "RESOLVED"}})
    print(f" - Unresolved Quarantined Records Found: {total_quarantined:,}")

    if total_quarantined == 0:
        print(" - No unresolved records in quarantine to process.")
        client.close()
        return {"total": 0, "repaired": 0, "unrepairable": 0}

    repaired_count = 0
    unrepairable_count = 0

    upsert_ops: List[UpdateOne] = []
    quar_update_ops: List[UpdateOne] = []

    t_start = time.time()
    cursor = quar_col.find({"status": {"$ne": "RESOLVED"}}, batch_size=batch_size)

    for doc in cursor:
        is_repaired, repaired_rec, corrs, note = attempt_repair_quarantine_record(doc)

        if is_repaired and repaired_rec.get("order_id"):
            repaired_count += 1
            # 1. Upsert into orders_validated
            upsert_ops.append(
                UpdateOne({"order_id": repaired_rec["order_id"]}, {"$set": repaired_rec}, upsert=True)
            )
            # 2. Mark as resolved in orders_quarantine
            quar_update_ops.append(
                UpdateOne(
                    {"_id": doc["_id"]},
                    {"$set": {
                        "status": "RESOLVED",
                        "resolution_action": "REPAIRED_AND_PROMOTED_TO_VALIDATED",
                        "resolved_at": datetime.utcnow().isoformat(),
                        "repair_notes": note
                    }}
                )
            )
        else:
            unrepairable_count += 1

        # Flush batches
        if len(upsert_ops) >= batch_size:
            val_col.bulk_write(upsert_ops, ordered=False)
            quar_col.bulk_write(quar_update_ops, ordered=False)
            upsert_ops = []
            quar_update_ops = []
            print(f"   ... Processed {repaired_count + unrepairable_count:,} (Repaired: {repaired_count:,} | Unrepairable: {unrepairable_count:,})")

    # Flush remaining
    if upsert_ops:
        val_col.bulk_write(upsert_ops, ordered=False)
        quar_col.bulk_write(quar_update_ops, ordered=False)

    cursor.close()
    elapsed = time.time() - t_start

    print("\n" + "-" * 75)
    print(" 🎯 [QUARANTINE REPAIR COMPLETED]")
    print(f"   * Total Scanned:       {total_quarantined:,}")
    print(f"   * Successfully Repaired: {repaired_count:,} ({ (repaired_count/total_quarantined*100) if total_quarantined else 0:.1f}%)")
    print(f"   * Remaining Fatal:     {unrepairable_count:,}")
    print(f"   * Elapsed Time:        {elapsed:.2f}s ({repaired_count/elapsed if elapsed else 0:,.1f} rec/s)")
    print("=" * 75 + "\n")

    client.close()
    return {
        "total_scanned": total_quarantined,
        "successfully_repaired": repaired_count,
        "unrepairable_count": unrepairable_count,
        "elapsed_seconds": round(elapsed, 2)
    }


if __name__ == "__main__":
    run_quarantine_repair()
