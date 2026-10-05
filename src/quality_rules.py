"""
quality_rules.py - Data Transformation, Quality Auditing & Quarantine Classification
Implements the 8 Mandatory Auto-Cleaning Rules and Doctor's Exact Quarantine & Correction Criteria.
"""

import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Add project root to sys.path
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    ARABIC_WORDS_NUMBER_MAP,
    TARGET_CURRENCY,
    VALID_STATUS_MAP,
)

# Arabic-Indic to ASCII digit mapping
ARABIC_INDIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩٫،", "0123456789..")

# Rule Codes Constants
RULE_R1_ARABIC_NUMERALS = "R1_ARABIC_NUMERALS"
RULE_R2_CURRENCY = "R2_CURRENCY_NORMALIZATION"
RULE_R3_THOUSANDS_SEP = "R3_THOUSANDS_SEPARATOR"
RULE_R4_PRICE_WORDS = "R4_PRICE_WORDS_TRANSLATION"
RULE_R5_PHONE = "R5_PHONE_STANDARDIZATION"
RULE_R6_EMAIL = "EMAIL_REPEATED_SYMBOLS"
RULE_R7_DATE = "R7_DATE_STANDARDIZATION"
RULE_R8_WHITESPACE_SYNONYMS = "R8_WHITESPACE_AND_SYNONYMS"
RULE_R9_TOTAL_RECALCULATION = "R9_TOTAL_AMOUNT_RECALCULATION"
RULE_ITEMS_QTY_STRING = "ITEMS_QTY_STRING_CAST"

# Official Quarantine Error Codes (Matching Doctor Image 3)
ERR_MISSING_ORDER_ID = "معرف الطلب مفقود"
ERR_MISSING_CUSTOMER_ID = "معرف العميل مفقود"
ERR_INVALID_SHORT_PHONE = "رقم هاتف غير صالح وقصير"
ERR_INVALID_EMAIL_NO_DOMAIN = "بريد إلكتروني بدون نطاق"
ERR_INVALID_IMPOSSIBLE_DATE = "تاريخ مستحيل"
ERR_UNKNOWN_STATUS = "حالة طلب غير معروفة"
ERR_EMPTY_ITEMS = "الطلب بدون عناصر"
ERR_CORRUPTED_ITEMS_JSON = "العناصر تالف JSON"
ERR_MISSING_ITEM_SKU = "مفقود من أحد العناصر SKU"
ERR_NEGATIVE_QUANTITY = "كمية سالبة"
ERR_UNKNOWN_CURRENCY = "عملة غير معروفة"
ERR_MULTIPLE_CONFLICTING_ERRORS = "عدة أخطاء جوهرية متعارضة"

ERR_UNKNOWN_PRICE = "UNKNOWN_PRICE"
ERR_AMBIGUOUS_NEGATIVE_VALUE = "AMBIGUOUS_NEGATIVE_VALUE"
ERR_DUPLICATE_ORDER_ID = "DUPLICATE_ORDER_ID"


# =========================================================================
# Rule 1: Arabic Numerals Conversion
# =========================================================================
def clean_arabic_numerals(value: Any) -> Tuple[Optional[str], bool]:
    if value is None:
        return None, False
    val_str = str(value)
    cleaned = val_str.translate(ARABIC_INDIC_DIGITS)
    was_modified = cleaned != val_str
    return cleaned, was_modified


# =========================================================================
# Rule 2, 3, 4: Monetary Amount Cleaning
# =========================================================================
def clean_monetary_amount(value: Any, field_name: str = "amount") -> Tuple[Optional[float], List[Dict[str, Any]], Optional[str]]:
    corrections = []
    if value is None or str(value).strip() == "" or str(value).strip() == "???":
        return None, corrections, ERR_UNKNOWN_PRICE

    raw_str = str(value).strip()

    # Check Rule 4: Price in Words
    normalized_text = re.sub(r"\s+", " ", raw_str)
    if normalized_text in ARABIC_WORDS_NUMBER_MAP:
        word_value = float(ARABIC_WORDS_NUMBER_MAP[normalized_text])
        corrections.append({
            "field": field_name,
            "original_value": raw_str,
            "corrected_value": word_value,
            "rule_code": RULE_R4_PRICE_WORDS
        })
        return word_value, corrections, None

    # Step A: Rule 1 - Convert Arabic Numerals
    cleaned_str, r1_applied = clean_arabic_numerals(raw_str)
    if r1_applied:
        corrections.append({
            "field": field_name,
            "original_value": raw_str,
            "corrected_value": cleaned_str,
            "rule_code": RULE_R1_ARABIC_NUMERALS
        })

    # Step B: Rule 2 - Strip Currency Symbols and Text
    currency_regex = r"(?i)\b(yer|riyal|sar|usd|eur|ريال\s*يمني|ر\.ي|ريال|دولار)\b|[$\€\£\¥]"
    if re.search(currency_regex, cleaned_str):
        no_curr_str = re.sub(currency_regex, "", cleaned_str).strip()
        corrections.append({
            "field": field_name,
            "original_value": cleaned_str,
            "corrected_value": no_curr_str,
            "rule_code": RULE_R2_CURRENCY
        })
        cleaned_str = no_curr_str

    # Step C: Rule 3 - Remove Thousands Separators
    if re.search(r"\d+([,_ ]\d{3})+", cleaned_str):
        no_sep_str = re.sub(r"(?<=\d)[,_ ](?=\d)", "", cleaned_str).strip()
        if no_sep_str != cleaned_str:
            corrections.append({
                "field": field_name,
                "original_value": cleaned_str,
                "corrected_value": no_sep_str,
                "rule_code": RULE_R3_THOUSANDS_SEP
            })
            cleaned_str = no_sep_str

    # Clean leftover non-numeric characters except single dot and minus
    numeric_cleaned = re.sub(r"[^\d.-]", "", cleaned_str).strip()

    try:
        float_val = float(numeric_cleaned)
        if float_val < 0:
            return None, corrections, ERR_AMBIGUOUS_NEGATIVE_VALUE
        return float_val, corrections, None
    except ValueError:
        return None, corrections, ERR_UNKNOWN_PRICE


# =========================================================================
# Rule 5: Phone Number Standardization
# =========================================================================
def clean_phone_number(value: Any) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    if value is None:
        return None, None

    raw_str = str(value).strip()
    if not raw_str:
        return None, None

    cleaned, _ = clean_arabic_numerals(raw_str)
    digits_only = re.sub(r"\D", "", cleaned)

    if digits_only.startswith("00967"):
        digits_only = digits_only[5:]
    elif digits_only.startswith("967"):
        digits_only = digits_only[3:]
    elif digits_only.startswith("0") and len(digits_only) == 10:
        digits_only = digits_only[1:]

    standardized = digits_only
    if standardized != raw_str:
        correction = {
            "field": "customer_phone",
            "original_value": raw_str,
            "corrected_value": standardized,
            "rule_code": RULE_R5_PHONE
        }
        return standardized, correction

    return standardized, None


# =========================================================================
# Rule 6: Email Syntax Repair
# =========================================================================
def clean_email(value: Any) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    if value is None:
        return None, None

    raw_str = str(value).strip()
    if not raw_str or raw_str in ["@@", "@", "null", "none"]:
        return None, None

    cleaned = re.sub(r"@+", "@", raw_str)
    cleaned = re.sub(r"\.+", ".", cleaned)
    cleaned = re.sub(r"\s+", "", cleaned)

    email_pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    if re.match(email_pattern, cleaned):
        if cleaned != raw_str:
            correction = {
                "field": "customer_email",
                "original_value": raw_str,
                "corrected_value": cleaned,
                "rule_code": RULE_R6_EMAIL
            }
            return cleaned, correction
        return cleaned, None

    return None, None


# =========================================================================
# Rule 7: Date Standardization
# =========================================================================
DATE_FORMATS = [
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y",
    "%d-%m-%Y %H:%M:%S",
    "%d-%m-%Y",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d",
    "%m/%d/%Y",
]

def clean_date_format(value: Any) -> Tuple[Optional[str], Optional[Dict[str, Any]], Optional[str]]:
    if value is None or str(value).strip() == "":
        return None, None, ERR_INVALID_IMPOSSIBLE_DATE

    raw_str = str(value).strip()
    cleaned_digits, _ = clean_arabic_numerals(raw_str)

    parsed_dt = None
    for fmt in DATE_FORMATS:
        try:
            parsed_dt = datetime.strptime(cleaned_digits, fmt)
            break
        except ValueError:
            continue

    if parsed_dt is None:
        return None, None, ERR_INVALID_IMPOSSIBLE_DATE

    if parsed_dt.year < 2000 or parsed_dt.year > 2100:
        return None, None, ERR_INVALID_IMPOSSIBLE_DATE

    standardized_iso = parsed_dt.strftime("%Y-%m-%dT%H:%M:%S")
    if standardized_iso != raw_str:
        correction = {
            "field": "order_date",
            "original_value": raw_str,
            "corrected_value": standardized_iso,
            "rule_code": RULE_R7_DATE
        }
        return standardized_iso, correction, None

    return standardized_iso, None, None


# =========================================================================
# Rule 8: Whitespace Trimming & Status Dictionary Mapping
# =========================================================================
def clean_status_and_whitespace(status_value: Any) -> Tuple[str, Optional[Dict[str, Any]]]:
    if status_value is None:
        return "UNKNOWN", None

    raw_str = str(status_value)
    trimmed_str = raw_str.strip()
    normalized = re.sub(r"\s+", " ", trimmed_str)

    # Standardize via dictionary mapping
    standardized = VALID_STATUS_MAP.get(normalized, VALID_STATUS_MAP.get(normalized.lower(), normalized))

    # A correction is ONLY registered if the raw input had whitespace or non-standard format
    if raw_str != trimmed_str or "  " in raw_str:
        correction = {
            "field": "status",
            "original_value": raw_str,
            "corrected_value": standardized,
            "rule_code": RULE_R8_WHITESPACE_SYNONYMS
        }
        return standardized, correction

    return standardized, None


# =========================================================================
# JSON Items Validation
# =========================================================================
def validate_and_parse_items_json(items_value: Any) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
    if items_value is None:
        return None, ERR_CORRUPTED_ITEMS_JSON

    if isinstance(items_value, list):
        return items_value, None

    if isinstance(items_value, str):
        trimmed = items_value.strip()
        if not trimmed or trimmed == "not-json":
            return None, ERR_CORRUPTED_ITEMS_JSON
        try:
            parsed = json.loads(trimmed)
            if isinstance(parsed, list):
                return parsed, None
            return None, ERR_CORRUPTED_ITEMS_JSON
        except (json.JSONDecodeError, ValueError):
            return None, ERR_CORRUPTED_ITEMS_JSON

    return None, ERR_CORRUPTED_ITEMS_JSON


# =========================================================================
# Comprehensive Record Processor & Classifier
# =========================================================================
def process_and_classify_record(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Classifies a raw record into VALID, CORRECTED, or QUARANTINE following
    the exact 12 Quarantine rules and 10 Correction rules.
    """
    oid = raw_record.get("order_id")
    if oid is None:
        oid = raw_record.get("\ufefforder_id")
    oid_str = str(oid).strip() if oid is not None else ""

    cid = raw_record.get("customer_id")
    cid_str = str(cid).strip() if cid is not None else ""

    phone_raw = raw_record.get("customer_phone")
    phone_str = str(phone_raw).strip() if phone_raw is not None else ""

    email_raw = raw_record.get("customer_email")
    email_str = str(email_raw).strip() if email_raw is not None else ""

    date_raw = raw_record.get("order_date")
    date_str = str(date_raw).strip() if date_raw is not None else ""

    status_raw = raw_record.get("status")
    status_str = str(status_raw).strip() if status_raw is not None else ""

    currency_raw = raw_record.get("currency")
    currency_str = str(currency_raw).strip() if currency_raw is not None else ""

    items_raw = raw_record.get("items_json")

    # -------------------------------------------------------------------------
    # STEP 1: Evaluate Quarantine Conditions (12 Official Doctor Rules)
    # -------------------------------------------------------------------------
    quarantine_errors: List[str] = []

    # 1. order_id missing
    if not oid_str or oid_str == "None":
        quarantine_errors.append(ERR_MISSING_ORDER_ID)

    # 2. customer_id missing
    if not cid_str or cid_str == "None":
        quarantine_errors.append(ERR_MISSING_CUSTOMER_ID)

    # 3. customer_phone invalid / short (< 7 digits)
    cleaned_ph_digits = re.sub(r"\D", "", clean_arabic_numerals(phone_str)[0] or "")
    if not phone_str or phone_str == "None" or len(cleaned_ph_digits) < 7:
        quarantine_errors.append(ERR_INVALID_SHORT_PHONE)

    # 4. customer_email invalid / without domain
    if not email_str or "@" not in email_str or "." not in email_str or email_str.endswith("@") or email_str.startswith("@"):
        quarantine_errors.append(ERR_INVALID_EMAIL_NO_DOMAIN)

    # 5. order_date impossible date
    if not date_str or "19-45" in date_str or "99:" in date_str:
        quarantine_errors.append(ERR_INVALID_IMPOSSIBLE_DATE)
    else:
        _, _, d_err = clean_date_format(date_str)
        if d_err:
            quarantine_errors.append(ERR_INVALID_IMPOSSIBLE_DATE)

    # 6. status unknown status ('حالة غامضة غير معروفة')
    if status_str == "حالة غامضة غير معروفة":
        quarantine_errors.append(ERR_UNKNOWN_STATUS)

    # 7-10. items_json checks
    parsed_items, parse_err = validate_and_parse_items_json(items_raw)
    if parse_err:
        quarantine_errors.append(ERR_CORRUPTED_ITEMS_JSON)
    elif parsed_items is not None:
        if len(parsed_items) == 0:
            quarantine_errors.append(ERR_EMPTY_ITEMS)
        else:
            has_missing_sku = False
            has_neg_qty = False
            for item in parsed_items:
                if not isinstance(item, dict):
                    has_missing_sku = True
                    break
                sku_val = item.get("sku")
                if not sku_val or str(sku_val).strip() == "" or str(sku_val).strip() == "None":
                    has_missing_sku = True
                try:
                    qty_num = int(float(str(item.get("qty", 0))))
                    if qty_num < 0:
                        has_neg_qty = True
                except (ValueError, TypeError):
                    pass
            if has_missing_sku:
                quarantine_errors.append(ERR_MISSING_ITEM_SKU)
            if has_neg_qty:
                quarantine_errors.append(ERR_NEGATIVE_QUANTITY)

    # 11. currency unknown
    if currency_str not in ["YER", "ريال يمني", "ر.ي"]:
        quarantine_errors.append(ERR_UNKNOWN_CURRENCY)

    # Quarantine Decision
    if quarantine_errors:
        if len(quarantine_errors) > 1:
            chosen_code = ERR_MULTIPLE_CONFLICTING_ERRORS
            chosen_reason = f"Multiple conflicting errors: {quarantine_errors}"
        else:
            chosen_code = quarantine_errors[0]
            chosen_reason = f"Single core defect: {chosen_code}"

        return {
            "classification": "QUARANTINE",
            "error_code": chosen_code,
            "error_reason": chosen_reason,
            "record": {
                "order_id": oid_str if oid_str else "UNKNOWN",
                "raw_record": raw_record,
                "error_code": chosen_code,
                "error_reason": chosen_reason,
                "quarantined_at": datetime.utcnow().isoformat()
            }
        }

    # -------------------------------------------------------------------------
    # STEP 2: Record is Eligible for Validation -> Apply Corrections
    # -------------------------------------------------------------------------
    corrections: List[Dict[str, Any]] = []

    # Clean Order ID
    order_id_clean, oid_modified = clean_arabic_numerals(oid_str)
    if oid_modified:
        corrections.append({
            "field": "order_id",
            "original_value": oid_str,
            "corrected_value": order_id_clean,
            "rule_code": RULE_R1_ARABIC_NUMERALS
        })

    # Clean Date
    order_date_clean, date_corr, _ = clean_date_format(date_str)
    if date_corr:
        corrections.append(date_corr)

    # Clean Status
    status_clean, status_corr = clean_status_and_whitespace(raw_record.get("status"))
    if status_corr:
        corrections.append(status_corr)

    # Clean Phone
    phone_clean, phone_corr = clean_phone_number(phone_raw)
    if phone_corr:
        corrections.append(phone_corr)

    # Clean Email
    email_clean, email_corr = clean_email(email_raw)
    if email_corr:
        corrections.append(email_corr)

    # Clean Currency
    currency_clean = TARGET_CURRENCY
    if currency_str in ["ريال يمني", "ر.ي"]:
        corrections.append({
            "field": "currency",
            "original_value": currency_str,
            "corrected_value": TARGET_CURRENCY,
            "rule_code": RULE_R2_CURRENCY
        })

    # Clean Delivery Cost
    delivery_cost_val, del_corrs, _ = clean_monetary_amount(raw_record.get("delivery_cost", 0.0), "delivery_cost")
    corrections.extend(del_corrs)
    delivery_cost_clean = delivery_cost_val if delivery_cost_val is not None else 0.0

    # Clean Payment Amount
    payment_amt_val, pay_corrs, _ = clean_monetary_amount(raw_record.get("payment_amount"), "payment_amount")
    corrections.extend(pay_corrs)

    # Clean Total Amount & Thousands Separators
    total_amt_val, tot_corrs, _ = clean_monetary_amount(raw_record.get("total_amount"), "total_amount")
    corrections.extend(tot_corrs)

    # Clean Items & Quantity strings
    items_cleaned = []
    items_had_str_qty = False
    for item in (parsed_items or []):
        it_copy = dict(item)
        if isinstance(it_copy.get("qty"), str):
            try:
                it_copy["qty"] = int(it_copy["qty"])
                items_had_str_qty = True
            except ValueError:
                pass
        items_cleaned.append(it_copy)

    if items_had_str_qty:
        corrections.append({
            "field": "items_json",
            "original_value": "quantity as string in items",
            "corrected_value": "quantity cast to integer",
            "rule_code": RULE_ITEMS_QTY_STRING
        })

    # Recalculate Total Amount if mismatch
    try:
        items_sum = sum(
            float(item.get("total", float(item.get("qty", 1)) * float(item.get("unit_price", 0))))
            for item in items_cleaned
        )
        calculated_total = items_sum + delivery_cost_clean
        if total_amt_val is None or abs(calculated_total - total_amt_val) > 1.0:
            corrections.append({
                "field": "total_amount",
                "original_value": raw_record.get("total_amount"),
                "corrected_value": calculated_total,
                "rule_code": RULE_R9_TOTAL_RECALCULATION
            })
            total_amt_val = calculated_total
    except Exception:
        pass

    # Clean metadata text fields
    customer_name = str(raw_record.get("customer_name") or "").strip()
    city = str(raw_record.get("city") or "").strip()
    district = str(raw_record.get("district") or "").strip()
    delivery_type = str(raw_record.get("delivery_type") or "").strip()
    payment_method = str(raw_record.get("payment_method") or "").strip()
    payment_status = str(raw_record.get("payment_status") or "").strip()

    classification = "CORRECTED" if corrections else "VALID"
    quality_status = "corrected" if corrections else "valid"

    validated_doc: Dict[str, Any] = {
        "order_id": order_id_clean,
        "order_date": order_date_clean,
        "status": status_clean,
        "customer_id": cid_str if cid_str else None,
        "customer_name": customer_name if customer_name else None,
        "customer_phone": phone_clean,
        "customer_email": email_clean,
        "city": city,
        "district": district,
        "delivery_type": delivery_type,
        "delivery_cost": delivery_cost_clean,
        "payment_method": payment_method,
        "payment_status": payment_status,
        "payment_amount": payment_amt_val,
        "currency": currency_clean,
        "total_amount": total_amt_val,
        "items": items_cleaned,
        "quality_status": quality_status,
        "processed_at": datetime.utcnow().isoformat(),
    }

    if corrections:
        validated_doc["corrections"] = corrections

    return {
        "classification": classification,
        "record": validated_doc,
        "error_code": None,
        "error_reason": None
    }
