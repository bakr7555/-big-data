"""
quality_rules.py - Data Transformation, Quality Auditing & Quarantine Classification

Implements the 8 Mandatory Auto-Cleaning Rules:
  1. Arabic Numerals (R1_ARABIC_NUMERALS): '٠١٢٣٤٥٦٧٨٩' -> '0123456789' and numeric casting.
  2. Currency Normalization (R2_CURRENCY_NORMALIZATION): Strip symbols/text, standardize to 'YER'.
  3. Thousands Separators (R3_THOUSANDS_SEPARATOR): Remove ',' / '_' / spaces in numbers.
  4. Price in Words (R4_PRICE_WORDS_TRANSLATION): Convert known Arabic price words (e.g. 'ألفان' -> 2000).
  5. Phone Number Standardization (R5_PHONE_STANDARDIZATION): Clean spaces, standardize to 9-digit format.
  6. Email Syntax Repair (R6_EMAIL_SYNTAX_REPAIR): Fix '@@', '..', leading/trailing garbage.
  7. Date Standardization (R7_DATE_STANDARDIZATION): Unify formats ('31/01/2025', ISO, etc.) to ISO 8601.
  8. Whitespace & Synonyms (R8_WHITESPACE_AND_SYNONYMS): Trim extra whitespace and map statuses via standard dictionary.

Maintains strict audit trail with `corrections` object and classifies uncorrectable records into quarantine.
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

# Official Quarantine Error Codes (Page 5, Section 6.8)
ERR_MISSING_ORDER_ID = "MISSING_ORDER_ID"
ERR_MISSING_CUSTOMER_ID = "MISSING_CUSTOMER_ID"
ERR_INVALID_IMPOSSIBLE_DATE = "INVALID_IMPOSSIBLE_DATE"
ERR_CORRUPTED_ITEMS_JSON = "CORRUPTED_ITEMS_JSON"
ERR_EMPTY_ITEMS = "EMPTY_ITEMS"
ERR_UNKNOWN_PRICE = "UNKNOWN_PRICE"
ERR_AMBIGUOUS_NEGATIVE_VALUE = "AMBIGUOUS_NEGATIVE_VALUE"
ERR_DUPLICATE_ORDER_ID = "DUPLICATE_ORDER_ID"
ERR_MULTIPLE_CONFLICTING_ERRORS = "MULTIPLE_CONFLICTING_ERRORS"
ERR_CORRUPTED_RECORD = "CORRUPTED_RECORD"


# =========================================================================
# Rule 1: Arabic Numerals Conversion
# =========================================================================
def clean_arabic_numerals(value: Any) -> Tuple[Optional[str], bool]:
    """
    Converts Eastern Arabic/Indic numerals ('٠١٢٣٤٥٦٧٨٩') and Arabic decimal separator ('٫') to ASCII.
    Returns (cleaned_str, was_corrected).
    """
    if value is None:
        return None, False
    val_str = str(value)
    cleaned = val_str.translate(ARABIC_INDIC_DIGITS)
    was_modified = cleaned != val_str
    return cleaned, was_modified


# =========================================================================
# Rule 2 & 3 & 4: Monetary Amount Cleaning (Words, Currency, Thousands Separator)
# =========================================================================
def clean_monetary_amount(value: Any, field_name: str = "amount") -> Tuple[Optional[float], List[Dict[str, Any]], Optional[str]]:
    """
    Applies R1, R2, R3, R4 to convert raw amount into a valid float.
    Returns: (cleaned_float, list_of_corrections, error_code_if_failed)
    """
    corrections = []
    if value is None or str(value).strip() == "" or str(value).strip() == "???":
        return None, corrections, ERR_UNKNOWN_PRICE

    raw_str = str(value).strip()

    # Check Rule 4: Price in Words (exact match or known phrase)
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

    # Step C: Rule 3 - Remove Thousands Separators (commas, underscores, spaces within digits)
    # Handle patterns like 1,000,000 or 1 000 000
    if re.search(r"\d+([,_ ]\d{3})+", cleaned_str):
        # Remove commas, underscores, and spacing inside numbers
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
    """
    Standardizes Yemeni phone numbers:
    - Removes spaces, dashes, brackets, country codes (+967, 00967).
    - Normalizes to 9-digit format (e.g. 702390941, 777123456, 712345678, 732345678).
    """
    if value is None:
        return None, None

    raw_str = str(value).strip()
    if not raw_str:
        return None, None

    # Apply Arabic numerals conversion first
    cleaned, _ = clean_arabic_numerals(raw_str)

    # Strip non-digits
    digits_only = re.sub(r"\D", "", cleaned)

    # Remove Yemen country code prefix 967 or 00967
    if digits_only.startswith("00967"):
        digits_only = digits_only[5:]
    elif digits_only.startswith("967"):
        digits_only = digits_only[3:]
    elif digits_only.startswith("0") and len(digits_only) == 10:
        digits_only = digits_only[1:]

    # A standard Yemeni mobile number is 9 digits (starts with 7) or local 9 digits
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
    """
    Repairs common email syntax errors:
    - Multiple '@' signs (e.g., 'user@@mail.com' -> 'user@mail.com').
    - Multiple dots in domain (e.g., 'user@mail..com' -> 'user@mail.com').
    - Leading/trailing whitespace or invalid punctuation.
    """
    if value is None:
        return None, None

    raw_str = str(value).strip()
    if not raw_str or raw_str in ["@@", "@", "null", "none"]:
        return None, None

    # Replace repeated '@' with a single '@'
    cleaned = re.sub(r"@+", "@", raw_str)

    # Replace multiple dots with a single dot
    cleaned = re.sub(r"\.+", ".", cleaned)

    # Remove spaces
    cleaned = re.sub(r"\s+", "", cleaned)

    # Validate basic email regex: username@domain.tld
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

    # If email is totally unrepairable, return None without failing the whole order
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
    """
    Standardizes varied date formats into ISO format 'YYYY-MM-DDTHH:MM:SS' or 'YYYY-MM-DD'.
    Checks for impossible dates (year < 2000 or year > 2100).
    """
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

    # Check for impossible year
    if parsed_dt.year < 2000 or parsed_dt.year > 2100:
        return None, None, ERR_INVALID_IMPOSSIBLE_DATE

    # Standardize to ISO string
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
    """
    Trims extra whitespaces and maps order status synonyms to standard status values.
    """
    if status_value is None:
        return "UNKNOWN", None

    raw_str = str(status_value).strip()
    normalized = re.sub(r"\s+", " ", raw_str)
    normalized_lower = normalized.lower()

    standardized = VALID_STATUS_MAP.get(normalized, VALID_STATUS_MAP.get(normalized_lower, normalized))

    if standardized != raw_str:
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
    """
    Validates items_json structure.
    If string, attempts to parse as JSON. Must result in a valid list of item dicts.
    """
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
    Processes a single raw order record, applying the 8 cleaning rules,
    logging corrections, and classifying the result into VALID, CORRECTED, or QUARANTINE.

    Returns:
        {
            "classification": "VALID" | "CORRECTED" | "QUARANTINE",
            "record": dict,              # Validated/Corrected or Quarantine doc
            "error_code": Optional[str],
            "error_reason": Optional[str]
        }
    """
    corrections: List[Dict[str, Any]] = []

    # 1. Check Stable Business Key: order_id
    order_id = raw_record.get("order_id")
    if order_id is None:
        order_id = raw_record.get("\ufefforder_id")

    if order_id is None or str(order_id).strip() == "" or str(order_id).strip() == "None":
        return {
            "classification": "QUARANTINE",
            "error_code": ERR_MISSING_ORDER_ID,
            "error_reason": "order_id is missing or empty",
            "record": {
                "raw_record": raw_record,
                "error_code": ERR_MISSING_ORDER_ID,
                "error_reason": "order_id is missing or empty",
                "quarantined_at": datetime.utcnow().isoformat()
            }
        }

    order_id_clean, _ = clean_arabic_numerals(str(order_id).strip())

    # 2. Rule 7: Date Standardization
    order_date, date_corr, date_err = clean_date_format(raw_record.get("order_date"))
    if date_err:
        return {
            "classification": "QUARANTINE",
            "error_code": date_err,
            "error_reason": f"Unparseable or impossible order_date: {raw_record.get('order_date')}",
            "record": {
                "order_id": order_id_clean,
                "raw_record": raw_record,
                "error_code": date_err,
                "error_reason": f"Unparseable or impossible order_date: {raw_record.get('order_date')}",
                "quarantined_at": datetime.utcnow().isoformat()
            }
        }
    if date_corr:
        corrections.append(date_corr)

    # 3. Items JSON Validation
    items_list, items_err = validate_and_parse_items_json(raw_record.get("items_json"))
    if items_err:
        return {
            "classification": "QUARANTINE",
            "error_code": items_err,
            "error_reason": f"Corrupted items_json: {raw_record.get('items_json')}",
            "record": {
                "order_id": order_id_clean,
                "raw_record": raw_record,
                "error_code": items_err,
                "error_reason": f"Corrupted items_json: {raw_record.get('items_json')}",
                "quarantined_at": datetime.utcnow().isoformat()
            }
        }
    
    if len(items_list) == 0:
        return {
            "classification": "QUARANTINE",
            "error_code": ERR_EMPTY_ITEMS,
            "error_reason": "Items list is empty",
            "record": {
                "order_id": order_id_clean,
                "raw_record": raw_record,
                "error_code": ERR_EMPTY_ITEMS,
                "error_reason": "Items list is empty",
                "quarantined_at": datetime.utcnow().isoformat()
            }
        }

    # 4. Rule 8: Status & Whitespace Cleaning
    status_clean, status_corr = clean_status_and_whitespace(raw_record.get("status"))
    if status_corr:
        corrections.append(status_corr)

    # 5. Rule 5: Phone Number Standardization
    phone_clean, phone_corr = clean_phone_number(raw_record.get("customer_phone"))
    if phone_corr:
        corrections.append(phone_corr)

    # 6. Rule 6: Email Repair
    email_clean, email_corr = clean_email(raw_record.get("customer_email"))
    if email_corr:
        corrections.append(email_corr)

    # 7. Delivery Cost Cleaning
    delivery_cost_val, del_corrs, _ = clean_monetary_amount(raw_record.get("delivery_cost", 0.0), "delivery_cost")
    corrections.extend(del_corrs)
    delivery_cost_clean = delivery_cost_val if delivery_cost_val is not None else 0.0

    # 8. Rules 1-4 & Recalculation: Payment & Total Amount Cleaning
    payment_amt, pay_corrs, pay_err = clean_monetary_amount(raw_record.get("payment_amount"), "payment_amount")
    corrections.extend(pay_corrs)

    total_amt, tot_corrs, tot_err = clean_monetary_amount(raw_record.get("total_amount"), "total_amount")
    corrections.extend(tot_corrs)

    # If total_amount is corrupted but items are valid, recalculate total from items + delivery (Page 5 rule)
    if tot_err:
        try:
            items_sum = sum(
                float(item.get("total", float(item.get("qty", 1)) * float(item.get("unit_price", 0))))
                for item in items_list
            )
            if items_sum > 0:
                recalculated_total = items_sum + delivery_cost_clean
                corrections.append({
                    "field": "total_amount",
                    "original_value": raw_record.get("total_amount"),
                    "corrected_value": recalculated_total,
                    "rule_code": RULE_R9_TOTAL_RECALCULATION
                })
                total_amt = recalculated_total
                tot_err = None
        except Exception:
            pass

    # If critical price amounts are broken beyond repair, quarantine
    if pay_err or tot_err:
        err_code = pay_err or tot_err or ERR_UNKNOWN_PRICE
        return {
            "classification": "QUARANTINE",
            "error_code": err_code,
            "error_reason": f"Invalid payment_amount or total_amount: {raw_record.get('total_amount')}",
            "record": {
                "order_id": order_id_clean,
                "raw_record": raw_record,
                "error_code": err_code,
                "error_reason": f"Invalid payment_amount or total_amount: {raw_record.get('total_amount')}",
                "quarantined_at": datetime.utcnow().isoformat()
            }
        }

    # Clean Customer & Location String fields (Whitespace trimming)
    customer_name = str(raw_record.get("customer_name") or "").strip()
    customer_id = str(raw_record.get("customer_id") or "").strip()
    city = str(raw_record.get("city") or "").strip()
    district = str(raw_record.get("district") or "").strip()
    delivery_type = str(raw_record.get("delivery_type") or "").strip()
    payment_method = str(raw_record.get("payment_method") or "").strip()
    payment_status = str(raw_record.get("payment_status") or "").strip()

    # Classification: VALID vs CORRECTED
    if corrections:
        quality_status = "corrected"
        classification = "CORRECTED"
    else:
        quality_status = "valid"
        classification = "VALID"

    # Target validated document (Schema compliant with Section 6.7)
    validated_doc: Dict[str, Any] = {
        "order_id": order_id_clean,
        "order_date": order_date,
        "status": status_clean,
        "customer_id": customer_id if customer_id else None,
        "customer_name": customer_name if customer_name else None,
        "customer_phone": phone_clean,
        "customer_email": email_clean,
        "city": city,
        "district": district,
        "delivery_type": delivery_type,
        "delivery_cost": delivery_cost_clean,
        "payment_method": payment_method,
        "payment_status": payment_status,
        "payment_amount": payment_amt,
        "currency": TARGET_CURRENCY,
        "total_amount": total_amt,
        "items": items_list,
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
