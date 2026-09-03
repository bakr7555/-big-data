"""
test_classification.py - Comprehensive Unit tests for Record Processing, Quarantine & Audit Trail
"""

import unittest
import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.quality_rules import (
    process_and_classify_record,
    ERR_MISSING_ORDER_ID,
    ERR_CORRUPTED_ITEMS_JSON,
    ERR_EMPTY_ITEMS,
    ERR_INVALID_IMPOSSIBLE_DATE,
    ERR_UNKNOWN_PRICE,
    ERR_AMBIGUOUS_NEGATIVE_VALUE,
    RULE_R9_TOTAL_RECALCULATION,
)


class TestClassification(unittest.TestCase):

    def test_valid_clean_record(self):
        """A clean record should be classified as VALID without corrections."""
        raw = {
            "order_id": "ORD-100",
            "order_date": "2025-02-24T21:29:00",
            "status": "CONFIRMED",
            "customer_id": "CUST-1",
            "customer_name": "محمد علي",
            "customer_phone": "702390941",
            "customer_email": "user@example.com",
            "city": "تعز",
            "district": "شعوب",
            "delivery_type": "سريع",
            "delivery_cost": "5000.0",
            "payment_method": "محفظة إلكترونية",
            "payment_status": "تم الدفع",
            "payment_amount": "769000.0",
            "currency": "YER",
            "total_amount": "769000.0",
            "items_json": '[{"sku":"SKU-1010","name":"هاتف سامسونج","qty":1,"unit_price":764000.0,"total":764000.0}]'
        }
        result = process_and_classify_record(raw)
        self.assertEqual(result["classification"], "VALID")
        self.assertEqual(result["record"]["quality_status"], "valid")
        self.assertNotIn("corrections", result["record"])
        self.assertEqual(result["record"]["order_id"], "ORD-100")

    def test_corrected_record_audit_trail(self):
        """A dirty record with Arabic numbers and messy email should be CORRECTED with audit trail."""
        raw = {
            "order_id": "طلب-200",
            "order_date": "31/01/2025",
            "status": "  مؤكد  ",
            "customer_id": "عميل-1",
            "customer_name": "محمد علي",
            "customer_phone": "+967 77 123 4567",
            "customer_email": "user@@mail..com",
            "city": "صنعاء",
            "district": "التحرير",
            "delivery_type": "سريع",
            "delivery_cost": "ألفان",
            "payment_method": "محفظة إلكترونية",
            "payment_status": "تم الدفع",
            "payment_amount": "٧٦٩٠٠٠٫٠",
            "currency": "ريال يمني",
            "total_amount": "769,000 YER",
            "items_json": '[{"sku":"SKU-1010","name":"هاتف سامسونج","qty":1,"unit_price":767000.0,"total":767000.0}]'
        }
        result = process_and_classify_record(raw)
        self.assertEqual(result["classification"], "CORRECTED")
        self.assertEqual(result["record"]["quality_status"], "corrected")
        rec = result["record"]
        self.assertIn("corrections", rec)
        self.assertGreaterEqual(len(rec["corrections"]), 5)

        # Verify audit trail structure
        for corr in rec["corrections"]:
            self.assertIn("field", corr)
            self.assertIn("original_value", corr)
            self.assertIn("corrected_value", corr)
            self.assertIn("rule_code", corr)

    def test_recalculate_total_from_items_and_delivery(self):
        """If total_amount is corrupted ('???') but items and delivery are valid, recalculate total."""
        raw = {
            "order_id": "ORD-500",
            "order_date": "2025-02-24T21:29:00",
            "status": "CONFIRMED",
            "delivery_cost": "2000.0",
            "payment_amount": "27000.0",
            "total_amount": "???",
            "items_json": '[{"sku":"SKU-1","name":"شاحن","qty":2,"unit_price":12500.0,"total":25000.0}]'
        }
        result = process_and_classify_record(raw)
        self.assertEqual(result["classification"], "CORRECTED")
        self.assertEqual(result["record"]["total_amount"], 27000.0) # 25000 + 2000
        self.assertTrue(any(c["rule_code"] == RULE_R9_TOTAL_RECALCULATION for c in result["record"]["corrections"]))

    def test_quarantine_missing_order_id(self):
        """Missing order_id must go to quarantine."""
        raw = {
            "order_id": "",
            "order_date": "2025-02-24T21:29:00",
            "items_json": '[{"sku":"SKU-1","name":"item","qty":1,"unit_price":100,"total":100}]'
        }
        result = process_and_classify_record(raw)
        self.assertEqual(result["classification"], "QUARANTINE")
        self.assertEqual(result["error_code"], ERR_MISSING_ORDER_ID)

    def test_quarantine_empty_items(self):
        """Empty items list must go to quarantine."""
        raw = {
            "order_id": "ORD-301",
            "order_date": "2025-02-24T21:29:00",
            "items_json": "[]"
        }
        result = process_and_classify_record(raw)
        self.assertEqual(result["classification"], "QUARANTINE")
        self.assertEqual(result["error_code"], ERR_EMPTY_ITEMS)

    def test_quarantine_corrupted_json(self):
        """Corrupted items_json ('not-json') must go to quarantine."""
        raw = {
            "order_id": "ORD-300",
            "order_date": "2025-02-24T21:29:00",
            "items_json": "not-json"
        }
        result = process_and_classify_record(raw)
        self.assertEqual(result["classification"], "QUARANTINE")
        self.assertEqual(result["error_code"], ERR_CORRUPTED_ITEMS_JSON)

    def test_quarantine_impossible_date(self):
        """Impossible year date must go to quarantine."""
        raw = {
            "order_id": "ORD-400",
            "order_date": "1800-01-01",
            "items_json": '[{"sku":"SKU-1","name":"item","qty":1,"unit_price":100,"total":100}]'
        }
        result = process_and_classify_record(raw)
        self.assertEqual(result["classification"], "QUARANTINE")
        self.assertEqual(result["error_code"], ERR_INVALID_IMPOSSIBLE_DATE)


if __name__ == "__main__":
    unittest.main()
