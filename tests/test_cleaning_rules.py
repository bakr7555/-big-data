"""
test_cleaning_rules.py - Comprehensive Unit tests for the 8 Mandatory Auto-Cleaning Rules
"""

import unittest
import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.quality_rules import (
    clean_arabic_numerals,
    clean_monetary_amount,
    clean_phone_number,
    clean_email,
    clean_date_format,
    clean_status_and_whitespace,
    RULE_R1_ARABIC_NUMERALS,
    RULE_R2_CURRENCY,
    RULE_R3_THOUSANDS_SEP,
    RULE_R4_PRICE_WORDS,
    RULE_R5_PHONE,
    RULE_R6_EMAIL,
    RULE_R7_DATE,
    RULE_R8_WHITESPACE_SYNONYMS,
)


class TestCleaningRules(unittest.TestCase):

    def test_rule_1_arabic_numerals_conversion(self):
        """Rule 1: Convert Eastern Arabic numerals (٠-٩) and Arabic decimal comma (٫) to ASCII digits."""
        cleaned, modified = clean_arabic_numerals("٧٠٦٠٠٠٫٠")
        self.assertTrue(modified)
        self.assertEqual(cleaned, "706000.0")

        cleaned, modified = clean_arabic_numerals("٥٠٠٠")
        self.assertTrue(modified)
        self.assertEqual(cleaned, "5000")

        cleaned, modified = clean_arabic_numerals("12345")
        self.assertFalse(modified)
        self.assertEqual(cleaned, "12345")

    def test_rule_2_currency_normalization(self):
        """Rule 2: Strip currency symbols/names and standardize."""
        amt, corrs, err = clean_monetary_amount("769000 YER", "total_amount")
        self.assertIsNone(err)
        self.assertEqual(amt, 769000.0)
        self.assertTrue(any(c["rule_code"] == RULE_R2_CURRENCY for c in corrs))

        amt, corrs, err = clean_monetary_amount("5000 ريال يمني", "payment_amount")
        self.assertIsNone(err)
        self.assertEqual(amt, 5000.0)

        amt, corrs, err = clean_monetary_amount("5000 ر.ي", "payment_amount")
        self.assertIsNone(err)
        self.assertEqual(amt, 5000.0)

    def test_rule_3_thousands_separators(self):
        """Rule 3: Remove thousands separators (commas, spaces, underscores)."""
        amt, corrs, err = clean_monetary_amount("125,000.00", "total_amount")
        self.assertIsNone(err)
        self.assertEqual(amt, 125000.00)
        self.assertTrue(any(c["rule_code"] == RULE_R3_THOUSANDS_SEP for c in corrs))

        amt, corrs, err = clean_monetary_amount("1,250,000.50", "total_amount")
        self.assertIsNone(err)
        self.assertEqual(amt, 1250000.50)

    def test_rule_4_price_in_words(self):
        """Rule 4: Convert known Arabic words for prices."""
        amt, corrs, err = clean_monetary_amount("ألفان", "delivery_cost")
        self.assertIsNone(err)
        self.assertEqual(amt, 2000.0)
        self.assertTrue(any(c["rule_code"] == RULE_R4_PRICE_WORDS for c in corrs))

        amt, corrs, err = clean_monetary_amount("خمسة آلاف", "delivery_cost")
        self.assertIsNone(err)
        self.assertEqual(amt, 5000.0)

        amt, corrs, err = clean_monetary_amount("عشرة آلاف", "total_amount")
        self.assertIsNone(err)
        self.assertEqual(amt, 10000.0)

    def test_rule_5_phone_standardization(self):
        """Rule 5: Phone number standardization into 9 digits."""
        phone, corr = clean_phone_number("+967 77 123 4567")
        self.assertEqual(phone, "771234567")
        self.assertIsNotNone(corr)
        self.assertEqual(corr["rule_code"], RULE_R5_PHONE)

        phone, corr = clean_phone_number(" 702 390 941 ")
        self.assertEqual(phone, "702390941")

        phone_intl, corr_intl = clean_phone_number("00967739988747")
        self.assertEqual(phone_intl, "739988747")

    def test_rule_6_email_syntax_repair(self):
        """Rule 6: Email syntax repair (repeated symbols like @@, ..)."""
        email, corr = clean_email("user@@mail..com")
        self.assertEqual(email, "user@mail.com")
        self.assertIsNotNone(corr)
        self.assertEqual(corr["rule_code"], RULE_R6_EMAIL)

        email2, corr2 = clean_email("john.doe@@example...com")
        self.assertEqual(email2, "john.doe@example.com")

    def test_rule_7_date_standardization(self):
        """Rule 7: Date formats standardization into ISO format."""
        # Slash format DD/MM/YYYY
        d1, corr1, err1 = clean_date_format("31/01/2025")
        self.assertIsNone(err1)
        self.assertTrue(d1.startswith("2025-01-31"))
        self.assertIsNotNone(corr1)
        self.assertEqual(corr1["rule_code"], RULE_R7_DATE)

        # Dash format YYYY-MM-DD
        d2, corr2, err2 = clean_date_format("2025-01-31")
        self.assertIsNone(err2)
        self.assertTrue(d2.startswith("2025-01-31"))

        # ISO timestamp format
        d3, corr3, err3 = clean_date_format("2025-02-24T21:29:00")
        self.assertIsNone(err3)
        self.assertEqual(d3, "2025-02-24T21:29:00")
        self.assertIsNone(corr3)

    def test_rule_8_whitespace_and_status_synonyms(self):
        """Rule 8: Trim whitespaces and standardize order statuses."""
        s1, corr1 = clean_status_and_whitespace("  مؤكد  ")
        self.assertEqual(s1, "CONFIRMED")
        self.assertIsNotNone(corr1)
        self.assertEqual(corr1["rule_code"], RULE_R8_WHITESPACE_SYNONYMS)

        s2, corr2 = clean_status_and_whitespace("تم الدفع")
        self.assertEqual(s2, "PAID")

        s3, corr3 = clean_status_and_whitespace("قيد الانتظار")
        self.assertEqual(s3, "PENDING")

        s4, corr4 = clean_status_and_whitespace("تم التسليم")
        self.assertEqual(s4, "DELIVERED")

        s5, corr5 = clean_status_and_whitespace("ملغي")
        self.assertEqual(s5, "CANCELLED")


if __name__ == "__main__":
    unittest.main()
