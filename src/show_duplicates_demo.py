"""
show_duplicates_demo.py - Duplicate Detection & Consolidation Visualizer

Demonstrates how duplicate raw rows for the same `order_id` in `orders_raw`
are consolidated via Idempotent Upsert into a single clean record in `orders_validated`.
"""

import sys
from pathlib import Path
from pprint import pprint
from typing import Dict, Any, List

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    COLLECTION_RAW,
    COLLECTION_VALIDATED,
)
from src.mongo_setup import get_mongo_client, get_database


def analyze_and_show_duplicates(limit_examples: int = 2) -> None:
    """
    Finds duplicated order_ids in `orders_raw`, prints their raw state progression,
    and shows the resulting unified record in `orders_validated`.
    """
    print("\n" + "=" * 80)
    print(" 🔍 [DUPLICATE CONSOLIDATION AUDITOR] فحص وتحليل التكرار قبل وبعد الدمج")
    print("=" * 80)

    client = get_mongo_client()
    db = get_database(client)

    raw_col = db[COLLECTION_RAW]
    val_col = db[COLLECTION_VALIDATED]

    # 1. Aggregation to find duplicates in orders_raw
    print(" ⏳ جاري فحص السجلات المكررة في مجموعة البيانات الخام (orders_raw)...")
    pipeline = [
        {"$group": {
            "_id": "$raw_record.order_id",
            "count": {"$sum": 1},
            "source_rows": {"$push": "$source_row_number"},
            "ingested_times": {"$push": "$ingested_at"}
        }},
        {"$match": {
            "count": {"$gt": 1},
            "_id": {"$ne": "", "$ne": None}
        }},
        {"$sort": {"count": -1}},
        {"$limit": limit_examples}
    ]

    duplicates = list(raw_col.aggregate(pipeline))

    if not duplicates:
        print(" ℹ️ لم يتم العثور على أرقام طلبات مكررة في الدفعة الحالية.")
        client.close()
        return

    print(f" ✅ تم العثور على نماذج لطلبات مكررة متعددة في الملف الخام!\n")

    for idx, dup in enumerate(duplicates, 1):
        order_id = dup["_id"]
        count = dup["count"]
        print("-" * 80)
        print(f" 📦 مثال رقم [{idx}]: رقم الطلب (order_id): '{order_id}'")
        print(f"    - عدد مرات تكراره في الملف الخام: {count} مرات")
        print(f"    - أرقام الأسطر في ملف الـ CSV الأصلي: {dup['source_rows']}")
        print("-" * 80)

        # Retrieve all raw documents for this order_id
        raw_instances = list(raw_col.find({"raw_record.order_id": order_id}).sort("source_row_number", 1))

        print("\n 📥 1. النسخ المكررة كما وصلت في الطبقة الخام (orders_raw قبل الدمج):")
        for i, raw_doc in enumerate(raw_instances, 1):
            rec = raw_doc.get("raw_record", {})
            print(f"\n   [النسخة الخام #{i}] (السطر في CSV: {raw_doc.get('source_row_number')}) | وقت الوصول: {raw_doc.get('ingested_at')}")
            print(f"      - الحالة (Status):         '{rec.get('status')}'")
            print(f"      - المبلغ (Payment Amount):  '{rec.get('payment_amount')}'")
            print(f"      - الإجمالي (Total Amount):  '{rec.get('total_amount')}'")
            print(f"      - الهاتف (Phone):           '{rec.get('customer_phone')}'")

        # Retrieve consolidated document in orders_validated
        val_doc = val_col.find_one({"order_id": order_id}, {"_id": 0})

        print("\n ✨ 2. النتيجة النهائية الموحدة في (orders_validated بعد تطبيق الـ Upsert):")
        if val_doc:
            print("   👉 وثيقة واحدة فقط فريدة ومحدثة (Single Consolidated Document):")
            print(f"      - رقم الطلب (order_id):      '{val_doc.get('order_id')}'")
            print(f"      - الحالة الموحدة النهائية:   '{val_doc.get('status')}'")
            print(f"      - إجمالي الطلب الرقمي:       {val_doc.get('total_amount')} {val_doc.get('currency')}")
            print(f"      - حالة الجودة:               '{val_doc.get('quality_status')}'")
            print(f"      - سجل التدقيق (Corrections): {val_doc.get('corrections', [])}")
        else:
            print("   ℹ️ (السجل لم يستوفِ شروط التحقق أو عُزل في orders_quarantine)")

        print("\n" + "=" * 80)

    client.close()


if __name__ == "__main__":
    analyze_and_show_duplicates()
