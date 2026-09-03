"""
inspect_live_data.py - Granular Live Data Viewer & Inspector for MongoDB Collections

Displays distinct, formatted live samples from MongoDB for:
  1. Pure Valid Data (quality_status: 'valid' - Clean with zero modifications)
  2. Auto-Corrected Data (quality_status: 'corrected' - Repaired with Audit Trail)
  3. Repaired from Quarantine (quality_status: 'repaired_from_quarantine')
  4. Quarantined / Defective Data (orders_quarantine)
  5. Duplicated Raw Data before & after Idempotent Upsert
"""

import sys
from pathlib import Path
from pprint import pprint

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    MONGO_DB_NAME,
    COLLECTION_RAW,
    COLLECTION_VALIDATED,
    COLLECTION_QUARANTINE,
)
from src.mongo_setup import get_mongo_client, get_database
def inspect_all_data_layers():
    """
    Queries MongoDB and prints distinct live structured samples for:
    - Pure Valid Records (quality_status = 'valid')
    - Auto-Corrected Records (quality_status = 'corrected')
    - Quarantined Records (orders_quarantine)
    - Duplicated Records Before and After Deduplication
    """
    client = get_mongo_client()
    db = get_database(client)

    raw_col = db[COLLECTION_RAW]
    val_col = db[COLLECTION_VALIDATED]
    quar_col = db[COLLECTION_QUARANTINE]

    print("\n" + "=" * 80)
    print(" 🔎 [LIVE DATA INSPECTOR] استعراض البيانات المفصلة (صالحة | مصححة | معزولة | مكررة)")
    print("=" * 80)

    # -------------------------------------------------------------------------
    # 1. PURE VALID DATA (وصلت نظيفة 100% دون أي تعديل)
    # -------------------------------------------------------------------------
    pure_valid_count = val_col.count_documents({"quality_status": "valid"})
    pure_sample = val_col.find_one({"quality_status": "valid"}, {"_id": 0})

    print("\n" + "─" * 80)
    print(f" 🟢 1. البيانات الصالحة النقية (Pure Clean Valid) - [إجمالي: {pure_valid_count:,} سجل]")
    print("    - سجلات وصلت نظيفة 100% من المصدر ولم تحتج لأي تصحيح (corrections: [])")
    print("─" * 80)

    if pure_sample:
        print(" [نموذج حي لسجل صالح ونقي تماماً]:")
        print(f"   • رقم الطلب (order_id):        '{pure_sample.get('order_id')}'")
        print(f"   • اسم العميل:                  '{pure_sample.get('customer_name')}'")
        print(f"   • رقم الهاتف:                  '{pure_sample.get('customer_phone')}'")
        print(f"   • البريد الإلكتروني:           '{pure_sample.get('customer_email')}'")
        print(f"   • تاريخ الطلب (ISO):           '{pure_sample.get('order_date')}'")
        print(f"   • حالة الطلب:                  '{pure_sample.get('status')}'")
        print(f"   • الإجمالي:                    {pure_sample.get('total_amount')} {pure_sample.get('currency')}")
        print(f"   • حالة الجودة (quality_status):'{pure_sample.get('quality_status')}'")
        print(f"   • سجل التعديلات (Corrections): {pure_sample.get('corrections', [])} (صفر تعديلات)")
    else:
        print(" ℹ️ لا توجد سجلات بحالة 'valid' نقية في الدفعة الحالية.")

    # -------------------------------------------------------------------------
    # 2. AUTO-CORRECTED DATA (وصلت غير نظيفة وتم تصحيحها آلياً وتوثيق التعديل)
    # -------------------------------------------------------------------------
    corrected_count = val_col.count_documents({"quality_status": "corrected"})
    corrected_sample = val_col.find_one({"quality_status": "corrected"}, {"_id": 0})

    print("\n" + "─" * 80)
    print(f" 🟡 2. البيانات المصححة آلياً (Auto-Corrected) - [إجمالي: {corrected_count:,} سجل]")
    print("    - سجلات كانت تحتوي على أخطاء قابلة للعلاج (أرقام مشرقية، فواصل، إيميل، هاتف، مسافات)")
    print("    - تم تصحيحها آلياً وحفظ أثر التدقيق الجنائي داخلها")
    print("─" * 80)

    if corrected_sample:
        print(" [نموذج حي لسجل تم تصحيحه وتوثيق تعديلاته]:")
        print(f"   • رقم الطلب (order_id):        '{corrected_sample.get('order_id')}'")
        print(f"   • اسم العميل:                  '{corrected_sample.get('customer_name')}'")
        print(f"   • الهاتف بعد التصحيح:          '{corrected_sample.get('customer_phone')}'")
        print(f"   • الإيميل بعد التصحيح:         '{corrected_sample.get('customer_email')}'")
        print(f"   • التاريخ الموحد (ISO):        '{corrected_sample.get('order_date')}'")
        print(f"   • الحالة المعيارية:            '{corrected_sample.get('status')}'")
        print(f"   • الإجمالي الرقمي:             {corrected_sample.get('total_amount')} {corrected_sample.get('currency')}")
        print(f"   • حالة الجودة (quality_status):'{corrected_sample.get('quality_status')}'")
        print("\n   📋 سجل التدقيق الجنائي الموثق في السجل (Audit Trail - Corrections):")
        for idx, corr in enumerate(corrected_sample.get("corrections", []), 1):
            print(f"      ({idx}) [{corr.get('rule_code')}]")
            print(f"          - الحقل المستهدف:    '{corr.get('field')}'")
            print(f"          - القيمة الأصلية الخام: '{corr.get('original_value')}'")
            print(f"          - القيمة بعد التصحيح:  '{corr.get('corrected_value')}'")
    else:
        print(" ℹ️ لا توجد سجلات بحالة 'corrected' حالياً.")

    # -------------------------------------------------------------------------
    # 3. REPAIRED FROM QUARANTINE (تم إنقاذها من العزل)
    # -------------------------------------------------------------------------
    repaired_count = val_col.count_documents({"quality_status": "repaired_from_quarantine"})
    if repaired_count > 0:
        repaired_sample = val_col.find_one({"quality_status": "repaired_from_quarantine"}, {"_id": 0})
        print("\n" + "─" * 80)
        print(f" 🛠️ 3. السجلات المسترجعة من العزل (Repaired From Quarantine) - [إجمالي: {repaired_count:,} سجل]")
        print("    - تم إصلاح JSON المبتور لها أو تصحيح تواريخها وترقيتها للبيانات الصالحة")
        print("─" * 80)
        if repaired_sample:
            print(" [نموذج حي لسجل مسترجع من العزل]:")
            print(f"   • رقم الطلب:                   '{repaired_sample.get('order_id')}'")
            print(f"   • الخطأ السابق في العزل:       '{repaired_sample.get('original_quarantine_error')}'")
            print(f"   • حالة الجودة:                 '{repaired_sample.get('quality_status')}'")
            print(f"   • المنتجات المستعادة:          {repaired_sample.get('items', [])}")

    # -------------------------------------------------------------------------
    # 4. QUARANTINED DATA (السجلات المعزولة)
    # -------------------------------------------------------------------------
    quar_count = quar_col.count_documents({})
    quar_sample = quar_col.find_one({}, {"_id": 0})

    print("\n" + "─" * 80)
    print(f" 🔴 4. البيانات المعزولة (orders_quarantine) - [إجمالي: {quar_count:,} سجل]")
    print("    - سجلات تحتوي على أخطاء قاتلة يستحيل إصلاحها دون تخمين عشوائي (عزل لحماية الجودة)")
    print("─" * 80)

    if quar_sample:
        print(" [نموذج حي لسجل معزول]:")
        print(f"   • رقم الطلب (إن وجد):          '{quar_sample.get('order_id')}'")
        print(f"   • كود الخطأ (error_code):      '{quar_sample.get('error_code')}'")
        print(f"   • سبب العزل (error_reason):    '{quar_sample.get('error_reason')}'")
        print(f"   • توقيت العزل:                 '{quar_sample.get('quarantined_at')}'")
        print("\n   📦 السجل الخام المعزول كما وصل من المصدر:")
        pprint(quar_sample.get("raw_record", {}))
    else:
        print(" ℹ️ لا توجد سجلات معزولة حالياً.")

    # -------------------------------------------------------------------------
    # 5. DUPLICATED DATA (السجلات المكررة قبل وبعد الدمج)
    # -------------------------------------------------------------------------
    print("\n" + "─" * 80)
    print(" 🔄 5. البيانات المكررة في orders_raw ودمجها بـ Upsert في orders_validated")
    print("─" * 80)

    pipeline = [
        {"$group": {
            "_id": "$raw_record.order_id",
            "count": {"$sum": 1},
            "rows": {"$push": "$source_row_number"}
        }},
        {"$match": {
            "count": {"$gt": 1},
            "_id": {"$ne": "", "$ne": None}
        }},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ]

    dup_agg = list(raw_col.aggregate(pipeline))
    if dup_agg:
        dup = dup_agg[0]
        dup_order_id = dup["_id"]
        dup_count = dup["count"]
        print(f" 📦 رقم طلب مكرر: '{dup_order_id}' (تكرر {dup_count} مرات في الأسطر: {dup['rows']})")

        print("\n   📥 النسخ المكررة في orders_raw (قبل الدمج):")
        for i, r_doc in enumerate(raw_col.find({"raw_record.order_id": dup_order_id}).limit(3), 1):
            r = r_doc.get("raw_record", {})
            print(f"      [نسخة خام #{i}] سطر: {r_doc.get('source_row_number')} | الحالة: '{r.get('status')}' | المبلغ: '{r.get('total_amount')}'")

        print("\n   ✨ السجل الموحد في orders_validated (بعد الدمج بالـ Upsert):")
        val_dup = val_col.find_one({"order_id": dup_order_id}, {"_id": 0})
        if val_dup:
            print(f"      👉 وثيقة واحدة فريدة: order_id='{val_dup.get('order_id')}' | الحالة='{val_dup.get('status')}' | الإجمالي={val_dup.get('total_amount')} {val_dup.get('currency')}")
    else:
        print(" ℹ️ لا توجد تكرارات في الدفعة الحالية.")

    print("\n" + "=" * 80 + "\n")
    client.close()


if __name__ == "__main__":
    inspect_all_data_layers()
