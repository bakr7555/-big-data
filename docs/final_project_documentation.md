# وثيقة التوثيق الشاملة لمشروع البيانات الضخمة (Final Project Documentation)
## خط معالجة البيانات الهجين، التحليلات المتقدمة، الجداول الملخصة، وخادم الـ API

---

## 1. بطاقة المشروع (Project Overview)
- **إعداد الطالب:** بكر مهيوب خالد سيف التبعي (Baker Mahyoub Khaled Saif Al-Tabei)
- **المقرر الدراسي:** البيانات الضخمة (Big Data - Practical)
- **المؤسسة:** جامعة الرازي - كلية الحاسوب وتكنولوجيا المعلومات
- **المشروع:** نظام متكامل لابتلاع وتدقيق البيانات الضخمة وتحليلها برمجياً (Hybrid ELT Pipeline & Advanced Analytics)
- **التقنيات المستخدمة:**
  - **لغة البرمجة:** Python 3.10+
  - **قاعدة البيانات:** MongoDB Community Server (WiredTiger Engine)
  - **محركات المعالجة:** Python Streaming Batch Generator + Apache PySpark DataFrame API
  - **واجهة برمجة التطبيقات:** FastAPI + Uvicorn + Pydantic
  - **المجدول الزمني:** APScheduler

---

## 2. المعمارية الهندسية للنظام (System Architecture)

النظام مبني وفق معمارية **ELT (Extract - Load - Transform)** للتعامل مع البيانات غير النظيفة والضخمة بكفاءة:

1. **الابتلاع والتوجيه الذكي (File Router):**
   - فحص حجم الملف الوارد.
   - إذا كان الحجم $\le 200\text{ MB}$: يوجه لمحرك بايثون المتدفق (Python Streaming Batch) بذاكرة $O(1)$ وسرعة فائقة.
   - إذا كان الحجم $> 200\text{ MB}$: يوجه لمحرك Apache Spark للمعالجة الموزعة.
2. **التحميل الخام (Raw Ingestion):**
   - تفريغ كافة السجلات كما هي في مجموعة `orders_raw` مع توثيق `run_id` و `ingested_at` و `source_file`.
3. **محرك الجودة والتحويل (Quality Engine):**
   - تطبيق قواعد الجودة والتنظيف، وتوثيق سجل التعديل (Audit Trail) في مصفوفة `corrections`.
   - عزل السجلات ذات الأخطاء الجوهرية غير القابلة للإصلاح في `orders_quarantine`.
   - حفظ السجلات السليمة والمصححة في `orders_validated` باستخدام عمليات الـ Upsert لضمان عدم التكرار (Idempotency).
4. **الطبقة التحليلية (Analytics & Materialized Views):**
   - بناء وتحديث الجداول الملخصة (`daily_sales_summary` و `top_products_summary`) تدريجياً عبر آلية الـ Watermark.
5. **طبقة الخدمات (API & Scheduled Jobs):**
   - توفير 10 مسارات REST API موثقة عبر Swagger UI.
   - تشغيل مهام التحديث المجدولة وتسجيلها في `job_execution_logs`.

---

## 3. طبقات تخزين البيانات في MongoDB (Data Layers)

| المجموعة (Collection) | وظيفتها المعمارية | الحقول الأساسية | الفهارس |
| :--- | :--- | :--- | :--- |
| **`orders_raw`** | حفظ البيانات الأصلية دون حذف أو تعديل | `_id`, `run_id`, `ingested_at`, `source_file`, `raw_data` | `idx_raw_run_id` |
| **`orders_validated`** | السجلات المقبولة (السليمة + المصححة) | `order_id`, `order_date`, `status`, `customer_id`, `total_amount`, `quality_status`, `corrections` | `order_id` (Unique), الفهارس الـ 4 المتقدمة |
| **`orders_quarantine`** | السجلات المعزولة لاحتوائها على أخطاء قاتلة | `order_id`, `raw_record`, `error_code`, `error_reason`, `quarantined_at` | `idx_quarantine_error_code` |
| **`daily_sales_summary`** | جدول ملخص المبيعات اليومية المحدث تدريجياً | `date`, `total_sales`, `orders_count`, `avg_order_value`, `last_updated_at` | `date` (Unique) |
| **`top_products_summary`** | جدول ملخص مبيعات المنتجات بحسب SKU | `sku`, `product_name`, `total_units_sold`, `total_revenue`, `last_updated_at` | `sku` (Unique) |
| **`mv_refresh_metadata`** | تتبع العلامة الزمنية (Watermark) للجداول الملخصة | `view_name`, `last_watermark`, `last_refreshed_at`, `last_delta_processed` | `view_name` (Unique) |
| **`job_execution_logs`** | سجل تتبع تنفيذ المهام المجدولة | `job_name`, `started_at`, `completed_at`, `status`, `duration_seconds`, `details` | `idx_job_name_time` |

---

## 4. محرك قواعد الجودة والعزل (Quality & Quarantine Rules)

### قواعد الإصلاح والتصحيح الآلي (10 قواعد):
1. **`R1_ARABIC_NUMERALS`**: تحويل الأرقام المشرقية (`٠-٩`) إلى أرقام عشرية إنجليزية في المبالغ وتكلفة التوصيل.
2. **`R2_CURRENCY_NORMALIZATION`**: توحيد مسميات العملات (`ريال يمني`، `ر.ي`) إلى `YER`.
3. **`R3_THOUSANDS_SEPARATOR`**: إزالة فواصل الآلاف والمسافات داخل القيم المالية.
4. **`R4_PRICE_WORDS_TRANSLATION`**: استبدال الكلمات العربية الثابتة المعبرة عن المبالغ بقيمها الرقمية.
5. **`R5_PHONE_STANDARDIZATION`**: توحيد أرقام الهواتف وإزالة المفتاح الدولي `+967` وتنسيقها إلى 9 أرقام تبدأ بـ 7.
6. **`R6_EMAIL_CLEANING`**: إزالة الرموز المكررة في البريد الإلكتروني (`@@` و `..`).
7. **`R7_DATE_STANDARDIZATION`**: تحويل صيغ التواريخ المختلفة إلى صيغة ISO-8601 القياسية (`YYYY-MM-DDTHH:MM:SS`).
8. **`R8_WHITESPACE_AND_SYNONYMS`**: إزالة المسافات الزائدة وتوحيد مسميات حالات الطلب.
9. **`R9_TOTAL_AMOUNT_RECALCULATION`**: إعادة حساب المبلغ الإجمالي من مجموع العناصر وتكلفة التوصيل عند وجود تباين.
10. **`R10_ITEMS_DATA_TYPES`**: تحويل وتصحيح الكميات المكتوبة كنصوص في JSON إلى أرقام صحيحة `int`.

### قواعد العزل الجوهري (12 فئة):
- عزل معرف الطلب المفقود (`MISSING_ORDER_ID`).
- عزل معرف العميل المفقود (`MISSING_CUSTOMER_ID`).
- عزل أرقام الهواتف غير الصالحة والقصيرة (`INVALID_PHONE`).
- عزل البريد الإلكتروني بدون نطاق (`INVALID_EMAIL_DOMAIN`).
- عزل التواريخ المستحيلة وغير المنطقية (`INVALID_IMPOSSIBLE_DATE`).
- عزل حالات الطلب غير المعروفة (`UNKNOWN_STATUS`).
- عزل عناصر الطلب الفارغة `[]` (`EMPTY_ITEMS`).
- عزل نصوص JSON التالفة (`CORRUPTED_ITEMS_JSON`).
- عزل العناصر المفقود منها كود المنتج (`MISSING_ITEM_SKU`).
- عزل السجلات ذات الكميات السالبة (`NEGATIVE_QUANTITY`).
- عزل العملات المجهولة (`UNKNOWN_CURRENCY`).
- عزل السجلات ذات الأخطاء الجوهرية المتعارضة (`MULTIPLE_CONFLICTING_ERRORS`).

---

## 5. المتطلبات المتقدمة للمشروع النهائي (Phase 2 Deliverables)

### 1. الفهارس المتقدمة وتحليل الأداء (`src/indexes.py`):
- **`idx_customer_id`**: فهرس مفرد على معرف العميل (Single Index).
- **`idx_order_date_status`**: فهرس مركب على تاريخ الطلب وحالته (Compound Index).
- **`idx_city_total_amount`**: فهرس مركب على المدينة والمبلغ الإجمالي (Compound Index).
- **`idx_items_sku`**: فهرس متعدد المفاتيح على عناصر الطلب (Multikey Index).
- **قياس الأداء:** استخدام `explain("executionStats")` لقياس زمن التنفيذ وعدد الوثائق المفحوصة (`totalDocsExamined`).

### 2. استعلامات الأعمال الخمسة (`src/queries.py`):
1. `Q1_TOP_CUSTOMERS`: استعلام كبار العملاء بحسب إجمالي الإنفاق.
2. `Q2_PENDING_ORDERS`: تصفية الطلبات المعلقة بحسب النطاق الزمني.
3. `Q3_CITY_ORDERS_DISTRIBUTION`: توزيع الطلبات بحسب المدن والمديريات.
4. `Q4_PRODUCT_SKU_SEARCH`: البحث في مصفوفة العناصر عن طلبات تحتوي على SKU محدد.
5. `Q5_PAYMENT_METHODS_AUDIT`: مراجعة قنوات الدفع ومعدلات نجاحها ومبالغها.

### 3. تقارير التجميع الإحصائية الخمسة (`src/aggregations.py`):
1. `sales_by_city`: إجمالي المبيعات والإيرادات ومتوسط قيمة الطلب لكل مدينة.
2. `top_products`: المنتجات الأكثر مبيعاً والإيراد الإجمالي بحسب الـ SKU عبر `$unwind`.
3. `top_customers`: تصنيف كبار العملاء ومجموع مدفوعاتهم.
4. `monthly_sales_trend`: التطور الشهري للمبيعات وتكاليف التوصيل.
5. `order_status_distribution`: تحليل توزيع مراحل تنفيذ الطلبات ونسب نجاحها.

### 4. الجداول الملخصة المحدثة تدريجياً (`src/materialized_views.py`):
- **`daily_sales_summary`**: تجميع يومي لإيرادات المبيعات وعدد الطلبات.
- **`top_products_summary`**: تجميع كميات وإيرادات كل منتج.
- **التحديث التدريجي (Incremental Refresh):** يعتمد على علامة زمنية (Watermark) مخزنة في `mv_refresh_metadata` لمعالجة السجلات الجديدة فقط واستخدام عمليات `$inc` الذرية دون الحاجة لإعادة مسح أو تجميع الجدول بأكمله.

### 5. المهام المجدولة في الخلفية (`src/scheduler.py`):
- مهمة تحديث الجداول الملخصة دورياً (`refresh_materialized_views_job`).
- مهمة إنشاء تقرير الأداء اليومي (`daily_performance_report_job`).
- توثيق سجلات التشغيل في `job_execution_logs`.

### 6. خادم واجهة برمجة التطبيقات (`src/api.py`):
تطبيق مبني بـ **FastAPI** يوفر المسارات التالية:
- `GET /health` : فحص حالة الخادم وقاعدة البيانات وإحصائيات المجموعات.
- `POST /ingest` : تشغيل خط الأنابيب على ملف بيانات جديد.
- `POST /indexes/create` : إنشاء الفهارس المتقدمة.
- `GET /indexes/benchmark` : تشغيل اختبارات قياس الأداء قبل وبعد الفهارس.
- `GET /queries/{query_name}` : تنفيذ أي استعلام من استعلامات الأعمال الـ 5.
- `GET /aggregations/{report_name}` : تنفيذ أي تقرير من تقارير التجميع الـ 5.
- `POST /refresh-mv` : تشغيل التحديث التدريجي أو الكامل للجداول الملخصة.
- `POST /jobs/run/{job_name}` : تنفيذ المهام الخلفية يدوياً.
- `GET /jobs/history` : استعراض سجلات تنفيذ المهام الخلفية.

---

## 6. معادلة الاتساق والنزاهة الرياضية (Consistency Invariant)

يتحقق النظام برمجياً بعد كل تشغيل من المعادلة الصارمة:

$$\text{Total Raw} = \text{Clean Valid} + \text{Corrected} + \text{Quarantined}$$

- **`Total Raw`**: إجمالي السجلات المقروءة والمحفوظة في `orders_raw`.
- **`Clean Valid`**: سجلات دخلت نظيفة بدون الحاجة لأي تصحيح.
- **`Corrected`**: سجلات تم إصلاحها وتوثيق تصحيحاتها ودخلت `orders_validated`.
- **`Quarantined`**: سجلات معزولة لاحتوائها على خلل جوهري لا يمكن إصلاحه.

---

## 7. دليل التشغيل السريع (Commands Guide)

```powershell
# 1. تثبيت الحزم المطلوبة:
pip install -r requirements.txt

# 2. تشغيل خدمة قاعدة البيانات MongoDB:
net start MongoDB

# 3. تشغيل خط أنابيب البيانات على ملف البيانات:
python src/main.py -f "H:/01_student_test_small.csv" --reset-db

# 4. تشغيل خادم الـ API وفتح واجهة Swagger:
uvicorn src.api:app --reload --port 8000
# الرابط في المتصفح: http://localhost:8000/docs

# 5. تشغيل حزمة الاختبارات الآلية الشاملة (21 اختباراً):
python -m unittest discover tests
```
