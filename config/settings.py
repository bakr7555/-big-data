"""
settings.py - Global Configuration & Constants for Hybrid ELT Pipeline
Course: Big Data (Practical) - Al-Razi University
"""

import os
from pathlib import Path
from typing import Dict

# =========================================================================
# 1. Base Project Paths
# =========================================================================
CONFIG_DIR: Path = Path(__file__).resolve().parent
PROJECT_ROOT: Path = CONFIG_DIR.parent
DATA_DIR: Path = PROJECT_ROOT / "data"
REPORTS_DIR: Path = PROJECT_ROOT / "reports"
DOCS_DIR: Path = PROJECT_ROOT / "docs"

# Ensure runtime directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
DOCS_DIR.mkdir(parents=True, exist_ok=True)

# =========================================================================
# 2. MongoDB Ingestion & Persistence (Section 6.6)
# =========================================================================
MONGO_URI: str = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB_NAME: str = os.getenv("MONGO_DB_NAME", "bigdata_orders_db")

# Collection Names (Raw Ingestion -> Production Validated -> DLQ Quarantine)
COLLECTION_RAW: str = os.getenv("COLLECTION_RAW", "orders_raw")
COLLECTION_VALIDATED: str = os.getenv("COLLECTION_VALIDATED", "orders_validated")
COLLECTION_QUARANTINE: str = os.getenv("COLLECTION_QUARANTINE", "orders_quarantine")

# =========================================================================
# 3. Hybrid Routing & Batch Engine Settings (Sections 6.2 & 6.3)
# =========================================================================
# Threshold in MB to dynamically route between Python Batch and PySpark
SMALL_FILE_THRESHOLD_MB: float = float(os.getenv("SMALL_FILE_THRESHOLD_MB", "200.0"))

# Optimal batch size for streaming Python insert_many (~2.1 MB payload)
BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "5000"))

# =========================================================================
# 4. Apache PySpark Distributed Engine Settings (Section 6.4)
# =========================================================================
SPARK_APP_NAME: str = os.getenv("SPARK_APP_NAME", "HybridOrdersPipeline")
SPARK_MASTER: str = os.getenv("SPARK_MASTER", "local[*]")
SPARK_DRIVER_MEMORY: str = os.getenv("SPARK_DRIVER_MEMORY", "2g")
SPARK_EXECUTOR_MEMORY: str = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")

# Official MongoDB Spark Connector (Spark 3.x / Scala 2.12 compatible)
SPARK_MONGO_PACKAGE: str = os.getenv(
    "SPARK_MONGO_PACKAGE",
    "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"
)

# =========================================================================
# 5. Data Standardization & Quality Mapping (Section 6.7)
# =========================================================================
TARGET_CURRENCY: str = "YER"
DATE_OUTPUT_FORMAT: str = "%Y-%m-%d"
DATETIME_OUTPUT_FORMAT: str = "%Y-%m-%d %H:%M:%S"

# Standard Order Status Dictionary Mapping (Rule 8)
VALID_STATUS_MAP: Dict[str, str] = {
    "مؤكد": "CONFIRMED",
    "تم التأكيد": "CONFIRMED",
    "confirmed": "CONFIRMED",
    "مدفوع": "PAID",
    "تم الدفع": "PAID",
    "paid": "PAID",
    "ملغي": "CANCELLED",
    "ملغى": "CANCELLED",
    "cancelled": "CANCELLED",
    "canceled": "CANCELLED",
    "قيد الانتظار": "PENDING",
    "معلق": "PENDING",
    "pending": "PENDING",
    "تم التوصيل": "DELIVERED",
    "تم التسليم": "DELIVERED",
    "مكتمل": "DELIVERED",
    "delivered": "DELIVERED",
    "مسترجع": "REFUNDED",
    "مرتجع": "REFUNDED",
    "refunded": "REFUNDED",
    "قيد الشحن": "SHIPPED",
    "شحن": "SHIPPED",
    "shipped": "SHIPPED",
}

# Arabic Words to Number Mapping for Prices (Rule 4)
ARABIC_WORDS_NUMBER_MAP: Dict[str, int] = {
    "مائة": 100,
    "مئتان": 200,
    "ثلاثمائة": 300,
    "أربعمائة": 400,
    "خمسمائة": 500,
    "ستمائة": 600,
    "سبعمائة": 700,
    "ثمانمائة": 800,
    "تسعمائة": 900,
    "ألف": 1000,
    "الف": 1000,
    "ألفان": 2000,
    "الفان": 2000,
    "ألفين": 2000,
    "الفين": 2000,
    "ثلاثة آلاف": 3000,
    "ثلاثة الاف": 3000,
    "أربعة آلاف": 4000,
    "اربعة الاف": 4000,
    "خمسة آلاف": 5000,
    "خمسة الاف": 5000,
    "ستة آلاف": 6000,
    "ستة الاف": 6000,
    "سبعة آلاف": 7000,
    "سبعة الاف": 7000,
    "ثمانية آلاف": 8000,
    "ثمانية الاف": 8000,
    "تسعة آلاف": 9000,
    "تسعة الاف": 9000,
    "عشرة آلاف": 10000,
    "عشرة الاف": 10000,
    "عشرون ألف": 20000,
    "عشرون الف": 20000,
    "خمسون ألف": 50000,
    "خمسون الف": 50000,
    "مائة ألف": 100000,
    "مائة الف": 100000,
}

# =========================================================================
# 6. Reports & Output Paths (Section 6.12)
# =========================================================================
REPORTS_FILE_PATH: Path = REPORTS_DIR / "results.json"
