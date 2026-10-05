"""
queries.py - 5 Practical Business Queries for Big Data E-Commerce Pipeline
Big Data Final Project - Requirement 1: Practical Queries
"""

import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
from pymongo import DESCENDING, ASCENDING
from pymongo.database import Database

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import COLLECTION_VALIDATED
from src.mongo_setup import get_mongo_client, get_database

QUERIES_METADATA = [
    {
        "id": "Q1_CUSTOMER_HISTORY",
        "name": "Customer Purchase History",
        "description": "Retrieves chronological order history for a specific customer.",
        "params": ["customer_id", "limit"]
    },
    {
        "id": "Q2_DATE_STATUS_FILTER",
        "name": "Date Range and Status Filter",
        "description": "Finds orders in a date range filtered by fulfillment status.",
        "params": ["start_date", "end_date", "status", "limit"]
    },
    {
        "id": "Q3_HIGH_VALUE_CITY",
        "name": "High Value Orders by City",
        "description": "Finds premium high-value orders in a specific city sorted descending by amount.",
        "params": ["city", "min_amount", "limit"]
    },
    {
        "id": "Q4_PRODUCT_SKU_LOOKUP",
        "name": "Orders by Product SKU",
        "description": "Queries orders containing a specific product SKU inside embedded items.",
        "params": ["sku", "limit"]
    },
    {
        "id": "Q5_PAYMENT_METHOD_AUDIT",
        "name": "Payment Method and Currency Audit",
        "description": "Audits payment channels (e.g. Cash/Card) and flags non-standard payment flows.",
        "params": ["payment_method", "payment_status", "limit"]
    }
]

def query_customer_history(db: Database, customer_id: str = "عميل-1000", limit: int = 50) -> List[Dict[str, Any]]:
    col = db[COLLECTION_VALIDATED]
    cursor = col.find({"customer_id": customer_id}, {"_id": 0}).sort("order_date", DESCENDING).limit(limit)
    return list(cursor)

def query_date_status_filter(db: Database, start_date: str = "2025-01-01", end_date: str = "2025-12-31", status: str = "DELIVERED", limit: int = 50) -> List[Dict[str, Any]]:
    col = db[COLLECTION_VALIDATED]
    filt = {
        "order_date": {"$gte": start_date, "$lte": end_date},
        "status": status
    }
    cursor = col.find(filt, {"_id": 0}).sort("order_date", DESCENDING).limit(limit)
    return list(cursor)

def query_high_value_city(db: Database, city: str = "صنعاء", min_amount: float = 50000.0, limit: int = 50) -> List[Dict[str, Any]]:
    col = db[COLLECTION_VALIDATED]
    filt = {
        "city": city,
        "total_amount": {"$gte": min_amount}
    }
    cursor = col.find(filt, {"_id": 0}).sort("total_amount", DESCENDING).limit(limit)
    return list(cursor)

def query_product_sku_lookup(db: Database, sku: str = "SKU-1001", limit: int = 50) -> List[Dict[str, Any]]:
    col = db[COLLECTION_VALIDATED]
    cursor = col.find({"items.sku": sku}, {"_id": 0}).limit(limit)
    return list(cursor)

def query_payment_method_audit(db: Database, payment_method: str = "بطاقة", payment_status: str = "تم الدفع", limit: int = 50) -> List[Dict[str, Any]]:
    col = db[COLLECTION_VALIDATED]
    filt = {
        "payment_method": payment_method,
        "payment_status": payment_status
    }
    cursor = col.find(filt, {"_id": 0}).limit(limit)
    return list(cursor)

QUERY_DISPATCHER = {
    "Q1_CUSTOMER_HISTORY": query_customer_history,
    "Q2_DATE_STATUS_FILTER": query_date_status_filter,
    "Q3_HIGH_VALUE_CITY": query_high_value_city,
    "Q4_PRODUCT_SKU_LOOKUP": query_product_sku_lookup,
    "Q5_PAYMENT_METHOD_AUDIT": query_payment_method_audit
}

def execute_named_query(db: Database, name: str, **kwargs) -> Dict[str, Any]:
    query_func = QUERY_DISPATCHER.get(name.upper())
    if not query_func:
        valid_names = list(QUERY_DISPATCHER.keys())
        raise ValueError(f"Unknown query '{name}'. Available queries: {valid_names}")
    
    # Filter kwargs to matching parameters
    import inspect
    sig = inspect.signature(query_func)
    filtered_kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters and v is not None}
    
    results = query_func(db, **filtered_kwargs)
    return {
        "query_name": name,
        "parameters_applied": filtered_kwargs,
        "result_count": len(results),
        "results": results
    }

if __name__ == "__main__":
    client = get_mongo_client()
    db = get_database(client)
    for q_name in QUERY_DISPATCHER:
        res = execute_named_query(db, q_name, limit=5)
        print(f"[TEST QUERY] {q_name} -> returned {res['result_count']} documents")
