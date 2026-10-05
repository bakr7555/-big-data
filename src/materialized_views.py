"""
materialized_views.py - Incremental Materialized Views Engine
Big Data Final Project - Requirement 3: Materialized Views

Implements 2 Incremental Materialized Views:
  1. daily_sales_summary: Daily revenue, total orders, and average order value.
  2. top_products_summary: Aggregate sales volume, unit counts, and revenue per product SKU.

Features True Incremental Refresh:
  - Tracks a high-watermark timestamp in `mv_refresh_metadata`.
  - On refresh, only queries delta orders (records ingested/repaired after watermark).
  - Uses atomic $inc, $set, and Upsert operations so the view updates in-place without rebuilding.
"""

import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
from pymongo import UpdateOne, ASCENDING
from pymongo.database import Database

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    COLLECTION_VALIDATED,
    COLLECTION_MV_DAILY_SALES,
    COLLECTION_MV_TOP_PRODUCTS,
    COLLECTION_MV_METADATA
)
from src.mongo_setup import get_mongo_client, get_database

def safe_float(val: Any, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def safe_int(val: Any, default: int = 0) -> int:
    if val is None:
        return default
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default

def init_materialized_views(db: Database) -> None:
    # 1. Daily sales view index
    daily_col = db[COLLECTION_MV_DAILY_SALES]
    daily_col.create_index([("date", ASCENDING)], unique=True, name="idx_mv_daily_date")
    
    # 2. Top products view index
    prod_col = db[COLLECTION_MV_TOP_PRODUCTS]
    prod_col.create_index([("sku", ASCENDING)], unique=True, name="idx_mv_prod_sku")
    
    # 3. Metadata watermark collection
    meta_col = db[COLLECTION_MV_METADATA]
    meta_col.create_index([("view_name", ASCENDING)], unique=True, name="idx_mv_meta_view")

def get_watermark(db: Database, view_name: str) -> str:
    meta_col = db[COLLECTION_MV_METADATA]
    doc = meta_col.find_one({"view_name": view_name})
    if doc and "last_watermark" in doc:
        return doc["last_watermark"]
    return "1970-01-01T00:00:00"

def update_watermark(db: Database, view_name: str, new_watermark: str, delta_count: int) -> None:
    meta_col = db[COLLECTION_MV_METADATA]
    meta_col.update_one(
        {"view_name": view_name},
        {"$set": {
            "last_watermark": new_watermark,
            "last_refreshed_at": datetime.utcnow().isoformat(),
            "last_delta_processed": delta_count
        }},
        upsert=True
    )

def refresh_daily_sales_mv(db: Database, full_refresh: bool = False) -> Dict[str, Any]:
    init_materialized_views(db)
    val_col = db[COLLECTION_VALIDATED]
    mv_col = db[COLLECTION_MV_DAILY_SALES]
    
    t_start = time.time()
    watermark = "1970-01-01T00:00:00" if full_refresh else get_watermark(db, COLLECTION_MV_DAILY_SALES)
    
    query = {"$or": [
        {"processed_at": {"$gt": watermark}},
        {"ingested_at": {"$gt": watermark}},
        {"repaired_at": {"$gt": watermark}}
    ]} if not full_refresh else {}
    
    delta_cursor = val_col.find(query, {"order_date": 1, "total_amount": 1, "delivery_cost": 1, "processed_at": 1, "ingested_at": 1, "repaired_at": 1})
    
    delta_records = list(delta_cursor)
    delta_count = len(delta_records)
    
    if delta_count == 0:
        return {
            "view_name": COLLECTION_MV_DAILY_SALES,
            "status": "UP_TO_DATE",
            "delta_processed": 0,
            "duration_seconds": round(time.time() - t_start, 3),
            "watermark": watermark
        }

    daily_deltas: Dict[str, Dict[str, float]] = {}
    max_ts = watermark

    for rec in delta_records:
        raw_date = rec.get("order_date")
        if not raw_date or len(raw_date) < 10:
            continue
        date_key = raw_date[:10]
        amt = safe_float(rec.get("total_amount"))
        
        if date_key not in daily_deltas:
            daily_deltas[date_key] = {"total_sales": 0.0, "orders_count": 0}
        daily_deltas[date_key]["total_sales"] += amt
        daily_deltas[date_key]["orders_count"] += 1
        
        ts = rec.get("processed_at") or rec.get("repaired_at") or rec.get("ingested_at") or ""
        if ts > max_ts:
            max_ts = ts

    bulk_ops = []
    now_iso = datetime.utcnow().isoformat()
    for date_key, delta in daily_deltas.items():
        bulk_ops.append(
            UpdateOne(
                {"date": date_key},
                {
                    "$inc": {
                        "total_sales": delta["total_sales"],
                        "orders_count": delta["orders_count"]
                    },
                    "$set": {
                        "last_updated_at": now_iso
                    }
                },
                upsert=True
            )
        )

    if bulk_ops:
        mv_col.bulk_write(bulk_ops, ordered=False)

    for date_key in daily_deltas.keys():
        doc = mv_col.find_one({"date": date_key})
        if doc and doc.get("orders_count", 0) > 0:
            avg_val = round(doc["total_sales"] / doc["orders_count"], 2)
            mv_col.update_one(
                {"date": date_key},
                {"$set": {
                    "avg_order_value": avg_val,
                    "total_sales": round(doc["total_sales"], 2)
                }}
            )

    if max_ts == watermark:
        max_ts = datetime.utcnow().isoformat()
    update_watermark(db, COLLECTION_MV_DAILY_SALES, max_ts, delta_count)

    duration = time.time() - t_start
    return {
        "view_name": COLLECTION_MV_DAILY_SALES,
        "status": "REFRESHED",
        "delta_processed": delta_count,
        "unique_days_updated": len(daily_deltas),
        "new_watermark": max_ts,
        "duration_seconds": round(duration, 3)
    }

def refresh_top_products_mv(db: Database, full_refresh: bool = False) -> Dict[str, Any]:
    init_materialized_views(db)
    val_col = db[COLLECTION_VALIDATED]
    mv_col = db[COLLECTION_MV_TOP_PRODUCTS]
    
    t_start = time.time()
    watermark = "1970-01-01T00:00:00" if full_refresh else get_watermark(db, COLLECTION_MV_TOP_PRODUCTS)
    
    query = {"$or": [
        {"processed_at": {"$gt": watermark}},
        {"ingested_at": {"$gt": watermark}},
        {"repaired_at": {"$gt": watermark}}
    ]} if not full_refresh else {}
    
    delta_cursor = val_col.find(query, {"items": 1, "processed_at": 1, "ingested_at": 1, "repaired_at": 1})
    delta_records = list(delta_cursor)
    delta_count = len(delta_records)

    if delta_count == 0:
        return {
            "view_name": COLLECTION_MV_TOP_PRODUCTS,
            "status": "UP_TO_DATE",
            "delta_processed": 0,
            "duration_seconds": round(time.time() - t_start, 3),
            "watermark": watermark
        }

    prod_deltas: Dict[str, Dict[str, Any]] = {}
    max_ts = watermark

    for rec in delta_records:
        items = rec.get("items") or []
        for itm in items:
            sku = itm.get("sku")
            if not sku:
                continue
            name = itm.get("name", "Unknown Product")
            qty = safe_int(itm.get("qty"))
            tot = safe_float(itm.get("total"))
            if tot == 0.0:
                price = safe_float(itm.get("unit_price"))
                tot = price * qty
            
            if sku not in prod_deltas:
                prod_deltas[sku] = {"name": name, "units_sold": 0, "total_revenue": 0.0}
            prod_deltas[sku]["units_sold"] += qty
            prod_deltas[sku]["total_revenue"] += tot

        ts = rec.get("processed_at") or rec.get("repaired_at") or rec.get("ingested_at") or ""
        if ts > max_ts:
            max_ts = ts

    bulk_ops = []
    now_iso = datetime.utcnow().isoformat()
    for sku, delta in prod_deltas.items():
        bulk_ops.append(
            UpdateOne(
                {"sku": sku},
                {
                    "$inc": {
                        "total_units_sold": delta["units_sold"],
                        "total_revenue": delta["total_revenue"]
                    },
                    "$set": {
                        "product_name": delta["name"],
                        "last_updated_at": now_iso
                    }
                },
                upsert=True
            )
        )

    if bulk_ops:
        mv_col.bulk_write(bulk_ops, ordered=False)

    if max_ts == watermark:
        max_ts = datetime.utcnow().isoformat()
    update_watermark(db, COLLECTION_MV_TOP_PRODUCTS, max_ts, delta_count)

    duration = time.time() - t_start
    return {
        "view_name": COLLECTION_MV_TOP_PRODUCTS,
        "status": "REFRESHED",
        "delta_processed": delta_count,
        "unique_products_updated": len(prod_deltas),
        "new_watermark": max_ts,
        "duration_seconds": round(duration, 3)
    }

def refresh_all_materialized_views(db: Database, full_refresh: bool = False) -> Dict[str, Any]:
    daily_res = refresh_daily_sales_mv(db, full_refresh=full_refresh)
    prod_res = refresh_top_products_mv(db, full_refresh=full_refresh)
    return {
        "status": "SUCCESS",
        "refreshed_at": datetime.utcnow().isoformat(),
        "views": [daily_res, prod_res]
    }

if __name__ == "__main__":
    client = get_mongo_client()
    db = get_database(client)
    res = refresh_all_materialized_views(db, full_refresh=True)
    import json
    print("\n=== MATERIALIZED VIEWS INITIAL BUILD ===")
    print(json.dumps(res, indent=2))
