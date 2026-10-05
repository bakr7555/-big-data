"""
indexes.py - Production Index Management and Explain Benchmarking
Big Data Final Project - Requirement 1: Indexes and ExecutionStats
"""

import time
import sys
from pathlib import Path
from typing import Dict, Any, List
from pymongo import ASCENDING, DESCENDING
from pymongo.database import Database

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import COLLECTION_VALIDATED
from src.mongo_setup import get_mongo_client, get_database

INDEX_DEFINITIONS = [
    {
        "name": "idx_customer_id",
        "keys": [("customer_id", ASCENDING)],
        "type": "Single Field Index",
        "justification": "Accelerates customer profile lookups and customer purchase history queries (Q1). Eliminates full collection scan (COLLSCAN) for single-tenant filtering.",
        "collection": COLLECTION_VALIDATED
    },
    {
        "name": "idx_order_date_status",
        "keys": [("order_date", DESCENDING), ("status", ASCENDING)],
        "type": "Compound Index",
        "justification": "Optimizes time-series queries filtered by business order status (Q2). Allows MongoDB to satisfy equality-sort-range criteria efficiently via IXSCAN.",
        "collection": COLLECTION_VALIDATED
    },
    {
        "name": "idx_city_total_amount",
        "keys": [("city", ASCENDING), ("total_amount", DESCENDING)],
        "type": "Compound Index",
        "justification": "Serves geographical queries filtered by city and sorted by order amount (Q3). Covers filter and sort stages simultaneously without in-memory sort buffer.",
        "collection": COLLECTION_VALIDATED
    },
    {
        "name": "idx_items_sku",
        "keys": [("items.sku", ASCENDING)],
        "type": "Multikey Index",
        "justification": "Indexes elements inside the embedded items array for fast product lookups (Q4) without scanning nested document structures.",
        "collection": COLLECTION_VALIDATED
    }
]

def create_project_indexes(db: Database) -> List[Dict[str, Any]]:
    col = db[COLLECTION_VALIDATED]
    created = []
    for idx in INDEX_DEFINITIONS:
        name = col.create_index(idx["keys"], name=idx["name"])
        created.append({
            "name": name,
            "keys": idx["keys"],
            "type": idx["type"],
            "justification": idx["justification"]
        })
        print(f" [INDEX CREATED] {name} ({idx['type']}) on {col.name}")
    return created

def drop_project_indexes(db: Database) -> None:
    col = db[COLLECTION_VALIDATED]
    for idx in INDEX_DEFINITIONS:
        try:
            col.drop_index(idx["name"])
            print(f" [INDEX DROPPED] {idx['name']}")
        except Exception:
            pass

def run_explain_benchmark(db: Database) -> Dict[str, Any]:
    col = db[COLLECTION_VALIDATED]
    
    # 3 target queries to benchmark
    test_queries = [
        {
            "id": "Q1_CUSTOMER_ORDERS",
            "name": "Customer Purchase History",
            "filter": {"customer_id": "عميل-1000"},
            "sort": [("order_date", DESCENDING)],
            "associated_index": "idx_customer_id"
        },
        {
            "id": "Q2_DATE_RANGE_STATUS",
            "name": "Recent Completed Sales by Date and Status",
            "filter": {"order_date": {"$gte": "2025-01-01"}, "status": "DELIVERED"},
            "sort": [("order_date", DESCENDING)],
            "associated_index": "idx_order_date_status"
        },
        {
            "id": "Q3_CITY_HIGH_VALUE",
            "name": "Top High-Value Orders in Sanaa",
            "filter": {"city": "صنعاء", "total_amount": {"$gt": 50000.0}},
            "sort": [("total_amount", DESCENDING)],
            "associated_index": "idx_city_total_amount"
        }
    ]

    results = []

    # 1. Drop indexes to test BEFORE
    print("\n--- Benchmarking Phase A: WITHOUT INDEXES (COLLSCAN) ---")
    drop_project_indexes(db)
    
    for q in test_queries:
        cursor = col.find(q["filter"])
        if q.get("sort"):
            cursor = cursor.sort(q["sort"])
        plan_before = cursor.explain()["executionStats"]
        
        print(f"Testing {q['id']} without index...")
        q["before_stats"] = {
            "stage": plan_before["executionStages"].get("stage", "COLLSCAN"),
            "executionTimeMillis": plan_before.get("executionTimeMillis", 0),
            "totalDocsExamined": plan_before.get("totalDocsExamined", 0),
            "nReturned": plan_before.get("nReturned", 0)
        }

    # 2. Create indexes to test AFTER
    print("\n--- Benchmarking Phase B: WITH INDEXES (IXSCAN) ---")
    create_project_indexes(db)

    for q in test_queries:
        cursor = col.find(q["filter"])
        if q.get("sort"):
            cursor = cursor.sort(q["sort"])
        plan_after = cursor.explain()["executionStats"]
        print(f"Testing {q['id']} with index...")
        
        q["after_stats"] = {
            "stage": plan_after["executionStages"].get("stage", "IXSCAN"),
            "executionTimeMillis": plan_after.get("executionTimeMillis", 0),
            "totalDocsExamined": plan_after.get("totalDocsExamined", 0),
            "nReturned": plan_after.get("nReturned", 0),
            "totalKeysExamined": plan_after.get("totalKeysExamined", 0)
        }
        
        docs_before = max(1, q["before_stats"]["totalDocsExamined"])
        docs_after = q["after_stats"]["totalDocsExamined"]
        reduction = ((docs_before - docs_after) / docs_before) * 100
        efficiency_gain = f"{reduction:.1f}% reduction in docs scanned"

        results.append({
            "query_id": q["id"],
            "query_name": q["name"],
            "filter": str(q["filter"]),
            "associated_index": q["associated_index"],
            "before": q["before_stats"],
            "after": q["after_stats"],
            "efficiency_gain": efficiency_gain
        })

    return {
        "status": "SUCCESS",
        "benchmark_timestamp": time.time(),
        "total_queries_benchmarked": len(results),
        "comparisons": results
    }

if __name__ == "__main__":
    client = get_mongo_client()
    db = get_database(client)
    res = run_explain_benchmark(db)
    import json
    print("\n=== BENCHMARK EXPLAIN RESULTS ===")
    print(json.dumps(res, indent=2, ensure_ascii=False))
