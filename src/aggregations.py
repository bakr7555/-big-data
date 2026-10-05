"""
aggregations.py - 5 Production Aggregation Reports for Big Data Pipeline
Big Data Final Project - Requirement 2: Aggregation Reports
"""

import sys
from pathlib import Path
from typing import Dict, Any, List
from pymongo.database import Database

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import COLLECTION_VALIDATED
from src.mongo_setup import get_mongo_client, get_database

AGGREGATIONS_METADATA = [
    {
        "name": "sales_by_city",
        "title": "Revenue and Order Metrics by City",
        "description": "Calculates total sales revenue, order counts, and average order value per city."
    },
    {
        "name": "top_products",
        "title": "Top Selling Products by Volume and Revenue",
        "description": "Unwinds items array to aggregate total units sold and gross product revenue."
    },
    {
        "name": "top_customers",
        "title": "High-Value VIP Customers",
        "description": "Ranks top spending customers by total gross spend and frequency."
    },
    {
        "name": "monthly_sales_trend",
        "title": "Monthly Revenue and Delivery Costs Trend",
        "description": "Groups orders by year-month to analyze commercial velocity and shipping overheads."
    },
    {
        "name": "order_status_distribution",
        "title": "Fulfillment Status Distribution and Proportions",
        "description": "Computes volume and percentage share across all standardized order statuses."
    }
]

def report_sales_by_city(db: Database, limit: int = 20) -> List[Dict[str, Any]]:
    col = db[COLLECTION_VALIDATED]
    pipeline = [
        {"$match": {"city": {"$ne": None, "$ne": ""}}},
        {
            "$group": {
                "_id": "$city",
                "total_sales": {"$sum": "$total_amount"},
                "order_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$total_amount"},
                "min_order": {"$min": "$total_amount"},
                "max_order": {"$max": "$total_amount"}
            }
        },
        {"$sort": {"total_sales": -1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "city": "$_id",
                "total_sales": {"$round": ["$total_sales", 2]},
                "order_count": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "min_order": {"$round": ["$min_order", 2]},
                "max_order": {"$round": ["$max_order", 2]}
            }
        }
    ]
    return list(col.aggregate(pipeline))

def report_top_products(db: Database, limit: int = 15) -> List[Dict[str, Any]]:
    col = db[COLLECTION_VALIDATED]
    pipeline = [
        {"$unwind": "$items"},
        {
            "$group": {
                "_id": "$items.sku",
                "product_name": {"$first": "$items.name"},
                "total_units_sold": {"$sum": "$items.qty"},
                "total_revenue": {"$sum": "$items.total"},
                "avg_unit_price": {"$avg": "$items.unit_price"}
            }
        },
        {"$sort": {"total_revenue": -1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "sku": "$_id",
                "product_name": 1,
                "total_units_sold": 1,
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "avg_unit_price": {"$round": ["$avg_unit_price", 2]}
            }
        }
    ]
    return list(col.aggregate(pipeline))

def report_top_customers(db: Database, limit: int = 10) -> List[Dict[str, Any]]:
    col = db[COLLECTION_VALIDATED]
    pipeline = [
        {"$match": {"customer_id": {"$ne": None, "$ne": ""}}},
        {
            "$group": {
                "_id": "$customer_id",
                "customer_name": {"$first": "$customer_name"},
                "customer_phone": {"$first": "$customer_phone"},
                "city": {"$first": "$city"},
                "total_spent": {"$sum": "$total_amount"},
                "order_count": {"$sum": 1},
                "last_order_date": {"$max": "$order_date"}
            }
        },
        {"$sort": {"total_spent": -1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "customer_id": "$_id",
                "customer_name": 1,
                "customer_phone": 1,
                "city": 1,
                "total_spent": {"$round": ["$total_spent", 2]},
                "order_count": 1,
                "last_order_date": 1
            }
        }
    ]
    return list(col.aggregate(pipeline))

def report_monthly_sales_trend(db: Database, limit: int = 24) -> List[Dict[str, Any]]:
    col = db[COLLECTION_VALIDATED]
    pipeline = [
        {"$match": {"order_date": {"$regex": r"^\d{4}-\d{2}"}}},
        {
            "$project": {
                "year_month": {"$substr": ["$order_date", 0, 7]},
                "total_amount": 1,
                "delivery_cost": 1
            }
        },
        {
            "$group": {
                "_id": "$year_month",
                "monthly_revenue": {"$sum": "$total_amount"},
                "orders_count": {"$sum": 1},
                "total_delivery_cost": {"$sum": "$delivery_cost"},
                "avg_order_value": {"$avg": "$total_amount"}
            }
        },
        {"$sort": {"_id": 1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "year_month": "$_id",
                "monthly_revenue": {"$round": ["$monthly_revenue", 2]},
                "orders_count": 1,
                "total_delivery_cost": {"$round": ["$total_delivery_cost", 2]},
                "avg_order_value": {"$round": ["$avg_order_value", 2]}
            }
        }
    ]
    return list(col.aggregate(pipeline))

def report_order_status_distribution(db: Database) -> List[Dict[str, Any]]:
    col = db[COLLECTION_VALIDATED]
    pipeline = [
        {
            "$group": {
                "_id": "$status",
                "count": {"$sum": 1},
                "total_value": {"$sum": "$total_amount"}
            }
        },
        {"$sort": {"count": -1}},
        {
            "$project": {
                "_id": 0,
                "status": "$_id",
                "count": 1,
                "total_value": {"$round": ["$total_value", 2]}
            }
        }
    ]
    return list(col.aggregate(pipeline))

REPORTS_REGISTRY = {
    "sales_by_city": report_sales_by_city,
    "top_products": report_top_products,
    "top_customers": report_top_customers,
    "monthly_sales_trend": report_monthly_sales_trend,
    "order_status_distribution": report_order_status_distribution
}

def execute_aggregation_report(db: Database, name: str, **kwargs) -> Dict[str, Any]:
    func = REPORTS_REGISTRY.get(name.lower())
    if not func:
        valid_reports = list(REPORTS_REGISTRY.keys())
        raise ValueError(f"Unknown report '{name}'. Available: {valid_reports}")
    
    import inspect
    sig = inspect.signature(func)
    filtered = {k: v for k, v in kwargs.items() if k in sig.parameters and v is not None}
    
    data = func(db, **filtered)
    return {
        "report_name": name,
        "rows_returned": len(data),
        "data": data
    }

if __name__ == "__main__":
    client = get_mongo_client()
    db = get_database(client)
    for r_name in REPORTS_REGISTRY:
        res = execute_aggregation_report(db, r_name, limit=3)
        print(f"[TEST REPORT] {r_name} -> returned {res['rows_returned']} rows")
