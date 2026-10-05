"""
api.py - Unified FastAPI REST Execution Interface
Big Data Final Project - Requirement 0: Unified API

Endpoints Implemented:
  - GET  /health              -> System health, MongoDB status, collection document counts
  - POST /ingest              -> Triggers ELT ingestion via existing pipeline
  - POST /indexes             -> Creates project indexes & runs explain benchmark
  - GET  /queries             -> Lists all 5 available business queries
  - GET  /queries/{name}      -> Executes a specific query with query params
  - GET  /aggregations        -> Lists all 5 aggregation reports
  - GET  /aggregations/{name} -> Executes a specific aggregation report
  - POST /refresh-mv          -> Triggers incremental refresh of Materialized Views
  - GET  /jobs                -> Lists scheduled jobs and recent execution history
  - POST /jobs/{name}/run     -> Triggers immediate manual execution of a scheduled job
"""

import sys
import os
from pathlib import Path
from typing import Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    MONGO_DB_NAME,
    COLLECTION_RAW,
    COLLECTION_VALIDATED,
    COLLECTION_QUARANTINE,
    COLLECTION_MV_DAILY_SALES,
    COLLECTION_MV_TOP_PRODUCTS,
    COLLECTION_JOB_LOGS
)
from src.mongo_setup import get_mongo_client, get_database
from src.indexes import create_project_indexes, run_explain_benchmark, INDEX_DEFINITIONS
from src.queries import execute_named_query, QUERIES_METADATA, QUERY_DISPATCHER
from src.aggregations import execute_aggregation_report, AGGREGATIONS_METADATA, REPORTS_REGISTRY
from src.materialized_views import refresh_all_materialized_views
from src.scheduler import (
    start_background_scheduler,
    stop_background_scheduler,
    run_job_manually,
    get_job_execution_history,
    REGISTERED_JOBS
)
from src.elt_pipeline import run_pipeline

app = FastAPI(
    title="Big Data Hybrid ELT & Analytics API",
    description="Unified Execution Interface for Big Data Project (Phase 2 - Final Evaluation)",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

@app.on_event("startup")
def on_startup():
    start_background_scheduler()

@app.on_event("shutdown")
def on_shutdown():
    stop_background_scheduler()

class IngestRequest(BaseModel):
    file_path: Optional[str] = "data/orders_sample.csv"
    reset_db: Optional[bool] = False

@app.get("/health", tags=["System"])
def health_check():
    try:
        client = get_mongo_client()
        db = get_database(client)
        db.command("ping")
        
        counts = {
            COLLECTION_RAW: db[COLLECTION_RAW].count_documents({}),
            COLLECTION_VALIDATED: db[COLLECTION_VALIDATED].count_documents({}),
            COLLECTION_QUARANTINE: db[COLLECTION_QUARANTINE].count_documents({}),
            COLLECTION_MV_DAILY_SALES: db[COLLECTION_MV_DAILY_SALES].count_documents({}),
            COLLECTION_MV_TOP_PRODUCTS: db[COLLECTION_MV_TOP_PRODUCTS].count_documents({}),
            COLLECTION_JOB_LOGS: db[COLLECTION_JOB_LOGS].count_documents({})
        }
        client.close()
        return {
            "status": "HEALTHY",
            "database": MONGO_DB_NAME,
            "mongodb_connected": True,
            "collections_count": counts
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database connection error: {str(e)}")

@app.post("/ingest", tags=["Pipeline"])
def trigger_ingest(payload: IngestRequest):
    try:
        target_path = payload.file_path
        if not os.path.isabs(target_path):
            target_path = str((PROJECT_ROOT / target_path).resolve())
            
        metrics = run_pipeline(
            file_path=target_path,
            reset_db=payload.reset_db
        )
        return {
            "status": "SUCCESS",
            "message": "Ingestion and ELT pipeline completed successfully.",
            "metrics": metrics
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/indexes", tags=["Indexes & Optimization"])
def create_and_benchmark_indexes():
    try:
        client = get_mongo_client()
        db = get_database(client)
        created = create_project_indexes(db)
        benchmark = run_explain_benchmark(db)
        client.close()
        return {
            "status": "SUCCESS",
            "indexes_created": created,
            "explain_benchmark": benchmark
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/queries", tags=["Queries"])
def list_queries():
    return {
        "total_queries": len(QUERIES_METADATA),
        "queries": QUERIES_METADATA
    }

@app.get("/queries/{name}", tags=["Queries"])
def execute_query(
    name: str,
    customer_id: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    min_amount: Optional[float] = Query(None),
    sku: Optional[str] = Query(None),
    payment_method: Optional[str] = Query(None),
    payment_status: Optional[str] = Query(None),
    limit: Optional[int] = Query(50)
):
    try:
        client = get_mongo_client()
        db = get_database(client)
        result = execute_named_query(
            db=db,
            name=name,
            customer_id=customer_id,
            start_date=start_date,
            end_date=end_date,
            status=status,
            city=city,
            min_amount=min_amount,
            sku=sku,
            payment_method=payment_method,
            payment_status=payment_status,
            limit=limit
        )
        client.close()
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/aggregations", tags=["Aggregations"])
def list_aggregations():
    return {
        "total_aggregations": len(AGGREGATIONS_METADATA),
        "reports": AGGREGATIONS_METADATA
    }

@app.get("/aggregations/{name}", tags=["Aggregations"])
def execute_aggregation(name: str, limit: Optional[int] = Query(20)):
    try:
        client = get_mongo_client()
        db = get_database(client)
        res = execute_aggregation_report(db, name, limit=limit)
        client.close()
        return res
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/refresh-mv", tags=["Materialized Views"])
def trigger_refresh_mv(full_refresh: bool = False):
    try:
        client = get_mongo_client()
        db = get_database(client)
        res = refresh_all_materialized_views(db, full_refresh=full_refresh)
        client.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/jobs", tags=["Scheduled Jobs"])
def list_jobs(limit: int = 20):
    try:
        client = get_mongo_client()
        db = get_database(client)
        history = get_job_execution_history(db, limit=limit)
        client.close()
        
        job_defs = [
            {"job_name": k, "description": v["description"], "default_interval_minutes": v["default_interval_minutes"]}
            for k, v in REGISTERED_JOBS.items()
        ]
        return {
            "registered_jobs": job_defs,
            "recent_execution_logs_count": len(history),
            "execution_history": history
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/jobs/{name}/run", tags=["Scheduled Jobs"])
def run_job(name: str):
    try:
        result = run_job_manually(name)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api:app", host="0.0.0.0", port=8000, reload=True)
