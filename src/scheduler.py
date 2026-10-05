"""
scheduler.py - Production Scheduled Jobs and Execution Logging Engine
Big Data Final Project - Requirement 4: Scheduled Jobs

Implements 2 Scheduled Jobs:
  1. refresh_materialized_views_job: Incremental update of daily_sales_summary and top_products_summary.
  2. daily_performance_report_job: Computes daily pipeline SLA, throughput, and system health metrics.

Features:
  - Backed by APScheduler BackgroundScheduler (Cron & Interval triggers).
  - Manual on-demand execution support (`run_job_manually(job_name)`).
  - Robust execution logging in MongoDB collection `job_execution_logs` (started_at, finished_at, duration_ms, status, details).
"""

import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
from pymongo import DESCENDING
from pymongo.database import Database
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    COLLECTION_JOB_LOGS,
    COLLECTION_VALIDATED,
    COLLECTION_RAW,
    COLLECTION_QUARANTINE
)
from src.mongo_setup import get_mongo_client, get_database
from src.materialized_views import refresh_all_materialized_views

_SCHEDULER_INSTANCE: Optional[BackgroundScheduler] = None

def log_job_execution(
    db: Database,
    job_name: str,
    trigger_type: str,
    status: str,
    started_at: datetime,
    finished_at: datetime,
    duration_ms: float,
    details: Dict[str, Any],
    error_message: Optional[str] = None
) -> str:
    col = db[COLLECTION_JOB_LOGS]
    doc = {
        "job_name": job_name,
        "trigger_type": trigger_type,
        "status": status,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_ms": round(duration_ms, 2),
        "details": details,
        "error_message": error_message,
        "logged_at": datetime.utcnow().isoformat()
    }
    res = col.insert_one(doc)
    return str(res.inserted_id)

def job_refresh_materialized_views(trigger_type: str = "SCHEDULED") -> Dict[str, Any]:
    client = get_mongo_client()
    db = get_database(client)
    t_start = datetime.utcnow()
    t_perf_start = time.time()
    job_name = "refresh_materialized_views_job"
    
    try:
        res = refresh_all_materialized_views(db, full_refresh=False)
        t_finish = datetime.utcnow()
        duration_ms = (time.time() - t_perf_start) * 1000
        
        log_job_execution(
            db=db,
            job_name=job_name,
            trigger_type=trigger_type,
            status="SUCCESS",
            started_at=t_start,
            finished_at=t_finish,
            duration_ms=duration_ms,
            details=res
        )
        return {"status": "SUCCESS", "job_name": job_name, "duration_ms": duration_ms, "result": res}
    except Exception as e:
        t_finish = datetime.utcnow()
        duration_ms = (time.time() - t_perf_start) * 1000
        log_job_execution(
            db=db,
            job_name=job_name,
            trigger_type=trigger_type,
            status="FAILED",
            started_at=t_start,
            finished_at=t_finish,
            duration_ms=duration_ms,
            details={},
            error_message=str(e)
        )
        return {"status": "FAILED", "job_name": job_name, "error": str(e)}
    finally:
        client.close()

def job_daily_performance_report(trigger_type: str = "SCHEDULED") -> Dict[str, Any]:
    client = get_mongo_client()
    db = get_database(client)
    t_start = datetime.utcnow()
    t_perf_start = time.time()
    job_name = "daily_performance_report_job"
    
    try:
        raw_count = db[COLLECTION_RAW].count_documents({})
        val_count = db[COLLECTION_VALIDATED].count_documents({})
        quar_count = db[COLLECTION_QUARANTINE].count_documents({})
        resolved_quar = db[COLLECTION_QUARANTINE].count_documents({"status": "RESOLVED"})
        
        metrics = {
            "snapshot_timestamp": datetime.utcnow().isoformat(),
            "total_raw_ingested": raw_count,
            "total_validated_orders": val_count,
            "total_quarantined_records": quar_count,
            "quarantined_repaired_count": resolved_quar,
            "pipeline_recovery_rate": f"{(resolved_quar / quar_count * 100):.2f}%" if quar_count else "100%"
        }
        
        t_finish = datetime.utcnow()
        duration_ms = (time.time() - t_perf_start) * 1000
        
        log_job_execution(
            db=db,
            job_name=job_name,
            trigger_type=trigger_type,
            status="SUCCESS",
            started_at=t_start,
            finished_at=t_finish,
            duration_ms=duration_ms,
            details=metrics
        )
        return {"status": "SUCCESS", "job_name": job_name, "duration_ms": duration_ms, "result": metrics}
    except Exception as e:
        t_finish = datetime.utcnow()
        duration_ms = (time.time() - t_perf_start) * 1000
        log_job_execution(
            db=db,
            job_name=job_name,
            trigger_type=trigger_type,
            status="FAILED",
            started_at=t_start,
            finished_at=t_finish,
            duration_ms=duration_ms,
            details={},
            error_message=str(e)
        )
        return {"status": "FAILED", "job_name": job_name, "error": str(e)}
    finally:
        client.close()

REGISTERED_JOBS = {
    "refresh_materialized_views_job": {
        "func": job_refresh_materialized_views,
        "description": "Incrementally updates daily_sales_summary and top_products_summary MVs",
        "default_interval_minutes": 30
    },
    "daily_performance_report_job": {
        "func": job_daily_performance_report,
        "description": "Calculates health metrics, recovery rate, and total volume snapshots",
        "default_interval_minutes": 60
    }
}

def run_job_manually(job_name: str) -> Dict[str, Any]:
    job_info = REGISTERED_JOBS.get(job_name)
    if not job_info:
        valid_jobs = list(REGISTERED_JOBS.keys())
        raise ValueError(f"Job '{job_name}' not registered. Available: {valid_jobs}")
    
    func = job_info["func"]
    return func(trigger_type="MANUAL")

def get_job_execution_history(db: Database, limit: int = 20) -> List[Dict[str, Any]]:
    col = db[COLLECTION_JOB_LOGS]
    cursor = col.find({}, {"_id": 0}).sort("started_at", DESCENDING).limit(limit)
    return list(cursor)

def start_background_scheduler() -> BackgroundScheduler:
    global _SCHEDULER_INSTANCE
    if _SCHEDULER_INSTANCE and _SCHEDULER_INSTANCE.running:
        return _SCHEDULER_INSTANCE
        
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        func=lambda: job_refresh_materialized_views(trigger_type="SCHEDULED"),
        trigger=IntervalTrigger(minutes=30),
        id="refresh_materialized_views_job",
        name="Refresh Materialized Views Job",
        replace_existing=True
    )
    scheduler.add_job(
        func=lambda: job_daily_performance_report(trigger_type="SCHEDULED"),
        trigger=IntervalTrigger(minutes=60),
        id="daily_performance_report_job",
        name="Daily Pipeline Performance Report Job",
        replace_existing=True
    )
    
    scheduler.start()
    _SCHEDULER_INSTANCE = scheduler
    print("[SCHEDULER] Background scheduler started with 2 registered jobs.")
    return scheduler

def stop_background_scheduler() -> None:
    global _SCHEDULER_INSTANCE
    if _SCHEDULER_INSTANCE and _SCHEDULER_INSTANCE.running:
        _SCHEDULER_INSTANCE.shutdown()
        print("[SCHEDULER] Background scheduler stopped cleanly.")

if __name__ == "__main__":
    print("\n=== TESTING MANUAL JOB EXECUTION ===")
    res1 = run_job_manually("refresh_materialized_views_job")
    print(f"Job 1 result: {res1['status']} in {res1['duration_ms']:.2f}ms")
    
    res2 = run_job_manually("daily_performance_report_job")
    print(f"Job 2 result: {res2['status']} in {res2['duration_ms']:.2f}ms")
    
    client = get_mongo_client()
    db = get_database(client)
    history = get_job_execution_history(db, limit=5)
    print(f"Execution history logs count in DB: {len(history)}")
