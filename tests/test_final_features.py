"""
test_final_features.py - Comprehensive Unit and Integration Tests for Phase 2
Tests:
  1. Index creation and verification of Compound / Single-field indexes
  2. Execution of all 5 business queries
  3. Execution of all 5 aggregation reports
  4. Materialized Views incremental update and watermark management
  5. Scheduled job execution and execution log auditing
  6. FastAPI endpoints validation
"""

import unittest
from fastapi.testclient import TestClient

from src.mongo_setup import get_mongo_client, get_database
from src.indexes import create_project_indexes, INDEX_DEFINITIONS
from src.queries import QUERY_DISPATCHER, execute_named_query
from src.aggregations import REPORTS_REGISTRY, execute_aggregation_report
from src.materialized_views import (
    refresh_daily_sales_mv,
    refresh_top_products_mv,
    get_watermark
)
from src.scheduler import run_job_manually, get_job_execution_history
from src.api import app
from config.settings import (
    COLLECTION_VALIDATED,
    COLLECTION_MV_DAILY_SALES,
    COLLECTION_MV_TOP_PRODUCTS
)

class TestFinalProjectFeatures(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.client = get_mongo_client()
        cls.db = get_database(cls.client)
        cls.api_client = TestClient(app)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def test_01_indexes_created(self):
        created = create_project_indexes(self.db)
        self.assertGreaterEqual(len(created), 3)
        
        # Verify at least one compound index
        compound_indices = [idx for idx in INDEX_DEFINITIONS if idx["type"] == "Compound Index"]
        self.assertGreaterEqual(len(compound_indices), 1)

    def test_02_all_five_queries_return_results(self):
        for q_name in QUERY_DISPATCHER:
            res = execute_named_query(self.db, q_name, limit=5)
            self.assertIn("results", res)
            self.assertEqual(res["query_name"], q_name)
            self.assertIsInstance(res["results"], list)

    def test_03_all_five_aggregations_return_data(self):
        for r_name in REPORTS_REGISTRY:
            res = execute_aggregation_report(self.db, r_name, limit=3)
            self.assertIn("data", res)
            self.assertEqual(res["report_name"], r_name)
            self.assertIsInstance(res["data"], list)

    def test_04_materialized_views_incremental(self):
        res_daily = refresh_daily_sales_mv(self.db, full_refresh=False)
        self.assertIn(res_daily["status"], ["REFRESHED", "UP_TO_DATE"])
        
        res_prod = refresh_top_products_mv(self.db, full_refresh=False)
        self.assertIn(res_prod["status"], ["REFRESHED", "UP_TO_DATE"])
        
        wm = get_watermark(self.db, COLLECTION_MV_DAILY_SALES)
        self.assertNotEqual(wm, "1970-01-01T00:00:00")

    def test_05_scheduled_jobs_execution_and_logging(self):
        res1 = run_job_manually("refresh_materialized_views_job")
        self.assertEqual(res1["status"], "SUCCESS")
        
        res2 = run_job_manually("daily_performance_report_job")
        self.assertEqual(res2["status"], "SUCCESS")
        
        history = get_job_execution_history(self.db, limit=5)
        self.assertGreater(len(history), 0)
        self.assertEqual(history[0]["status"], "SUCCESS")

    def test_06_fastapi_endpoints(self):
        # Health check
        r_health = self.api_client.get("/health")
        self.assertEqual(r_health.status_code, 200)
        self.assertEqual(r_health.json()["status"], "HEALTHY")
        
        # Queries listing
        r_queries = self.api_client.get("/queries")
        self.assertEqual(r_queries.status_code, 200)
        self.assertEqual(r_queries.json()["total_queries"], 5)
        
        # Aggregations listing
        r_aggs = self.api_client.get("/aggregations")
        self.assertEqual(r_aggs.status_code, 200)
        self.assertEqual(r_aggs.json()["total_aggregations"], 5)
        
        # Jobs listing
        r_jobs = self.api_client.get("/jobs")
        self.assertEqual(r_jobs.status_code, 200)
        self.assertEqual(len(r_jobs.json()["registered_jobs"]), 2)

if __name__ == "__main__":
    unittest.main()
