"""
mongo_setup.py - MongoDB Database and Collection Setup

Initializes collections and indexes for ELT Pipeline.
Ensures idempotency by setting a Unique Index on `order_id` in `orders_validated`.
"""

import sys
from pathlib import Path
from typing import Optional
from pymongo import MongoClient, ASCENDING
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, OperationFailure

# Add project root to sys.path
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    MONGO_URI,
    MONGO_DB_NAME,
    COLLECTION_RAW,
    COLLECTION_VALIDATED,
    COLLECTION_QUARANTINE,
)
def get_mongo_client(uri: str = MONGO_URI, server_selection_timeout_ms: int = 5000) -> MongoClient:
    """
    Creates and returns a thread-safe PyMongo MongoClient instance.
    Validates connection via server_info ping.
    """
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=server_selection_timeout_ms)
        # Verify connection
        client.admin.command("ping")
        return client
    except ConnectionFailure as err:
        print(f"[ERROR] Could not connect to MongoDB at {uri}: {err}")
        raise

def get_database(client: Optional[MongoClient] = None, db_name: str = MONGO_DB_NAME) -> Database:
    """Returns database handle from given client or creates a new client."""
    if client is None:
        client = get_mongo_client()
    return client[db_name]
def setup_mongo_collections(db: Database) -> dict:
    """
    Initializes collections and creates essential indexes.
    
    Key Index:
      - `orders_validated`: Unique Index on `order_id` (ensures Idempotency & Upsert integrity).
      - `orders_raw`: Index on (`run_id`, `source_row_number`) for audit tracking.
      - `orders_quarantine`: Index on `run_id` and `error_code` for diagnostic analytics.
    """
    print(f"\n[MongoDB Setup] Initializing database: '{db.name}'...")
    
    # 1. Setup orders_raw collection & indexes
    raw_col = db[COLLECTION_RAW]
    raw_col.create_index([("run_id", ASCENDING), ("source_row_number", ASCENDING)], name="idx_raw_run_row")
    raw_col.create_index([("order_id", ASCENDING)], name="idx_raw_order_id")
    print(f" - Collection '{COLLECTION_RAW}': Indexes verified.")

    # 2. Setup orders_validated collection & UNIQUE index on order_id
    val_col = db[COLLECTION_VALIDATED]
    val_col.create_index([("order_id", ASCENDING)], unique=True, name="idx_unique_order_id")
    val_col.create_index([("run_id", ASCENDING)], name="idx_val_run_id")
    val_col.create_index([("status", ASCENDING)], name="idx_val_status")
    val_col.create_index([("order_date", ASCENDING)], name="idx_val_date")
    print(f" - Collection '{COLLECTION_VALIDATED}': UNIQUE Index on 'order_id' verified (Idempotency guarantee).")
    # 3. Setup orders_quarantine collection & indexes
    quar_col = db[COLLECTION_QUARANTINE]
    quar_col.create_index([("run_id", ASCENDING)], name="idx_quar_run_id")
    quar_col.create_index([("error_code", ASCENDING)], name="idx_quar_error_code")
    print(f" - Collection '{COLLECTION_QUARANTINE}': Error diagnostic indexes verified.")

    print("[MongoDB Setup] All collections and indexes are ready.\n")
    return {
        "database": db.name,
        "collections": [COLLECTION_RAW, COLLECTION_VALIDATED, COLLECTION_QUARANTINE],
        "unique_index": f"{COLLECTION_VALIDATED}.order_id (UNIQUE)"
    }
def reset_collections(db: Database) -> None:
    """Drops pipeline collections for clean test/demo runs."""
    print(f"[MongoDB Reset] Dropping existing pipeline collections in '{db.name}'...")
    for col_name in [COLLECTION_RAW, COLLECTION_VALIDATED, COLLECTION_QUARANTINE]:
        db[col_name].drop()
        print(f" - Dropped: {col_name}")
    print("[MongoDB Reset] Done. Re-creating indexes...")
    setup_mongo_collections(db)

if __name__ == "__main__":
    client = None
    try:
        client = get_mongo_client()
        db = get_database(client)
        setup_mongo_collections(db)
    except Exception as e:
        print(f"[FATAL] MongoDB setup failed: {e}")
    finally:
        if client:
            client.close()
            print("[MongoDB] Connection closed cleanly.")
