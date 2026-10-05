from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')
db = client['bigdata_orders_db']

res = db['orders_quarantine'].update_many(
    {'status': None},
    {'': {
        'status': 'UNREPAIRABLE_FATAL',
        'resolution_action': 'PERMANENTLY_QUARANTINED',
        'repair_notes': 'Fatal defect: impossible date or severely truncated items json that cannot be recovered with confidence.'
    }}
)
print('Updated unrepairable count:', res.modified_count)
