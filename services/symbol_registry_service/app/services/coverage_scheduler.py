# app/services/coverage_scheduler.py

import time
from app.services.coverage_service import update_all_symbols


def run_scheduler(interval_minutes=60):
    print("Coverage Scheduler Started")

    while True:
        print("Running coverage update...")

        results = update_all_symbols()

        print(f"Updated {len(results)} symbols")

        time.sleep(interval_minutes * 60)


if __name__ == "__main__":
    run_scheduler(interval_minutes=60)