import json
import os
import random
import time
from datetime import datetime, timedelta

# Configuration
output_dir = "stream_data"
os.makedirs(output_dir, exist_ok=True)

stations_capacity = {
    "ST01": 5,
    "ST02": 10,
    "ST03": 3,
    "ST04": 7,
    "ST05": 4,
    "ST06": 6,
    "ST07": 8,
    "ST08": 2,
    "ST09": 9,
    "ST10": 5
}

status_options = ["charging", "not_charging"]

# Define desired target utilization (in %)
target_utilization = {
    "ST01": 0.9,
    "ST02": 0.8,
    "ST03": 0.3,
    "ST04": 0.5,
    "ST05": 0.2,
    "ST06": 0.6,
    "ST07": 0.7,
    "ST08": 0.1,
    "ST09": 0.9,
    "ST10": 0.4
}

def generate_status(station_id, charger_id):
    """
    Generate a charging status based on target utilization for the station.
    """
    utilization = target_utilization[station_id]
    status = "charging" if random.random() < utilization else "not_charging"
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "station_id": station_id,
        "charger_id": f"{station_id}_CH{charger_id}",
        "status": status
    }

def cleanup_old_files(directory, max_age_minutes=60):
    """
    Delete files older than `max_age_minutes` in the specified directory.
    """
    now = time.time()
    cutoff = now - (max_age_minutes * 60)
    deleted_count = 0

    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            if os.path.getmtime(filepath) < cutoff:
                os.remove(filepath)
                deleted_count += 1

    if deleted_count > 0:
        print(f"[{datetime.now()}] Deleted {deleted_count} old files from {directory}")

def main():
    iteration = 0
    while True:
        records = []
        for station_id, capacity in stations_capacity.items():
            for charger_id in range(1, capacity + 1):
                record = generate_status(station_id, charger_id)
                records.append(record)

        timestamp_str = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        filename = f"{output_dir}/ev_data_{timestamp_str}.json"
        with open(filename, "w") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")

        print(f"[{datetime.now()}] Generated file: {filename} with {len(records)} records")

        # Cleanup every 6 iterations (~every 1 minute if sleep=10s)
        if iteration % 6 == 0:
            cleanup_old_files(output_dir)

        iteration += 1
        time.sleep(60)

if __name__ == "__main__":
    main()
