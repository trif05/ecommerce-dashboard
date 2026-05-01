import time
import subprocess
import sys

print("Pipeline runner started - runs every 5 minutes")

while True:
    print("\nRunning silver transformer...")
    subprocess.run([sys.executable, "src/silver/silver_transformer.py"], check=True)

    print("Running gold aggregator...")
    subprocess.run([sys.executable, "src/gold/gold_aggregator.py"], check=True)

    print("Pipeline complete - waiting 5 minutes...")
    time.sleep(300)