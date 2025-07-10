import subprocess
import time
import os
import sys
from config import SCRIPTS

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import TIME_TO_SLEEP

# 1. Use the same Python interpreter that runs this script
python_exec = sys.executable

# 2. Start Docker
print("Starting Docker containers...")
subprocess.run(["docker-compose", "up", "-d"], check=True)

print("Waiting for services to become available...")
time.sleep(TIME_TO_SLEEP)

# 3. Run each script using the same Python interpreter
for script in SCRIPTS:
    print(f"\nRunning {script}...\n{'-'*50}")
    process = subprocess.Popen([python_exec, "-u", script], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    for line in process.stdout:
        print(line, end="")

    process.wait()
    if process.returncode != 0:
        print(f"\nScript failed: {script}")
        break
    print(f"\nFinished {script}\n{'='*50}")

print("\nAll scripts finished (or stopped on error).")
