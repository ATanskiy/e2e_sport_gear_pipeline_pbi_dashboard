import subprocess
import time
import sys

# 1. Use the same Python interpreter that runs this script
python_exec = sys.executable

# 2. Start Docker
print("Starting Docker containers...")
subprocess.run(["docker-compose", "up", "-d"], check=True)

print("Waiting for services to become available...")
time.sleep(4)

# 3. Scripts to run in order
scripts = [
    "scripts/1_download_to_s3_raw.py",
    "scripts/2_create_schemas_tables.py",
    "scripts/3_load_dim_tables.py",
    "scripts/4_extract_raw_to_s3_daily.py"
]

# 4. Run each script using the same Python interpreter
for script in scripts:
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
