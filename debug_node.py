"""Debug script to test node startup."""

import subprocess
import sys
import time

import requests

# Start a single node
cmd = [
    sys.executable,
    "-m",
    "raft.cli",
    "--id",
    "0",
    "--udp-port",
    "10000",
    "--http-port",
    "20000",
    "--peers",
    '[["localhost", 10001]]',
    "--peer-ids",
    "[1]",
]

print(f"Starting node with command: {' '.join(cmd)}")
proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

print(f"Node started with PID: {proc.pid}")
time.sleep(1)  # Give it time to start

# Check if process is still running
if proc.poll() is not None:
    print(f"ERROR: Process exited with code {proc.returncode}")
    stdout, stderr = proc.communicate()
    print(f"STDOUT:\n{stdout}")
    print(f"STDERR:\n{stderr}")
    sys.exit(1)

# Try to connect to HTTP server
try:
    response = requests.get("http://localhost:20000/health", timeout=2)
    print(f"Health check: {response.status_code}")
    print(f"Response: {response.json()}")
except Exception as e:
    print(f"ERROR: Failed to connect to HTTP server: {e}")
    stdout, stderr = proc.communicate(timeout=1)
    print(f"STDOUT:\n{stdout}")
    print(f"STDERR:\n{stderr}")
    proc.kill()
    sys.exit(1)

# Try to tick the node
try:
    response = requests.post("http://localhost:20000/tick", timeout=2)
    print(f"Tick response: {response.status_code}")
    print(f"Response: {response.json()}")
except Exception as e:
    print(f"ERROR: Failed to tick node: {e}")

# Get state
try:
    response = requests.get("http://localhost:20000/state", timeout=2)
    print(f"State: {response.json()}")
except Exception as e:
    print(f"ERROR: Failed to get state: {e}")

# Cleanup
proc.terminate()
proc.wait(timeout=2)
print("Node stopped successfully")
