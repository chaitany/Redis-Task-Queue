"""Interactive demo of the distributed task scheduler."""

import subprocess
import sys
import time

PYTHON = sys.executable
CLI = "main.py"


def run(cmd: str) -> str:
    print(f"\n$ dtask {cmd}")
    result = subprocess.run(
        [PYTHON, CLI] + cmd.split(),
        capture_output=True, text=True
    )
    output = result.stdout + result.stderr
    print(output.strip())
    return output


def main():
    print("=" * 60)
    print("  DISTRIBUTED TASK SCHEDULER - DEMO")
    print("=" * 60)

    run("purge all")

    print("\n--- Step 1: Check system info ---")
    run("info")

    print("\n--- Step 2: Submit tasks ---")
    run('submit echo {"message":"hello_world"}')
    run('submit add {"a":42,"b":58}')
    run('submit flaky_job {"fail_rate":0.6} --retries 3')
    run('submit slow_job {"duration":2}')
    run('submit divide {"a":100,"b":5}')
    run('submit echo {"message":"delayed"} --delay 5')

    print("\n--- Step 3: View queued tasks ---")
    run("list")

    print("\n--- Step 4: Starting worker (processing tasks...) ---")
    worker = subprocess.Popen(
        [PYTHON, CLI, "worker", "--concurrency", "2", "-v"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )

    time.sleep(10)
    worker.terminate()
    try:
        worker.wait(timeout=5)
    except subprocess.TimeoutExpired:
        worker.kill()

    print("\n--- Step 5: Check results ---")
    run("list")

    print("\n--- Step 6: System status ---")
    run("info")

    print("\n" + "=" * 60)
    print("  DEMO COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
