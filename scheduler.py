import asyncio
import os
import sys
import subprocess
import time
import traceback
from dotenv import load_dotenv

# --- Configuration ---
PIPELINE_BASE_DIR = "pipelines"
PIPELINE_NAMES =  [
    "pipeline_hubspot_to_postgres",
    "pipeline_wrike_to_postgres",
    "pipeline_xero_to_postgres",
]

# Load .env from the root directory ONCE at the start
print("[Scheduler] Loading environment variables from .env file...")
load_dotenv(verbose=True) # verbose=True helps confirm which .env is loaded

# Read the single interval for all pipelines from the loaded environment
DEFAULT_INTERVAL_SECONDS = 300  # Default 5 minutes
try:
    # Read from os.environ, which load_dotenv updated
    SHARED_INTERVAL_SECONDS = int(os.environ.get("PIPELINE_INTERVAL_SECONDS", DEFAULT_INTERVAL_SECONDS))
    if SHARED_INTERVAL_SECONDS <= 0:
        print(f"[Scheduler] Warning: Invalid (<=0) PIPELINE_INTERVAL_SECONDS found. Using default {DEFAULT_INTERVAL_SECONDS}s.", file=sys.stderr)
        SHARED_INTERVAL_SECONDS = DEFAULT_INTERVAL_SECONDS
except (ValueError, TypeError):
    print(f"[Scheduler] Warning: Non-integer PIPELINE_INTERVAL_SECONDS found. Using default {DEFAULT_INTERVAL_SECONDS}s.", file=sys.stderr)
    SHARED_INTERVAL_SECONDS = DEFAULT_INTERVAL_SECONDS

print(f"[Scheduler] Configured interval between sequence runs: {SHARED_INTERVAL_SECONDS} seconds.")

async def run_dbt() -> bool:
    print("\n[dbt] Running dbt models...")
    try:
        subprocess.run(
            ["dbt", "run", "--no-use-colors", "--profiles-dir", "."],
            cwd=os.path.join(os.getcwd(), "dbt"),  # <<< run from inside the dbt folder
            check=True
        )
        print("[dbt] Success")
        return True
    except subprocess.CalledProcessError as e:
        print("[dbt] Failed:", e, file=sys.stderr)
        return False



# --- End Configuration ---


async def run_pipeline(pipeline_name: str) -> bool:
    """
    Executes the run.sh script for a given pipeline sequentially and waits for completion.
    Returns True if successful (exit code 0), False otherwise.
    """    
    pipeline_path = os.path.abspath(os.path.join(PIPELINE_BASE_DIR, pipeline_name))
    run_script_path = os.path.join(pipeline_path, "run.sh")
    success = False # Default to failure

    # --- Pre-run Checks ---
    print(f"[{pipeline_name}] Checking prerequisites...")
    if not os.path.isdir(pipeline_path):
        print(f"[{pipeline_name}] Error: Pipeline directory not found: {pipeline_path}. Skipping.", file=sys.stderr)
        return success
    if not os.path.isfile(run_script_path):
        print(f"[{pipeline_name}] Error: run.sh not found in {pipeline_path}. Skipping.", file=sys.stderr)
        return success
    if not os.access(run_script_path, os.X_OK):
        # Attempt to make it executable
        try:
            os.chmod(run_script_path, os.stat(run_script_path).st_mode | 0o111) # Add execute permissions
            print(f"[{pipeline_name}] Info: Made run.sh executable.")
            if not os.access(run_script_path, os.X_OK): # Check again
                 raise OSError("Could not set execute permission.")
        except Exception as chmod_err:
            print(f"[{pipeline_name}] Error: run.sh ({run_script_path}) is not executable.", file=sys.stderr)
            print(f"[{pipeline_name}] Error: Attempted 'chmod +x' but failed: {chmod_err}", file=sys.stderr)
            print(f"[{pipeline_name}] Error: Please fix permissions manually. Skipping.", file=sys.stderr)
            return success
    print(f"[{pipeline_name}] Prerequisites OK.")
    # --- End Checks ---

    print(f"[{pipeline_name}] === Starting Pipeline Execution ===")
    start_time = time.monotonic()

    # Prepare environment: Pass a copy of the current environment
    # This ensures variables loaded from the root .env are available
    pipeline_env = os.environ.copy()

    try:
        # Debug: Print exact command and working directory
        print(f"[{pipeline_name}] DEBUG: Working Directory: {pipeline_path}")
        print(f"[{pipeline_name}] DEBUG: Executing Command: {run_script_path}")

        process = await asyncio.create_subprocess_exec(
            run_script_path,
            cwd=pipeline_path,
            env=pipeline_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        # Capture stdout and stderr real-time
        assert process.stdout is not None  # for type checkers
        while True:
            line = await process.stdout.readline()
            if not line:
                break
            print(f"[{pipeline_name}] {line.decode().rstrip()}")

        #Calculate duration
        await process.wait()
        end_time = time.monotonic()
        duration = end_time - start_time

        print(f"[{pipeline_name}] --- Raw Exit Code: {process.returncode} ---")

        # Optionally capture full stderr output
        if process.stderr:
            err_output = await process.stderr.read()
            stderr_str = err_output.decode().strip()
            if stderr_str:
                print(f"[{pipeline_name}] --- Captured STDERR ---", file=sys.stderr)
                print(stderr_str, file=sys.stderr)


        # Check return code for success/failure
        if process.returncode == 0:
            print(f"[{pipeline_name}] === Pipeline Completed Successfully in {duration:.2f} seconds ===")
            success = True
        else:
            print(f"[{pipeline_name}] === Pipeline Failed with Exit Code {process.returncode} after {duration:.2f} seconds ===", file=sys.stderr)
            # No need to print stderr again here, it was printed above

    except FileNotFoundError:
        # This can happen if run.sh itself is not found, although checked above
        end_time = time.monotonic()
        duration = start_time - end_time
        print(f"[{pipeline_name}] CRITICAL ERROR: run.sh not found during execution attempt after {duration:.2f}s.", file=sys.stderr)
        traceback.print_exc()
    except Exception as e:
        # Catch any other unexpected errors during subprocess execution
        end_time = time.monotonic()
        duration = start_time - end_time
        print(f"[{pipeline_name}] CRITICAL ERROR: An unexpected error occurred running '{pipeline_name}' after {duration:.2f} seconds:", file=sys.stderr)
        print(f"[{pipeline_name}] Error Type: {type(e).__name__}", file=sys.stderr)
        print(f"[{pipeline_name}] Error Details: {e}", file=sys.stderr)
        traceback.print_exc() # Print full traceback for unexpected errors

    return success

# --- End run_pipeline function ---


# --- New Sequential Runner Logic ---
async def run_sequence():
    """
    Discovers pipelines and runs them sequentially, repeating the sequence
    after the configured interval.
    """
    # --- Discover Pipelines ---
    if not os.path.isdir(PIPELINE_BASE_DIR):
        print(f"[Scheduler] Error: Base pipeline directory '{PIPELINE_BASE_DIR}' not found. Exiting.", file=sys.stderr)
        return
    pipeline_names = PIPELINE_NAMES

    print(f"[Scheduler] Found pipelines to run in sequence: {', '.join(pipeline_names)}")
    print("[Scheduler] --- Press Ctrl+C to stop. ---")
    # --- End Discovery ---

    run_count = 0
    while True:
        run_count += 1
        print(f"\n=================================================")
        print(f"[Scheduler] Starting Sequence Run #{run_count}")
        print(f"=================================================")
        start_sequence_time = time.monotonic()
        all_successful = True

        # --- Iterate and Run Sequentially ---
        for pipeline_name in pipeline_names:
            print(f"\n-------------------------------------------------")
            print(f"[Scheduler] Running Pipeline: '{pipeline_name}' (Sequence #{run_count})")
            print(f"-------------------------------------------------")

            # Await completion of the current pipeline
            pipeline_success = await run_pipeline(pipeline_name)

            if not pipeline_success:
                all_successful = False
                print(f"[Scheduler] Warning: Pipeline '{pipeline_name}' failed. Continuing sequence...", file=sys.stderr)
                # Decide if  want to stop the whole sequence on failure:
                # print(f"[Scheduler] Error: Pipeline '{pipeline_name}' failed. Stopping sequence run.", file=sys.stderr)
                # break # Uncomment this line to stop the sequence if one pipeline fails

            print(f"-------------------------------------------------")
            print(f"[Scheduler] Finished Pipeline: '{pipeline_name}'")
            print(f"-------------------------------------------------")

            # (Optional) short pause between pipelines?
            await asyncio.sleep(2)

        await asyncio.sleep(5)  # Let any lingering stdout flush
        print(f"[Scheduler] All pipelines in sequence completed.")

        # --- Run dbt after all pipelines ---
        dbt_success = await run_dbt()
        if not dbt_success:
            all_successful = False


        # --- Sequence Run Complete ---
        end_sequence_time = time.monotonic()
        sequence_duration = end_sequence_time - start_sequence_time
        print(f"\n=================================================")
        print(f"[Scheduler] Sequence Run #{run_count} Completed.")
        print(f"[Scheduler] Duration: {sequence_duration:.2f} seconds.")
        print(f"[Scheduler] Overall success: {all_successful}")
        print(f"=================================================")

        # --- Wait for Next Interval ---
        print(f"[Scheduler] Sleeping for {SHARED_INTERVAL_SECONDS} seconds until next sequence run...")
        await asyncio.sleep(SHARED_INTERVAL_SECONDS)

# --- End Sequential Runner Logic ---


# --- Main Execution Block ---
async def main():
    """Main asynchronous entry point."""
    await run_sequence() # Call the new sequential runner

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Scheduler] Shutdown requested by user. Exiting.")
    except Exception as e:
        print(f"\n[Scheduler] An critical error occurred in the main loop: {type(e).__name__} - {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
# --- End Main Execution Block ---