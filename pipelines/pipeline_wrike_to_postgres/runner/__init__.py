import os
import time
import yaml
import json
import argparse
import subprocess
from dotenv import load_dotenv


# Load environment variables from .env file found in the root or parent directories
load_dotenv(verbose=True)  # verbose=True helps confirm which .env is loaded

# --- Configuration specific to this pipeline ---
system = "wrike"  # <-- Make sure this matches the pipeline directory
# --- End Configuration ---

# --- Dynamic Paths & Config Loading ---
# Use absolute path for DISK_STORAGE_PATH from environment or default to a 'temp_files' subdir in the project root
project_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)  # Assumes runner/__init__.py structure
default_storage_path = os.path.join(
    project_root, "temp_files_storage"
)  # Default location
disk_storage_root = os.path.abspath(
    os.getenv("DISK_STORAGE_PATH", default_storage_path)
)

# Ensure the root storage directory exists
try:
    if not os.path.exists(disk_storage_root):
        print(f"Creating DISK_STORAGE_PATH directory: {disk_storage_root}")
        os.makedirs(disk_storage_root)
except Exception as e:
    print(f"[ERROR] Could not create storage directory at {disk_storage_root}")
    print(e)  # This ensures error gets shown in Render logs
    raise

# Define temp path specific to this pipeline instance
temp_files_path = os.path.join(disk_storage_root, f"{system}_temp")
print(f"Using temporary file path: {temp_files_path}")

# Ensure the pipeline-specific temp directory exists
if not os.path.exists(temp_files_path):
    print(f"Creating pipeline temp directory: {temp_files_path}")
    os.makedirs(temp_files_path)


# Read pipeline config from the current pipeline directory
# Assumes config.yml is in the same directory as the runner directory
pipeline_config_path = os.path.join(os.path.dirname(__file__), "..", "config.yml")
print(f"Loading pipeline config from: {pipeline_config_path}")

try:
    with open(pipeline_config_path) as file:
        pipelines_config = yaml.safe_load(file)
        if not pipelines_config:
            raise ValueError("Config file loaded as empty or invalid YAML.")
        streams_config = pipelines_config.get("extractors", [])
        if not streams_config:
            raise ValueError("No 'extractors' section found in config.yml")

        # This part seems less relevant now, but kept for context
        all_systems = [s.get("name", "").replace("tap-", "") for s in streams_config]
        selected_streams = {
            s["name"]: set(s["select"]) if "select" in s else None
            for s in streams_config
            if "name" in s
        }
except FileNotFoundError:
    print(f"Error: config.yml not found at {pipeline_config_path}")
    exit(1)
except yaml.YAMLError as e:
    print(f"Error parsing config.yml: {e}")
    exit(1)
except ValueError as e:
    print(f"Error processing config.yml: {e}")
    exit(1)
# --- End Dynamic Paths & Config Loading ---


# --- Argument Parsing ---
# Note: These arguments might not be easily passable via the current scheduler setup
parser = argparse.ArgumentParser(description="Data pipeline runner for " + system)
parser.add_argument(
    "--full_sync",
    dest="full_sync",
    action="store_true",
    default=False,
    help="if this flag is set then will run without state bookmarks (which means a full sync)",
)
parser.add_argument(
    "--once",
    dest="once",
    action="store_true",
    default=False,  # Default should be False if used by scheduler
    help="if this flag is set then will run once instead of waiting and running again",
)
parser.add_argument(
    "-t",
    "--tables",
    dest="tables",
    type=lambda t: t.split(","),
    help="select tables to sync; overrides selection in config.yml if provided. Note that this is case-sensitive.",
)
# Parse args, but be aware they might not be passed by the scheduler easily
# Consider reading these from ENV vars if needed via scheduler
args = parser.parse_args()
# --- End Argument Parsing ---


def main():
    print(f"[{system}] Starting runner...")
    runner()
    print(f"[{system}] Runner finished.")


def runner():
    unique_job_identifier = system
    state_file_path = os.path.join(
        temp_files_path, f"state_{unique_job_identifier}.json"
    )
    state_file_exists = os.path.exists(state_file_path)

    properties_file_name = f"properties_{system}.json"

    print(f"[{system}] Handling tap config...")
    handle_tap_config(system, unique_job_identifier)

    print(f"[{system}] Handling properties (discovery)...")
    # Call handle_properties WITHOUT activate_tap
    handle_properties(unique_job_identifier, properties_file_name)

    print(f"[{system}] Handling target config...")
    handle_target_config(unique_job_identifier)

    state_string = (
        ""
        if (args.full_sync or not state_file_exists)
        else f"--state {state_file_path}"  # Use absolute path for state
    )
    config_file_path = os.path.join(
        temp_files_path, f"config_{unique_job_identifier}.json"
    )
    properties_file_path = os.path.join(temp_files_path, properties_file_name)
    target_config_path = os.path.join(
        temp_files_path, f"postgres_{unique_job_identifier}.json"
    )

    # Construct the command WITHOUT activate_tap prefix
    # Use absolute paths for config files to be safe
    generated_command = (
        f"tap-{system} --config {config_file_path} --catalog {properties_file_path} {state_string} "
        f"| target-postgres --config {target_config_path}"
    )

    print(f"[{system}] Running command...")
    updated_state = run_command(generated_command)

    print(f"[{system}] Handling final state...")
    handle_state(unique_job_identifier, updated_state)


def run_command(command, as_json=True, show_output=True):
    process = subprocess.Popen(
        ["/bin/bash", "-ce", f"set -o pipefail; {command}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        universal_newlines=True,
        cwd=temp_files_path,
    )

    stdout_lines = []
    for line in process.stdout:
        line = line.strip()
        if not line:
            continue

        stdout_lines.append(line)

        if show_output and (
            "INFO" in line or "WARNING" in line or "ERROR" in line or "DEBUG" in line
        ):
            print(f"[{system}] {line}")

    process.wait()

    if process.returncode != 0:
        print("[run_command] --- FULL CAPTURED STDOUT (error path) ---")
        print("\n".join(stdout_lines))

    assert process.returncode == 0

    full_output = "\n".join(stdout_lines)

    if as_json:
        json_lines = [line for line in stdout_lines if line.startswith("{")]
        if json_lines:
            return json.loads(json_lines[-1])
        else:
            return {}
    else:
        return full_output


def handle_tap_config(pipeline, unique_job_identifier):
    # Debugging prints removed for brevity, re-add if needed
    expected_name = f"tap-{pipeline}"
    settings = None
    for s in pipelines_config.get("extractors", []):
        if s.get("name") == expected_name:
            settings = s
            break

    if not settings:
        print(
            f"[{system}] Error: No extractor configuration found for '{expected_name}' in config.yml",
            file=os.sys.stderr,
        )
        # Decide how to handle this - exit or try to continue? Exiting is safer.
        exit(1)  # Or raise an exception

    config = settings.get("config", {})

    # Apply environment variable overrides
    for setting_override in settings.get("settings", []):
        env_var_name = setting_override.get("env")
        setting_name = setting_override.get("name")
        if env_var_name and setting_name:
            env_value = os.getenv(env_var_name)
            if env_value is not None:  # Check if env var exists (even if empty)
                print(
                    f"[{system}] Applying env override: {setting_name} from {env_var_name}"
                )
                config[setting_name] = env_value
            # else: # Optional: Warn if env var is defined in config but not set
            #     print(f"[{system}] Warning: Env var '{env_var_name}' for setting '{setting_name}' not found in environment.")

    config_file_path = os.path.join(
        temp_files_path, f"config_{unique_job_identifier}.json"
    )
    print(f"[{system}] Writing tap config to: {config_file_path}")
    with open(config_file_path, "w") as f:
        json.dump(config, f, indent=2)  # Add indent for readability


def handle_properties(unique_job_identifier, properties_file_name):
    """Runs discovery and writes the catalog file properly with selected streams."""
    config_file_path = os.path.join(
        temp_files_path, f"config_{unique_job_identifier}.json"
    )
    properties_file_path = os.path.join(temp_files_path, properties_file_name)
    tap_exe_path = f"tap-{system}"

    discover_command = f"{tap_exe_path} --config {config_file_path} --discover"

    print(f"[{system}] Discovering schema with: {discover_command}")
    process = subprocess.run(
        ["/bin/bash", "-ce", f"set -o pipefail; {discover_command}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=temp_files_path,
    )

    stdout_str = process.stdout.decode().strip()
    stderr_str = process.stderr.decode().strip()

    if process.returncode != 0:
        print(f"[{system}] Discovery failed:")
        print(stderr_str)
        raise RuntimeError("Discovery failed")

    if not stdout_str:
        raise RuntimeError("Empty discovery output — tap didn't return a catalog")

    try:
        catalog = json.loads(stdout_str)
    except json.JSONDecodeError:
        print(f"[{system}] Catalog JSON parsing failed.")
        raise

    selected_streams = set()
    config = next(s for s in streams_config if s["name"] == f"tap-{system}")
    if "select" in config:
        selected_streams.update(config["select"])

    for stream in catalog.get("streams", []):
        stream_name = stream.get("stream")
        is_selected = stream_name in selected_streams

        for m in stream.get("metadata", []):
            breadcrumb = m.get("breadcrumb", [])
            if breadcrumb == []:  # Only select the root of the stream
                m.setdefault("metadata", {})["selected"] = is_selected

    # Write patched catalog
    with open(properties_file_path, "w") as f:
        json.dump(catalog, f, indent=2)
        print(
            f"[{system}] Wrote properties with selected streams to: {properties_file_path}"
        )


def handle_target_config(unique_job_identifier):
    # Assuming only one loader defined in config.yml
    if not pipelines_config.get("loaders"):
        print(
            f"[{system}] Error: No 'loaders' section found in config.yml",
            file=os.sys.stderr,
        )
        exit(1)

    target_settings = pipelines_config["loaders"][0]  # Take the first one
    config = target_settings.get("config", {})
    # Set schema specific to this pipeline run
    config["default_target_schema"] = unique_job_identifier
    print(f"[{system}] Setting target schema to: {unique_job_identifier}")

    # Apply environment variable overrides
    for setting_override in target_settings.get("settings", []):
        env_var_name = setting_override.get("env")
        setting_name = setting_override.get("name")
        if env_var_name and setting_name:
            env_value = os.getenv(env_var_name)
            if env_value is not None:
                print(
                    f"[{system}] Applying env override for target: {setting_name} from {env_var_name}"
                )
                config[setting_name] = env_value
            # else: # Optional Warning
            #     print(f"[{system}] Warning: Env var '{env_var_name}' for target setting '{setting_name}' not found.")

    target_config_file_path = os.path.join(
        temp_files_path, f"postgres_{unique_job_identifier}.json"
    )
    print(f"[{system}] Writing target config to: {target_config_file_path}")
    with open(target_config_file_path, "w") as f:
        json.dump(config, f, indent=2)  # Add indent


def handle_state(unique_job_identifier, updated_state):
    state_file_path = os.path.join(
        temp_files_path, f"state_{unique_job_identifier}.json"
    )

    if updated_state is None:
        print(f"[{system}] No updated state to save.")
        return

    # Check if state is already in correct format
    if (
        isinstance(updated_state, dict)
        and updated_state.get("type") == "STATE"
        and "value" in updated_state
    ):
        state_to_write = updated_state
    else:
        # Wrap properly if not in Singer format
        print(f"[{system}] Wrapping state inside proper Singer STATE envelope.")
        state_to_write = {"type": "STATE", "value": updated_state}

    print(f"[{system}] New state value: {state_to_write.get('value')}")
    with open(state_file_path, "w") as f:
        json.dump(state_to_write, f, indent=2)
        print(f"[{system}] Wrote state to: {state_file_path}")


if __name__ == "__main__":
    # This ensures the script runs when executed directly (python runner/__init__.py)
    main()
