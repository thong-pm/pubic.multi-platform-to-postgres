#!/bin/bash

# --- Configuration ---
set -e
set -o pipefail

# --- Helper Function for Install ---
run_poetry_install() {
    echo "> Installing dependencies with Poetry..."
    INSTALL_OUTPUT=$(poetry install 2>&1)
    INSTALL_EXIT_CODE=$?

    if [ $INSTALL_EXIT_CODE -ne 0 ]; then
        if echo "$INSTALL_OUTPUT" | grep -q "Run \`poetry lock\` to fix the lock file"; then
            echo "> Lock file mismatch detected, running 'poetry lock'..."
            set -e
            poetry lock
            set +e
            echo "> Retrying 'poetry install'..."
            INSTALL_OUTPUT=$(poetry install 2>&1)
            INSTALL_EXIT_CODE=$?
            if [ $INSTALL_EXIT_CODE -ne 0 ]; then
                echo "X 'poetry install' failed after retry:" >&2
                echo "$INSTALL_OUTPUT" >&2
                exit 1
            fi
        else
            echo "X 'poetry install' failed:" >&2
            echo "$INSTALL_OUTPUT" >&2
            exit 1
        fi
    fi
    set -e
}


# --- Script Start ---
echo "=== Starting Pipeline Setup ==="

# 1. Check for Poetry
echo "> Checking Poetry..."
if ! command -v poetry &> /dev/null; then
    echo "X Poetry not found. Install from https://python-poetry.org/docs/#installation" >&2
    exit 1
fi
echo "> Poetry version: $(poetry --version)"

# 2. Install deps
set +e
run_poetry_install
INSTALL_SUCCESS=$?
set -e
if [ $INSTALL_SUCCESS -ne 0 ]; then
    exit 1
fi

# 3. Activate venv
echo "> Activating virtual environment..."
ACTIVATE_COMMAND=$(poetry env activate 2>/dev/null)
if [ $? -ne 0 ] || [ -z "$ACTIVATE_COMMAND" ]; then
    echo "X Failed to activate Poetry environment." >&2
    exit 1
fi
eval "$ACTIVATE_COMMAND"
echo "> Environment activated."

# 4. Run scheduler
echo "> Running scheduler..."
python scheduler.py

# --- End ---
echo "=== Scheduler completed ==="
exit 0
