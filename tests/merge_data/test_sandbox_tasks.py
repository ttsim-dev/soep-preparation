"""Test that sandbox tasks produce identical results."""

import os
import subprocess
from pathlib import Path

import pandas as pd
import pytest

from soep_preparation.config import BLD, MODULES


def test_sandbox_tasks_produce_identical_results():
    """Test that the two sandbox tasks produce identical DataFrames.

    This test checks if pytask has run already (MODULES is filled).
    If not, it skips the test. If so, it runs the tasks in sandbox
    and checks whether the dataframes in the two created files are identical.
    """
    # Check if MODULES is filled (pytask has run already)
    if not MODULES._entries:  # noqa: SLF001
        pytest.skip("MODULES is empty. Run pytask first to populate modules.")

    # Paths to the sandbox task file and expected output files
    root = Path(__file__).parent.parent.parent
    sandbox_dir = root / "sandbox"
    out_file_1 = BLD / "example_final_dataset" / "from_catch_all_task.pickle"
    out_file_2 = BLD / "example_final_dataset" / "from_explicit_task.pickle"

    # Run pytask programmatically on the sandbox tasks
    # Create a temporary pyproject.toml in sandbox directory so pytask uses it
    # instead of the root one, allowing us to collect only sandbox tasks
    original_cwd = Path.cwd()
    sandbox_config_path = sandbox_dir / "pyproject.toml"
    try:
        # Create a minimal pyproject.toml in sandbox directory
        # This makes pytask use sandbox as root and only collect tasks from there
        sandbox_config_content = """[tool.pytask.ini_options]
paths = ["."]
task_files = ["task_*.py", "task.py", "tasks.py"]
"""
        sandbox_config_path.write_text(sandbox_config_content)

        # Change to sandbox directory and run pytask
        # Pytask will find the pyproject.toml in sandbox and use it
        os.chdir(sandbox_dir)
        # Force execution even if outputs exist (to ensure files are created)
        result = subprocess.run(
            ["pixi", "run", "pytask", "build", "--force"],  # noqa: S607
            capture_output=True,
            text=True,
            check=False,
        )
        # Print output for debugging (suppressed in test output unless test fails)
        if result.returncode != 0:
            print(f"Pytask stderr: {result.stderr}")  # noqa: T201
        print(f"Pytask stdout: {result.stdout}")  # noqa: T201
        # Check if sandbox tasks were collected
        if "Collected 0 tasks" in result.stdout:
            pytest.fail(
                f"Pytask did not collect sandbox tasks. Output: {result.stdout}"
            )
        result.check_returncode()
    finally:
        # Clean up: remove temporary config and restore original directory
        if sandbox_config_path.exists():
            sandbox_config_path.unlink()
        os.chdir(original_cwd)

    # Check if both output files exist
    assert out_file_1.exists(), f"Expected output file {out_file_1} does not exist"
    assert out_file_2.exists(), f"Expected output file {out_file_2} does not exist"

    # Load the DataFrames from the pickle files
    df1 = pd.read_pickle(out_file_1)  # noqa: S301
    df2 = pd.read_pickle(out_file_2)  # noqa: S301

    # Check if the DataFrames are identical
    # Use check_like=True to ignore column order, and check_dtype=False for flexibility
    pd.testing.assert_frame_equal(df1, df2, check_names=True, check_like=True)
