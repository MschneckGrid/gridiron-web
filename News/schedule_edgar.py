"""
schedule_edgar.py
─────────────────
Registers edgar_fund_info_v4.py as a monthly Windows Task Scheduler task.

Run ONCE from the Reporting\\News folder (as the same user who runs other tasks):
    python schedule_edgar.py

What it creates:
  Task name : Gridiron - EDGAR N-2 Monthly Refresh
  Schedule  : 1st of every month at 6:00 AM
  Action    : python edgar_fund_info_v4.py  (with --skip-existing flag)
  Working dir: this folder (Reporting\\News)
  Run as    : current logged-in user (no password required for interactive logon)

The --skip-existing flag means: only process funds that DON'T already have
edgar_n2 as their objective_source. This makes the monthly run fast (~2 min
for new/updated filings only) rather than reprocessing all 321 every time.
Remove --skip-existing if you want a full refresh each month instead.

Deps: none (uses built-in subprocess + schtasks.exe)
"""

import subprocess
import sys
import os
import pathlib

TASK_NAME   = "Gridiron - EDGAR N-2 Monthly Refresh"
SCRIPT_NAME = "edgar_fund_info_v4.py"
RUN_HOUR    = "06:00"          # 6 AM
RUN_DAY     = "1"              # 1st of the month

def find_python():
    """Return the full path to the current Python executable."""
    return sys.executable

def get_script_dir():
    """Return the directory containing this setup script (= Reporting\\News)."""
    return str(pathlib.Path(__file__).parent.resolve())

def get_supabase_key():
    """Try to read SUPABASE_KEY from environment or gridiron.cfg."""
    key = os.environ.get("SUPABASE_KEY", "")
    if not key:
        cfg = pathlib.Path(__file__).parent / "gridiron.cfg"
        if cfg.exists():
            for line in cfg.read_text().splitlines():
                if line.strip().startswith("SUPABASE_KEY="):
                    key = line.strip().split("=", 1)[1].strip()
                    break
    return key

def build_schtasks_command(python_exe, script_dir):
    """
    Build the schtasks /Create command.
    Uses /SC MONTHLY /D 1 to run on the 1st of every month.
    SUPABASE_KEY is passed via the task's environment using a wrapper .cmd file
    so the key never appears in the task XML visible in Task Scheduler UI.
    """
    wrapper_path = pathlib.Path(script_dir) / "run_edgar_monthly.cmd"
    return wrapper_path

def write_wrapper_cmd(python_exe, script_dir, supabase_key):
    """
    Write a .cmd wrapper that sets SUPABASE_KEY and runs the script.
    This keeps the key out of the schtasks command line.
    """
    wrapper = pathlib.Path(script_dir) / "run_edgar_monthly.cmd"
    lines = [
        "@echo off",
        f'set "SUPABASE_KEY={supabase_key}"',
        f'cd /d "{script_dir}"',
        f'"{python_exe}" "{script_dir}\\{SCRIPT_NAME}" --skip-existing >> "{script_dir}\\edgar_monthly.log" 2>&1',
        "echo Edgar N-2 monthly run completed: %date% %time% >> "
        f'"{script_dir}\\edgar_monthly.log"',
    ]
    wrapper.write_text("\r\n".join(lines), encoding="utf-8")
    print(f"  Wrapper written → {wrapper}")
    return str(wrapper)

def register_task(wrapper_path, script_dir):
    """Register the scheduled task via schtasks.exe."""
    cmd = [
        "schtasks", "/Create",
        "/TN", TASK_NAME,
        "/TR", f'"{wrapper_path}"',
        "/SC", "MONTHLY",
        "/D", RUN_DAY,
        "/ST", RUN_HOUR,
        "/RL", "HIGHEST",       # run with highest available privileges
        "/F",                   # force overwrite if task already exists
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result

def main():
    python_exe  = find_python()
    script_dir  = get_script_dir()
    supabase_key = get_supabase_key()

    print(f"Python     : {python_exe}")
    print(f"Script dir : {script_dir}")
    print(f"Script     : {SCRIPT_NAME}")
    print(f"Schedule   : Monthly, day {RUN_DAY} at {RUN_HOUR}")
    print()

    # Verify the edgar script exists
    if not (pathlib.Path(script_dir) / SCRIPT_NAME).exists():
        print(f"ERROR: {SCRIPT_NAME} not found in {script_dir}")
        print("Run this script from the same folder as edgar_fund_info_v4.py.")
        sys.exit(1)

    # Warn if no Supabase key found
    if not supabase_key:
        print("WARNING: SUPABASE_KEY not found in environment or gridiron.cfg.")
        print("The task will fail at runtime without it.")
        print("Options:")
        print("  1. Set it in gridiron.cfg (recommended): SUPABASE_KEY=eyJ...")
        print("  2. Re-run this script after: set SUPABASE_KEY=eyJ...")
        print()
        supabase_key = input("Paste your service role key now (or press Enter to skip): ").strip()
        if not supabase_key:
            print("Continuing without key — edit run_edgar_monthly.cmd manually before first run.")

    # Write the .cmd wrapper
    wrapper_path = write_wrapper_cmd(python_exe, script_dir, supabase_key)

    # Register the scheduled task
    print(f"\nRegistering Task Scheduler task: '{TASK_NAME}'...")
    result = register_task(wrapper_path, script_dir)

    if result.returncode == 0:
        print(f"  SUCCESS: {result.stdout.strip()}")
        print()
        print("═══════════════════════════════════════════")
        print(f"  Task registered: '{TASK_NAME}'")
        print(f"  Runs: 1st of every month at {RUN_HOUR}")
        print(f"  Log:  {script_dir}\\edgar_monthly.log")
        print(f"  Cmd:  {script_dir}\\run_edgar_monthly.cmd")
        print()
        print("  To test immediately:")
        print(f'    schtasks /Run /TN "{TASK_NAME}"')
        print()
        print("  To view in Task Scheduler UI:")
        print("    taskschd.msc → Task Scheduler Library")
        print("═══════════════════════════════════════════")
    else:
        print(f"  FAILED (code {result.returncode})")
        print(f"  stdout: {result.stdout}")
        print(f"  stderr: {result.stderr}")
        print()
        print("If you see 'Access is denied', run Command Prompt as Administrator.")
        sys.exit(1)

if __name__ == "__main__":
    main()
