#!/usr/bin/env python3
"""Execute weeks 1-9 notebooks and generate 1 HTML per week for all 16 weeks."""

import glob
import os
import subprocess
import sys
import nbformat
from nbformat.v4 import new_notebook

EXECUTE_WEEKS = {f"week{i:02d}" for i in range(2, 6)}
HTML_DIR = "html"
TIMEOUT = 300  # seconds per notebook

os.makedirs(HTML_DIR, exist_ok=True)


def get_weeks():
    weeks = sorted(
        set(p.split("/")[0] for p in glob.glob("week*/*.ipynb")),
        key=lambda w: int(w.replace("week", "")),
    )
    return weeks


def execute_notebook(nb_path):
    print(f"  Executing {nb_path} ...", flush=True)
    result = subprocess.run(
        [
            sys.executable, "-m", "jupyter", "nbconvert",
            "--to", "notebook",
            "--execute",
            f"--ExecutePreprocessor.timeout={TIMEOUT}",
            "--allow-errors",
            "--inplace",
            nb_path,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"    WARNING: {nb_path} had errors (continuing)", flush=True)
        print(result.stderr[-500:] if result.stderr else "", flush=True)


def merge_notebooks(nb_paths):
    combined = new_notebook()
    combined.metadata = {}
    for i, path in enumerate(nb_paths):
        nb = nbformat.read(path, as_version=4)
        if i == 0:
            combined.metadata = nb.metadata
        # Add a markdown separator between notebooks
        if i > 0:
            sep = nbformat.v4.new_markdown_cell(
                f"---\n## {os.path.basename(path).replace('.ipynb', '').replace('_', ' ').title()}"
            )
            combined.cells.append(sep)
        combined.cells.extend(nb.cells)
    return combined


def week_to_html(week, nb_paths):
    print(f"  Building HTML for {week} ({len(nb_paths)} notebooks)...", flush=True)
    merged = merge_notebooks(nb_paths)
    merged_path = os.path.join(HTML_DIR, f"{week}_merged.ipynb")
    nbformat.write(merged, merged_path)

    num = week.replace("week", "")
    padded = f"week{int(num):02d}"
    html_path = os.path.join(HTML_DIR, f"{padded}.html")
    result = subprocess.run(
        [
            sys.executable, "-m", "jupyter", "nbconvert",
            "--to", "html",
            "--output", os.path.abspath(html_path),
            merged_path,
        ],
        capture_output=True,
        text=True,
    )
    os.remove(merged_path)
    if result.returncode != 0:
        print(f"    ERROR converting {week} to HTML", flush=True)
        print(result.stderr[-500:] if result.stderr else "", flush=True)
    else:
        size_kb = os.path.getsize(html_path) // 1024
        print(f"    -> {html_path} ({size_kb} KB)", flush=True)


def main():
    weeks = get_weeks()
    print(f"Found {len(weeks)} weeks", flush=True)

    HTML_WEEKS = {f"week{i:02d}" for i in range(2, 6)}

    # Execute weeks 2-5 (skip already-executed notebooks)
    for week in weeks:
        if week not in EXECUTE_WEEKS:
            continue
        nb_paths = sorted(glob.glob(f"{week}/*.ipynb"))
        print(f"\n[{week}] Executing {len(nb_paths)} notebooks...", flush=True)
        for nb_path in nb_paths:
            nb = nbformat.read(nb_path, as_version=4)
            has_output = any(c.get("outputs") for c in nb.cells if c.cell_type == "code")
            if has_output:
                print(f"  Skipping {nb_path} (already executed)", flush=True)
                continue
            execute_notebook(nb_path)


    # Generate HTML for weeks 2-5
    print("\nGenerating HTML files...", flush=True)
    for week in weeks:
        if week not in HTML_WEEKS:
            continue
        nb_paths = sorted(glob.glob(f"{week}/*.ipynb"))
        if not nb_paths:
            continue
        week_to_html(week, nb_paths)

    print("\nDone!", flush=True)


if __name__ == "__main__":
    main()
