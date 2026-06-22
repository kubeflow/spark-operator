#!/usr/bin/env python3

# Copyright The Kubeflow authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Verify boilerplate copyright headers in source files.

This script verifies that all source files have valid copyright headers.
It follows the Kubernetes approach:
- Files with years 2016-2025 are allowed (year is stripped during comparison)
- Files with years 2026+ are rejected (enforces no-year policy for new files)
- Files without years are preferred (matches template directly)

Reference: https://github.com/kubernetes/steering/issues/299
"""

import argparse
import glob
import os
import re
import sys
from typing import Dict, List, Optional, Tuple


# After 2025, new files should not include a year in the copyright header.
# This aligns with CNCF/LF Legal guidance.
FINAL_YEAR = 2025

# First year of the project (for year range validation)
FIRST_YEAR = 2016

# Files that were merged with 2026 copyright before this policy was implemented.
# These are explicitly allowed to have "Copyright 2026" headers.
# Any new file must use the year-less format.
YEAR_2026_ALLOWED_FILES = {
    "internal/controller/scheduledsparkapplication/event_filter_test.go",
    "internal/webhook/sparkconnect_defaulter.go",
    "internal/webhook/sparkconnect_validator.go",
    "internal/webhook/sparkconnect_validator_test.go",
    "pkg/util/namespace.go",
    "pkg/util/namespace_test.go",
    "pkg/util/predicates_test.go",
    "test/e2e/namespace_filtering_test.go",
}

# Files with historical "Copyright Google LLC" headers.
# Per CNCF/LF Legal guidance, existing attribution notices should not be modified.
GOOGLE_LLC_ALLOWED_FILES = {
    "Dockerfile",
    "api/v1beta2/defaults.go",
    "api/v1beta2/defaults_test.go",
    "api/v1beta2/doc.go",
    "internal/controller/sparkapplication/monitoring_config.go",
    "internal/controller/sparkapplication/monitoring_config_test.go",
    "internal/controller/sparkapplication/submission.go",
    "internal/controller/sparkapplication/submission_test.go",
    "internal/controller/sparkapplication/web_ui.go",
    "internal/controller/sparkapplication/web_ui_test.go",
    "internal/scheduler/registry.go",
    "internal/scheduler/scheduler.go",
    "internal/scheduler/volcano/scheduler.go",
    "internal/scheduler/volcano/scheduler_test.go",
    "internal/webhook/doc.go",
    "internal/webhook/sparkpod_defaulter_test.go",
    "pkg/common/doc.go",
    "pkg/common/spark.go",
    "pkg/util/capabilities.go",
    "pkg/util/doc.go",
    "pkg/util/metrics.go",
    "pkg/util/util.go",
    "spark-docker/Dockerfile",
}

# Files with historical "Copyright The Kubernetes Authors" headers.
KUBERNETES_AUTHORS_ALLOWED_FILES = {
    "hack/install_packages.sh",
    "hack/update-codegen.sh",
    "hack/verify-codegen.sh",
    "pkg/util/interrupt.go",
}

# Files with minor header variations (capitalization, indentation) that are pre-existing.
HEADER_VARIATION_ALLOWED_FILES = {
    # lowercase "kubeflow" instead of "Kubeflow"
    "internal/controller/sparkapplication/event_filter.go",
    "internal/controller/sparkapplication/event_handler.go",
    "internal/controller/sparkconnect/util.go",
    "internal/controller/sparkconnect/util_test.go",
    # Tab indentation instead of spaces
    "internal/controller/scheduledsparkapplication/controller_test.go",
    "pkg/certificate/certificate.go",
    "pkg/scheme/scheme.go",
    # "Authors" (capital A) instead of "authors", plus 2026 year
    "hack/openapi/gen-openapi.sh",
    "hack/python-api/gen-api.sh",
    "hack/swagger/main.go",
    # "Authors" (capital A) instead of "authors"
    "api/python_api/kubeflow_spark_api/__init__.py",
    # Extra empty comment line at start
    "docker/Dockerfile.kubectl",
}

# Files with historical "Copyright spark-operator contributors" headers.
SPARK_OPERATOR_CONTRIBUTORS_ALLOWED_FILES = {
    "internal/controller/sparkapplication/driveringress.go",
    "internal/controller/sparkapplication/driveringress_test.go",
}

# TODO: Add copyright headers to these files.
# These files historically don't have copyright headers and should be fixed
# in a separate PR.
NO_HEADER_ALLOWED_FILES = {
    "entrypoint.sh",
    "hack/api-docs/template/placeholder.go",
    "hack/generate-changelog.py",
    "pkg/util/predicates.go",
    "pkg/util/workqueue.go",
    "test/e2e/sparkconnect_test.go",
}


def get_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Verify boilerplate copyright headers"
    )
    parser.add_argument(
        "filenames",
        nargs="*",
        help="Specific files to check (default: scan entire repo)",
    )
    parser.add_argument(
        "--boilerplate-dir",
        default=os.path.join(os.path.dirname(__file__)),
        help="Directory containing boilerplate template files",
    )
    parser.add_argument(
        "--rootdir",
        default=None,
        help="Root directory to scan (default: auto-detect git root)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show verbose output including diffs",
    )
    return parser.parse_args()


def find_root_dir() -> str:
    """Find the repository root directory."""
    # Try to find git root
    current = os.getcwd()
    while current != "/":
        if os.path.isdir(os.path.join(current, ".git")):
            return current
        current = os.path.dirname(current)

    # Fall back to current directory
    return os.getcwd()


def get_refs(boilerplate_dir: str) -> Dict[str, List[str]]:
    """Load boilerplate templates from the boilerplate directory.

    Returns a dictionary mapping file extensions to template lines.
    """
    refs = {}

    for path in glob.glob(os.path.join(boilerplate_dir, "boilerplate.*.txt")):
        basename = os.path.basename(path)
        # Extract extension: boilerplate.go.txt -> go
        ext = basename.replace("boilerplate.", "").replace(".txt", "")

        with open(path, "r") as f:
            refs[ext] = f.read().splitlines()

    return refs


def get_regexs() -> Dict[str, re.Pattern]:
    """Compile regex patterns used for validation."""
    regexs = {}

    # Pattern to match years in the allowed range (will be stripped)
    # Space before and after ensures we match " 2024 " not "12024"
    years = "|".join(str(year) for year in range(FIRST_YEAR, FINAL_YEAR + 1))
    regexs["date"] = re.compile(f"Copyright ({years}) ")

    # Go build constraints (stripped before comparison)
    regexs["go_build_constraints"] = re.compile(
        r"^(//(go:build| \+build).*\n)+\n", re.MULTILINE
    )

    # Shebang lines (stripped before comparison)
    regexs["shebang"] = re.compile(r"^(#!.*\n)\n*")

    # Pattern to detect any year in copyright (including years > FINAL_YEAR)
    regexs["any_year"] = re.compile(r"Copyright (\d{4}) ")

    return regexs


def file_extension(filename: str) -> str:
    """Extract file extension for template lookup."""
    basename = os.path.basename(filename)

    # Handle special filenames
    if basename == "Dockerfile" or basename.startswith("Dockerfile."):
        return "Dockerfile"

    # Standard extension extraction
    ext = os.path.splitext(filename)[1].lstrip(".")
    return ext.lower() if ext else ""


def get_files(
    rootdir: str,
    extensions: List[str],
    filenames: Optional[List[str]] = None,
) -> List[str]:
    """Get list of files to check."""
    # Paths to skip
    skip_patterns = [
        "hack/boilerplate/test/",  # Test fixtures with intentional failures
        "api/python_api/kubeflow_spark_api/models/",  # Auto-generated with different template
    ]

    # If specific files provided, use those
    if filenames:
        files = []
        for f in filenames:
            if os.path.isfile(f):
                files.append(f)
            elif os.path.isdir(f):
                for root, dirs, filelist in os.walk(f):
                    # Skip directories
                    dirs[:] = [d for d in dirs if not d.startswith("__")]
                    for name in filelist:
                        files.append(os.path.join(root, name))
        return files

    # Walk the directory tree
    files = []
    for root, dirs, filelist in os.walk(rootdir):
        # Skip directories in-place
        dirs[:] = [d for d in dirs if not d.startswith("__")]

        for name in filelist:
            filepath = os.path.join(root, name)
            relpath = os.path.relpath(filepath, rootdir)

            # Check skip patterns
            if any(pattern in relpath for pattern in skip_patterns):
                continue

            # Check extension
            ext = file_extension(filepath)
            if ext in extensions:
                files.append(filepath)

    return sorted(files)


def file_passes(
    filename: str,
    refs: Dict[str, List[str]],
    regexs: Dict[str, re.Pattern],
    verbose: bool = False,
) -> Tuple[bool, Optional[str]]:
    """Check if a file has a valid boilerplate header.

    Returns:
        Tuple of (passes, error_message)
    """
    # Check if file is in any of the historical allowlists
    relpath = os.path.relpath(filename).replace(os.sep, "/")
    if relpath in GOOGLE_LLC_ALLOWED_FILES:
        return True, None
    if relpath in KUBERNETES_AUTHORS_ALLOWED_FILES:
        return True, None
    if relpath in HEADER_VARIATION_ALLOWED_FILES:
        return True, None
    if relpath in NO_HEADER_ALLOWED_FILES:
        return True, None
    if relpath in SPARK_OPERATOR_CONTRIBUTORS_ALLOWED_FILES:
        return True, None

    ext = file_extension(filename)

    # Check if we have a template for this file type
    if ext not in refs:
        # No template for this file type, skip
        return True, None

    try:
        with open(filename, "r", encoding="utf-8") as f:
            data = f.read()
    except (IOError, UnicodeDecodeError) as e:
        return False, f"Error reading file: {e}"

    ref = refs[ext]

    # Get file lines
    lines = data.splitlines()

    # Strip Go build constraints
    if ext == "go":
        data_stripped = regexs["go_build_constraints"].sub("", data)
        lines = data_stripped.splitlines()

    # Strip shebang for shell/python
    if ext in ("sh", "py", "bash"):
        data_stripped = regexs["shebang"].sub("", data)
        lines = data_stripped.splitlines()

    # Check if file is long enough
    if len(lines) < len(ref):
        return False, "File too short for boilerplate header"

    # Get the header portion
    header = lines[:len(ref)]

    normalized_header = []
    for line in header:
        # Check for years outside the allowed range first
        match = regexs["any_year"].search(line)
        if match:
            year = int(match.group(1))
            if year > FINAL_YEAR:
                # Check if this file is in the 2026 allowlist
                # (files merged before the no-year policy was implemented)
                relpath = os.path.relpath(filename)
                # Normalize path separators for comparison
                relpath = relpath.replace(os.sep, "/")
                if year == 2026 and relpath in YEAR_2026_ALLOWED_FILES:
                    # This file is grandfathered, treat 2026 as allowed
                    pass
                else:
                    return False, f"Year {year} in copyright is not allowed (must be omitted)"

        # Strip allowed years for comparison (including 2026 for allowlisted files)
        normalized_line = regexs["date"].sub("Copyright ", line)
        # Also strip 2026 for allowlisted files
        normalized_line = re.sub(r"Copyright 2026 ", "Copyright ", normalized_line)
        normalized_header.append(normalized_line)
    header = normalized_header

    # Normalize http:// to https:// (both are acceptable for license URL)
    header = [line.replace("http://www.apache.org", "https://www.apache.org") for line in header]

    # Compare header with reference
    if header != ref:
        if verbose:
            diff_msg = []
            for i, (got, want) in enumerate(zip(header, ref)):
                if got != want:
                    diff_msg.append(f"  Line {i+1}:")
                    diff_msg.append(f"    got:  {got!r}")
                    diff_msg.append(f"    want: {want!r}")
            return False, "Header mismatch:\n" + "\n".join(diff_msg)
        return False, "Header does not match boilerplate template"

    return True, None


def main() -> int:
    """Main entry point."""
    args = get_args()

    # Find root directory
    if args.rootdir:
        rootdir = args.rootdir
    else:
        rootdir = find_root_dir()

    # Change to root directory for relative path resolution
    os.chdir(rootdir)

    # Load templates
    refs = get_refs(args.boilerplate_dir)
    if not refs:
        print(f"ERROR: No boilerplate templates found in {args.boilerplate_dir}", file=sys.stderr)
        return 1

    # Compile regexes
    regexs = get_regexs()

    # Get list of files
    extensions = list(refs.keys())

    # Check all files against templates
    files = get_files(rootdir, extensions, args.filenames or None)

    # Check each file
    failed_files = []
    for filepath in files:
        passes, error = file_passes(filepath, refs, regexs, args.verbose)
        if not passes:
            relpath = os.path.relpath(filepath, rootdir)
            failed_files.append(relpath)
            if args.verbose and error:
                print(f"{relpath}: {error}", file=sys.stderr)

    # Output failed files (one per line, for parsing by wrapper script)
    for f in failed_files:
        print(f)

    return 0 if not failed_files else 1


if __name__ == "__main__":
    sys.exit(main())
