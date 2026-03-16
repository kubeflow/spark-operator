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

"""Unit tests for boilerplate.py."""

import os
import sys
import unittest
from io import StringIO
from unittest.mock import patch

import boilerplate


class TestBoilerplate(unittest.TestCase):
    """Test cases for boilerplate verification."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = os.path.join(os.path.dirname(__file__), "test")
        self.boilerplate_dir = os.path.dirname(__file__)

    def test_get_refs(self):
        """Test that templates are loaded correctly."""
        refs = boilerplate.get_refs(self.boilerplate_dir)
        self.assertIn("go", refs)
        self.assertIn("sh", refs)
        self.assertIn("py", refs)
        self.assertIn("Dockerfile", refs)

    def test_file_extension(self):
        """Test file extension extraction."""
        self.assertEqual(boilerplate.file_extension("test.go"), "go")
        self.assertEqual(boilerplate.file_extension("test.py"), "py")
        self.assertEqual(boilerplate.file_extension("test.sh"), "sh")
        self.assertEqual(boilerplate.file_extension("Dockerfile"), "Dockerfile")
        self.assertEqual(boilerplate.file_extension("Dockerfile.dev"), "Dockerfile")

    def test_pass_no_year(self):
        """Test file without year passes."""
        refs = boilerplate.get_refs(self.boilerplate_dir)
        regexs = boilerplate.get_regexs()
        filepath = os.path.join(self.test_dir, "pass_no_year.go")
        passes, error = boilerplate.file_passes(filepath, refs, regexs)
        self.assertTrue(passes, f"Expected pass_no_year.go to pass: {error}")

    def test_pass_with_year(self):
        """Test file with year in allowed range passes."""
        refs = boilerplate.get_refs(self.boilerplate_dir)
        regexs = boilerplate.get_regexs()
        filepath = os.path.join(self.test_dir, "pass_with_year.go")
        passes, error = boilerplate.file_passes(filepath, refs, regexs)
        self.assertTrue(passes, f"Expected pass_with_year.go to pass: {error}")

    def test_pass_with_build_tag(self):
        """Test file with Go build tags passes."""
        refs = boilerplate.get_refs(self.boilerplate_dir)
        regexs = boilerplate.get_regexs()
        filepath = os.path.join(self.test_dir, "pass_with_build_tag.go")
        passes, error = boilerplate.file_passes(filepath, refs, regexs)
        self.assertTrue(passes, f"Expected pass_with_build_tag.go to pass: {error}")

    def test_pass_shell(self):
        """Test shell script with shebang passes."""
        refs = boilerplate.get_refs(self.boilerplate_dir)
        regexs = boilerplate.get_regexs()
        filepath = os.path.join(self.test_dir, "pass.sh")
        passes, error = boilerplate.file_passes(filepath, refs, regexs)
        self.assertTrue(passes, f"Expected pass.sh to pass: {error}")

    def test_pass_python(self):
        """Test Python script with shebang passes."""
        refs = boilerplate.get_refs(self.boilerplate_dir)
        regexs = boilerplate.get_regexs()
        filepath = os.path.join(self.test_dir, "pass.py")
        passes, error = boilerplate.file_passes(filepath, refs, regexs)
        self.assertTrue(passes, f"Expected pass.py to pass: {error}")

    def test_fail_2026(self):
        """Test file with year 2026 fails (not in allowlist)."""
        refs = boilerplate.get_refs(self.boilerplate_dir)
        regexs = boilerplate.get_regexs()
        filepath = os.path.join(self.test_dir, "fail_2026.go")
        passes, error = boilerplate.file_passes(filepath, refs, regexs)
        self.assertFalse(passes, "Expected fail_2026.go to fail")
        self.assertIn("2026", error)

    def test_fail_wrong_header(self):
        """Test file with wrong header fails."""
        refs = boilerplate.get_refs(self.boilerplate_dir)
        regexs = boilerplate.get_regexs()
        filepath = os.path.join(self.test_dir, "fail_wrong_header.go")
        passes, _ = boilerplate.file_passes(filepath, refs, regexs)
        self.assertFalse(passes, "Expected fail_wrong_header.go to fail")

    def test_main_with_test_files(self):
        """Test main function with test directory."""
        # Capture stdout
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            with patch("sys.argv", ["boilerplate.py", self.test_dir]):
                ret = boilerplate.main()

        output = mock_stdout.getvalue()
        failed_files = [f for f in output.strip().split("\n") if f]

        # Should have exactly 2 failures: fail_2026.go and fail_wrong_header.go
        self.assertEqual(ret, 1, "Expected non-zero return code")
        self.assertEqual(len(failed_files), 2, f"Expected 2 failures, got: {failed_files}")

        # Check that the right files failed
        failed_basenames = [os.path.basename(f) for f in failed_files]
        self.assertIn("fail_2026.go", failed_basenames)
        self.assertIn("fail_wrong_header.go", failed_basenames)


if __name__ == "__main__":
    unittest.main()
