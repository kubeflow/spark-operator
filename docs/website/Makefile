# Makefile for Sphinx documentation
# Kubeflow Spark Operator Documentation System
#
# Python tooling is managed with uv. `uv run` automatically syncs the `docs`
# dependency group from pyproject.toml / uv.lock before running.

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = uv run --group docs sphinx-build
SOURCEDIR     = .
BUILDDIR      = _build

# Default target
.PHONY: help
help:
	@echo "Kubeflow Spark Operator Documentation Build Targets:"
	@echo "  html         Build HTML documentation"
	@echo "  test         Strict build (warnings treated as errors)"
	@echo "  linkcheck    Check all links in documentation"
	@echo "  clean        Remove build artifacts"
	@echo "  serve        Build and serve documentation locally with live reload"

.PHONY: html
html:
	$(SPHINXBUILD) -b html $(SPHINXOPTS) $(SOURCEDIR) $(BUILDDIR)/html
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

.PHONY: test
test:
	@echo "Strict build — warnings are treated as errors."
	$(SPHINXBUILD) -b html -W --keep-going $(SPHINXOPTS) $(SOURCEDIR) $(BUILDDIR)/html
	@echo
	@echo "Docs build passed with no warnings."

.PHONY: linkcheck
linkcheck:
	$(SPHINXBUILD) -b linkcheck $(SPHINXOPTS) $(SOURCEDIR) $(BUILDDIR)/linkcheck
	@echo
	@echo "Link check complete. See $(BUILDDIR)/linkcheck/output.txt for results."

.PHONY: clean
clean:
	rm -rf $(BUILDDIR)
	@echo "Build artifacts removed."

.PHONY: serve
serve:
	@echo "Starting live-reload documentation server on http://localhost:8000"
	@echo "Documentation will auto-rebuild on file changes."
	@echo "Press Ctrl+C to stop."
	uv run --group docs sphinx-autobuild $(SPHINXOPTS) $(SOURCEDIR) $(BUILDDIR)/html --port 8000
