# Makefile for Sphinx documentation
# Kubeflow Spark Operator Documentation System

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = python3 -m sphinx
SOURCEDIR     = .
BUILDDIR      = _build

# Default target
.PHONY: help
help:
	@echo "Kubeflow Spark Operator Documentation Build Targets:"
	@echo "  html         Build HTML documentation"
	@echo "  linkcheck    Check all links in documentation"
	@echo "  clean        Remove build artifacts"
	@echo "  serve        Build and serve documentation locally with live reload"

.PHONY: html
html:
	$(SPHINXBUILD) -b html $(SPHINXOPTS) $(SOURCEDIR) $(BUILDDIR)/html
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

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
	python3 -m sphinx_autobuild $(SPHINXOPTS) $(SOURCEDIR) $(BUILDDIR)/html --port 8000
