# Runs the URL-scheme parity harness: the Go program reproduces the webhook's
# checkURLScheme decision, the Java program reproduces Spark's fetch decision,
# and `make check` asserts the safety invariant (no input is Go-ALLOW while
# Spark would FETCH it remotely). See README.md.
#
# Standalone: this Makefile and its go.mod are intentionally separate from the
# repo-root Makefile/module so the harness never enters the operator build.

GO ?= go
JAVA ?= java

OUT_DIR := $(CURDIR)/.out
GO_OUT := $(OUT_DIR)/go.txt
JAVA_OUT := $(OUT_DIR)/java.txt

.PHONY: help
help: ## Show available targets.
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

$(OUT_DIR):
	@mkdir -p $(OUT_DIR)

.PHONY: go-out
go-out: $(OUT_DIR) ## Run the Go (webhook) side.
	@$(GO) run parity.go > $(GO_OUT)

.PHONY: java-out
java-out: $(OUT_DIR) ## Run the Java (Spark) side.
	@$(JAVA) Parity.java > $(JAVA_OUT)

.PHONY: run
run: go-out java-out ## Run both sides and print them aligned row-for-row.
	@paste -d'|' $(GO_OUT) $(JAVA_OUT)

.PHONY: check
check: go-out java-out ## Assert the invariant: no Go ALLOW where Spark would FETCH.
	@# Join the two verdict columns row-by-row (skip the header) and flag any row
	@# where Go said ALLOW but Java said FETCH-*. Exit non-zero if any are found.
	@violations=$$(paste -d'|' $(GO_OUT) $(JAVA_OUT) | tail -n +2 | \
		awk -F'|' '{ \
			go_v=$$1; java_v=$$2; \
			sub(/[ \t].*/, "", go_v); sub(/[ \t].*/, "", java_v); \
			if (go_v == "ALLOW" && java_v ~ /^FETCH/) print $$0 \
		}'); \
	if [ -n "$$violations" ]; then \
		echo "FAIL: found value(s) the webhook ALLOWs but Spark would FETCH remotely:"; \
		echo "$$violations"; \
		exit 1; \
	fi; \
	echo "OK: no Go-ALLOW / Spark-FETCH divergence across $$(tail -n +2 $(GO_OUT) | wc -l | tr -d ' ') inputs"

.PHONY: clean
clean: ## Remove generated output.
	@rm -rf $(OUT_DIR)
