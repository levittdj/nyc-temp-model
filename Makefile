.PHONY: validate validate-canary

DB ?= nyc_temp_log.sqlite
PY ?= python3

validate:
	$(PY) scripts/validate_dsm_cli.py

validate-canary: validate
	$(PY) scripts/post_canary_sanity.py --db $(DB)
