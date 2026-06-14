PYTHON ?= python3
PORT ?= 4000
SERVER_ID ?= 1
CLIENTS ?= 4
REQUESTS ?= 1000
SEED ?= 20260613
ENTRIES ?= 25000
TARGET_DURATION ?= 240
OUTPUT_ROOT ?= benchmark-results
BASE_PORT ?= 47000

.PHONY: run-server run-client test benchmark benchmark-generate benchmark-chaos benchmark-chaos-quick

run-server:
	$(PYTHON) server/main.py $(PORT) --server-id $(SERVER_ID)

run-client:
	$(PYTHON) client/main.py $(PORT)

test:
	$(PYTHON) -m unittest discover -s testing -v

benchmark:
	$(PYTHON) benchmarks/stress.py $(PORT) --clients $(CLIENTS) --requests $(REQUESTS)

benchmark-generate:
	$(PYTHON) benchmarks/workload.py --output $(OUTPUT_ROOT)/generated --entries $(ENTRIES) --seed $(SEED)

benchmark-chaos:
	$(PYTHON) benchmarks/chaos.py --output-root $(OUTPUT_ROOT) --seed $(SEED) --entries $(ENTRIES) --target-duration $(TARGET_DURATION) --base-port $(BASE_PORT)

benchmark-chaos-quick:
	$(PYTHON) benchmarks/chaos.py --quick --output-root $(OUTPUT_ROOT) --seed $(SEED) --entries 2500 --target-duration 45 --calibration-requests 20 --stage-timeout 35 --completion-timeout 180 --base-port $(BASE_PORT)
