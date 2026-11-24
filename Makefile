PYTHON ?= python3
PID_DIR := .pids

.PHONY: start add delete pause resume killall clean

start:
	@mkdir -p $(PID_DIR)
	@for k in 1 2 3 4 5; do \
		if [ -f "$(PID_DIR)/node_$$k.pid" ] && kill -0 "$$(cat "$(PID_DIR)/node_$$k.pid")" 2>/dev/null; then \
			echo "Node $$k already running (PID $$(cat "$(PID_DIR)/node_$$k.pid"))"; \
		else \
			$(PYTHON) node.py --id $$k > "$(PID_DIR)/node_$$k.out" 2>&1 & \
			echo $$! > "$(PID_DIR)/node_$$k.pid"; \
			echo "Started node $$k (PID $$!)"; \
		fi; \
	done

add:
	@if [ -z "$(NODE)" ] || [ -z "$(SONG)" ]; then \
		echo "Usage: make add NODE={k} SONG={song}"; \
		exit 1; \
	fi
	@$(PYTHON) cli.py propose --node $(NODE) --song $(SONG) --op ADD

delete:
	@if [ -z "$(NODE)" ] || [ -z "$(SONG)" ]; then \
		echo "Usage: make delete NODE={k} SONG={song}"; \
		exit 1; \
	fi
	@$(PYTHON) cli.py propose --node $(NODE) --song $(SONG) --op DELETE

pause:
	@if [ -z "$(NODE)" ]; then \
		echo "Usage: make pause NODE={k}"; \
		exit 1; \
	fi
	@$(PYTHON) cli.py pause --node $(NODE)

resume:
	@if [ -z "$(NODE)" ]; then \
		echo "Usage: make resume NODE={k}"; \
		exit 1; \
	fi
	@$(PYTHON) cli.py resume --node $(NODE)

killall:
	@for k in 1 2 3 4 5; do \
		if [ -f "$(PID_DIR)/node_$$k.pid" ]; then \
			PID=$$(cat "$(PID_DIR)/node_$$k.pid"); \
			if kill -0 "$$PID" 2>/dev/null; then \
				kill "$$PID" 2>/dev/null || true; \
				echo "Killed node $$k (PID $$PID)"; \
			fi; \
			rm -f "$(PID_DIR)/node_$$k.pid"; \
		fi; \
	done
	@rm -rf $(PID_DIR)

clean:
	@rm -f playlist_*.txt changes_*.log messages_*.log
	@rm -rf $(PID_DIR) __pycache__ .pytest_cache