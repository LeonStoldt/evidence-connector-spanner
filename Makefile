PROJECT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
DEMO_APP_DIR:=${PROJECT_DIR}/demo

install:
	@echo "Updating the plugin dependencies"
	npm install

run-demo:
	@echo "Installing the plugin into the demo app"
	export SPANNER_EMULATOR_HOST=localhost:9010 && \
	cd "${DEMO_APP_DIR}" && \
	npm install "${PROJECT_DIR}" && \
	\
	echo "Run the test app" && \
	npm run sources && \
	npm run dev
	
build-spanner-emulator:
	docker build --pull -t evidence-test-spanner-emulator "${PROJECT_DIR}/spanner-emulator"

run-spanner-emulator: build-spanner-emulator
	docker run --rm -p "127.0.0.1:9010:9010" -p "127.0.0.1:9020:9020" evidence-test-spanner-emulator