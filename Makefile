.PHONY: dynamodb-up dynamodb-down dynamodb-seed dynamodb-local run-local dynamate-local dynamate-local-debug dynamate-compose

COMPOSE ?= docker compose
DYNAMO_ENDPOINT ?= http://localhost:8000
DYNAMO_ENDPOINT_DOCKER ?= http://dynamodb:8000
DYNAMO_TABLE ?= dyno-music
AWS_REGION ?= us-east-1
AWS_ACCESS_KEY_ID ?= local
AWS_SECRET_ACCESS_KEY ?= local
SEED_ARGS ?= --skip-if-exists
DYNAMATE_ARGS ?=
RELEASE ?=

dynamodb-up:
	@if [ -z "$$($(COMPOSE) -f compose.yaml ps -q --status=running dynamodb)" ]; then \
		$(COMPOSE) -f compose.yaml up -d dynamodb; \
	fi
	@printf "Waiting for DynamoDB Local at %s...\n" "$(DYNAMO_ENDPOINT)"
	@until curl --silent "$(DYNAMO_ENDPOINT)" >/dev/null; do sleep 0.2; done
	@printf "DynamoDB Local is ready.\n"

dynamodb-down:
	$(COMPOSE) -f compose.yaml down

dynamodb-seed:
	@DYNAMO_ENDPOINT="$(DYNAMO_ENDPOINT_DOCKER)" \
	DYNAMO_TABLE="$(DYNAMO_TABLE)" \
	AWS_REGION="$(AWS_REGION)" \
	AWS_ACCESS_KEY_ID="$(AWS_ACCESS_KEY_ID)" \
	AWS_SECRET_ACCESS_KEY="$(AWS_SECRET_ACCESS_KEY)" \
	SEED_ARGS="$(SEED_ARGS)" \
	$(COMPOSE) -f compose.yaml run --rm seed

dynamodb-local: dynamodb-up dynamodb-seed

run-local: dynamodb-local
	@AWS_REGION="$(AWS_REGION)" \
	AWS_ACCESS_KEY_ID="$(AWS_ACCESS_KEY_ID)" \
	AWS_SECRET_ACCESS_KEY="$(AWS_SECRET_ACCESS_KEY)" \
	cargo run $(RELEASE) -- --endpoint-url "$(DYNAMO_ENDPOINT)" --table "$(DYNAMO_TABLE)" $(DYNAMATE_ARGS)

dynamate-local:
	@AWS_REGION="$(AWS_REGION)" \
	AWS_ACCESS_KEY_ID="$(AWS_ACCESS_KEY_ID)" \
	AWS_SECRET_ACCESS_KEY="$(AWS_SECRET_ACCESS_KEY)" \
	cargo run $(RELEASE) -- --endpoint-url "$(DYNAMO_ENDPOINT)" --table "$(DYNAMO_TABLE)" $(DYNAMATE_ARGS)

dynamate-local-debug: dynamodb-local
	@DYNAMATE_DATA="." \
	DYNAMATE_LOGLEVEL="dynamate=trace" \
	DYNAMATE_LOG_STDERR=1 \
	AWS_REGION="$(AWS_REGION)" \
	AWS_ACCESS_KEY_ID="$(AWS_ACCESS_KEY_ID)" \
	AWS_SECRET_ACCESS_KEY="$(AWS_SECRET_ACCESS_KEY)" \
	cargo run $(RELEASE) -- --endpoint-url "$(DYNAMO_ENDPOINT)" --table "$(DYNAMO_TABLE)" $(DYNAMATE_ARGS)

dynamate-compose: dynamodb-local
	@DYNAMATE_USE_TTYD="$${DYNAMATE_USE_TTYD:-1}" \
	AWS_REGION="$(AWS_REGION)" \
	AWS_ACCESS_KEY_ID="$(AWS_ACCESS_KEY_ID)" \
	AWS_SECRET_ACCESS_KEY="$(AWS_SECRET_ACCESS_KEY)" \
	DYNAMO_TABLE="$(DYNAMO_TABLE)" \
	DYNAMO_ENDPOINT="$(DYNAMO_ENDPOINT_DOCKER)" \
	$(COMPOSE) -f compose.yaml --profile dynamate up --build -d dynamate
