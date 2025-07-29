.PHONY: create-topics list-topics topics gen-clicks gen-cart gen-purchases

create-topics:
	for TOPIC in product-clicks cart-adds purchases; do \
	  docker exec kafka kafka-topics \
	    --create --if-not-exists \
	    --bootstrap-server localhost:9092 \
	    --replication-factor 1 \
	    --partitions 3 \
	    --topic "$$TOPIC"; \
	done

list-topics:
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

topics: create-topics list-topics

gen-clicks:
	python3 scripts/event_generator.py -t product-clicks -s schemas/product_clicks.avsc -k user_id -i 0.5 -c 500
gen-cart:
	python3 scripts/event_generator.py -t cart-adds -s schemas/cart_adds.avsc -k user_id -i 1.0 -c 200
gen-purchases:
	python3 scripts/event_generator.py -t purchases -s schemas/purchases.avsc -k user_id -i 2.0 -c 100

.PHONY: test unit-test integration-test smoke

unit-test:
	pytest tests/unit -q

integration-test:
	@echo "Generating test messages in Kafka…"
	python3 scripts/event_generator.py \
	  --schema cart_adds.avsc \
	  --topic cart-adds \
	  --count 10 \
	  --bootstrap-servers localhost:9092
	@echo "Running integration tests…"
	pytest -m integration tests/integration -q

test: unit-test integration-test

smoke:
	python3 scripts/print_events.py \
		--topic cart-adds \
		--schema schemas/cart_adds.avsc \
		--bootstrap-server localhost:9092 \
		--timeout-ms 5000 \
		--max-messages 10
