.PHONY: pip-reqs create-topics list-topics topics gen-clicks gen-cart gen-purchases stage2-run stage2-stop stage2-clean features-stream feast-apply feast-mat unit-test integration-test test

pip-reqs:
	pip freeze --local > requirements.txt

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

stage2-run:
	docker compose up -d

stage2-stop:
	docker compose down -v

stage2-clean:
	rm -rf data/offline/partition_date=*

features-stream:
	python3 pipelines/feature_engineering.py

feast-apply:
	cd feature_repo && feast apply

feast-mat:
	cd feature_repo && feast materialize-incremental `date -u +%Y-%m-%dT%H:%M:%SZ`

unit-test:
	PYTHONPATH=. pytest -q -k 'avro or aggregation'

integration-test:
	PYTHONPATH=. pytest -q -m integration; \
	code=$$?; \
	if [ $$code -ne 0 ] && [ $$code -ne 5 ]; then exit $$code; fi

test: unit-test integration-test