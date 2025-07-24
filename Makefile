.PHONY: create-topics list-topics topics

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
