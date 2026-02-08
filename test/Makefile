.PHONY: up down smoke test stress soak logs

up:
	docker compose up -d --build

down:
	docker compose down -v

smoke:
	k6 run k6/smoke_test.js

test:
	k6 run k6/pubsub_test.js

stress:
	k6 run k6/stress_test.js

soak:
	k6 run k6/soak_test.js

logs:
	docker compose logs -f
