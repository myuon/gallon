up:
	docker compose up

down:
	docker compose down

migrate-mysql:
	go run ./test/data_to_mysql/main.go
