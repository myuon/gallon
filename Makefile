up:
	docker compose up

down:
	docker compose down

migrate-mysql:
	go1.20 run ./test/data_to_mysql/main.go

run:
	go1.20 run main.go run $(file)

doc:
	reflex -s -- sh -c 'pkgsite -http=:9090'
