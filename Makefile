build-hardware:
	@docker buildx build -f docker/hardware.Dockerfile -t gogo-hardware:v1.0.0 .  

build:
	@docker buildx build -f docker/hardware.Dockerfile -t gogo:v1.0.0 .  

run:
	@docker compose up -d