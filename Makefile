build:
	@docker buildx build -f docker/hardware.Dockerfile -t gogo:v1.0.0 .  