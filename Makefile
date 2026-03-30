DBF_SOURCE_DIR ?= /home/matias/DESARROLLO/TEST/APRENDICES/bck-socios/socios/bases
DBF_ENCODING ?= latin1
DBF_PAGE_SIZE ?= 100

.PHONY: up down restart logs

up:
	DBF_SOURCE_DIR="$(DBF_SOURCE_DIR)" DBF_ENCODING="$(DBF_ENCODING)" DBF_PAGE_SIZE="$(DBF_PAGE_SIZE)" docker compose up --build -d

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f
