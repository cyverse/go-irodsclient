#! /bin/bash

export ENV_NAME=irods_test
export DOMAIN="$ENV_NAME"_default
export IRODS_CONF_HOST="$ENV_NAME"_irods_1."$DOMAIN"
export IRODS_VER="4.2.8"

docker-compose --file docker-compose.yml --project-name "$ENV_NAME" down --remove-orphans
docker-compose --file docker-compose.yml --project-name "$ENV_NAME" up -d