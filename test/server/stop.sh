#! /bin/bash

export ENV_NAME=irods_test

docker-compose --file docker-compose.yml --project-name "$ENV_NAME" down --remove-orphans