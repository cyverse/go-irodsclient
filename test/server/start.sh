#! /bin/bash

docker rm irods_test &> /dev/null
docker rm irods_db_test &> /dev/null
docker-compose up