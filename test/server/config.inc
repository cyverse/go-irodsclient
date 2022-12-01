#!/bin/bash

export ENV_NAME=irods_test
export DOMAIN="$ENV_NAME"_default
export DBMS_HOST="$ENV_NAME"_dbms_1."$DOMAIN"
export IRODS_FIRST_EPHEMERAL_PORT=20000
export IRODS_LAST_EPHEMERAL_PORT=20199

export DBMS_CONF_HOST="$ENV_NAME"_dbms_1."$DOMAIN"
export IRODS_CONF_HOST="$ENV_NAME"_irods_1."$DOMAIN"
export IRODS_VER="4.2.8"
