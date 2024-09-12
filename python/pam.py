import os
import logging
from irods.session import iRODSSession
import irods.client_configuration as cfg
try:
    env_file = os.environ['IRODS_ENVIRONMENT_FILE']
    auth_file = os.environ['IRODS_AUTHENTICATION_FILE']
except KeyError:
    env_file = os.path.expanduser('~/.irods/irods_environment.json')
    auth_file = os.path.expanduser('~/.irods/.irodsA')

logging.basicConfig(level=logging.DEBUG)

cfg.load()

ssl_settings = {} # Or, optionally: {'ssl_context': <user_customized_SSLContext>}
session = iRODSSession(irods_env_file=env_file, **ssl_settings)
session.pool.account._auth_file = auth_file
coll = session.collections.get("/%s/home/%s" % (session.zone, session.username))

for obj in coll.data_objects:
    print(obj)
