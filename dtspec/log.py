import os

import logging

logging.basicConfig()
LOG = logging.getLogger('dtspec')
LOG.setLevel(getattr(logging, os.environ.get('DTSPEC_LOG_LEVEL', 'ERROR')))
