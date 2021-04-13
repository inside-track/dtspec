import os

import logging

logging.basicConfig()
LOG = logging.getLogger("dtspec")
level = os.environ.get("DTSPEC_LOG_LEVEL", "ERROR").upper()
LOG.setLevel(getattr(logging, level))
print(f"dtspec log level set to {level}")
