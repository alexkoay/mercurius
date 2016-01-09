import sys
import logging

logging.addLevelName(10, 'debug')
logging.addLevelName(12, 'sql')
logging.addLevelName(15, 'verbose')
logging.addLevelName(20, 'info')
logging.addLevelName(30, 'warning')
logging.addLevelName(40, 'error')
logging.addLevelName(50, 'critical')
logging.basicConfig(level=13, format='{asctime} [{name}] {message}', datefmt='%Y-%m-%d %H:%M:%S', style='{')
