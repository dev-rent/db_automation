###############################################################################
#
# This script populates the 'nbb_data' databsase.
#
# It needs to follow a certain sequence to ensure data integrity. In general
# this sequence goes as follows:
#   - separate initial filings from corrections, and begin with initial.
#   - first extract ALL natural persons and entities from the file.
#   - second extract ALL qualitative relations from the file.
#   - last extract ALL quantitative data (= rubrics).
#
# Also the sequence of database transactions matters and follows the same logic
# as above. Optimalisation is yet to be determined.
#
###############################################################################

import os
import json
import time
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import (
    MetaData, Table, Column, String, Integer, Date, Uuid, Float, text
)
from sqlalchemy.dialects.postgresql import insert

from log_config import ScriptLogger
from nbb_data.classes import NBBConnector, References, Filing, Person, Entity


start = time.time_ns()

load_dotenv()

pop_logger = ScriptLogger('population.log', level=20)
quit = "Quiting script..."
pop_logger.log.info("Initialising log...")


