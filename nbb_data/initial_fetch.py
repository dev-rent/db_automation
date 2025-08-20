###############################################################################
# This script reads a dedicated CSV file for enterprise ids.
#
# Step 1: Read enterprise ids and put them in a list.
# Step 2: API call to NBB for reference list.
# Step 3: If success, sort list ascending and only keep data where year > 2021.
# Step 4: Write company references in 'temp_references'.
# Step 5: Fetch all filings in references and write to 'temp_filing'.
#
###############################################################################

import os
import csv
import json
import uuid
import requests
from datetime import datetime
# from sqlalchemy import URL, create_engine

from log_config import ScriptLogger
from nbb_data.classes import URLgen_nbb


ref_logger = ScriptLogger('ref_url.log', level=20)
data_logger = ScriptLogger('data_url.log', level=20)

success = 0
fail = 0
failed_ent_list = []

# Step 1
with open('server1.csv', newline="") as csvfile:
    read = csv.reader(csvfile)
    enterprise_lst = [x[0] for x in read]

length = len(enterprise_lst)

# Step 2
api_authentic = os.getenv("API_KEY_AUTHENTIC")

hdr_ref = {
    'X-Request-Id': str(uuid.uuid4()),
    'NBB-CBSO-Subscription-Key': api_authentic,
    'Accept': "application/json",
    'User-Agent': 'PostmanRuntime/7.37.3'
}

hdr_accData = {
    'X-Request-Id': str(uuid.uuid4()),
    'NBB-CBSO-Subscription-Key': api_authentic,
    'Accept': "application/x.jsonxbrl",
    'User-Agent': 'PostmanRuntime/7.37.3'
}

for ent in enterprise_lst[:2]:
    ent = ent.replace(".", "")
    url_nbb = URLgen_nbb(db="authentic", request="ref", ref_id=ent).url

    try:
        resp = requests.get(url_nbb, headers=hdr_ref)
    except Exception:
        ref_logger.log.error(f"no response for {url_nbb}")
        fail += 1
        failed_ent_list.append(ent)
        continue

    json_data = resp.json()
    if not isinstance(json_data, list) or not json_data:
        ref_logger.log.error((
            f"Empty or invalid JSON for {ent}. "
            f"Data: {json_data}. "
            f"URL: {url_nbb}"
        ))
        fail += 1
        failed_ent_list.append(ent)
        continue

    # Step 3
    try:
        list_of_ref = sorted(
            (
                x for x in json_data
                if datetime.strptime(
                    x['ExerciseDates']['endDate'],
                    "%Y-%m-%d"
                    ).year >= 2021
            ),
            key=lambda x: x['ExerciseDates']['endDate']
            )
    except Exception as e:
        ref_logger.log.error(f"No exercise dates found in {ent}: {e}")
        fail += 1
        failed_ent_list.append(ent)
        continue

    acc_ref_list: list[tuple] = []
    for dct in list_of_ref:
        acc_ref_list.append(
            (dct.get('AccountingDataURL', ''), dct.get('ReferenceNumber'))
            )

    # Step 4
    target = "temp_references/{}.json"
    with open(target.format(ent), 'w') as file:
        json.dump(list_of_ref, file, indent=4)

    # Step 5: Fetch companies filings
    data_list = []
    target = "temp_filing/{}.json"
    for ref in acc_ref_list:
        try:
            resp = requests.get(ref[0], headers=hdr_accData)
        except Exception as e:
            data_logger.log.error(f"For {ent} - {ref[1]}: {e}")
            continue

        if resp.status_code != 200:
            data_logger.log.warning(
                f"Status: {resp.status_code} for {ent} - {ref[1]}")
            continue

        data_list.append(ref[1])
        with open(target.format(ref[1]), 'wb') as data:
            data.write(resp.content)

    success += 1

ref_logger.log.info(f"{success} of {length} succesfully fetched.")
ref_logger.log.info(f"{fail} of {length} failed to fetched.")
ref_logger.log.info(f"List of fails: {failed_ent_list}.")
