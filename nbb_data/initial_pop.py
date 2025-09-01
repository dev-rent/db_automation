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
# Structure is build on the facts that older data might need to be updated by
# newer data
#
###############################################################################

import os
import json
import time
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

from log_config import ScriptLogger
from nbb_data.functions import fuzzy_keys
from nbb_data.models import (
    table_accounting_codes, table_administrators_natural,
    table_administrators_legal, table_company_info, table_entities,
    table_facts, table_natural_persons, table_part_int, table_shareholders,
    table_statements, table_mandates
)
from nbb_data.classes import (
    NBBConnector, References, Filing, Person, Entity, CleanedData
)

x = '1'  # server folder
debug = False

# Begin
start = time.time_ns()

load_dotenv()

pop_logger = ScriptLogger(f"logs/population_{datetime.now()}.log", level=20)
quit = "Quiting script..."
pop_logger.log.info("Initialising log...")

nbb = NBBConnector(echo=debug)

with nbb.engine.begin() as conn:
    query = conn.execute(text("SELECT dutch_name, a_2 FROM country_codes;"))

country_codes_dct = {
    str(q[0]).title(): str(q[1]).upper()
    for q in query
}

# Step 1:
temp_references = f"server{x}/temp_references"

with os.scandir(temp_references) as it:
    ref_file_lst = [
        entry.path
        for entry in it
        if entry.is_file() and entry.name.endswith(".json")
        ]

# 1.a Get basic company info
for file in ref_file_lst[:]:
    # file = file if not debug else "temp_references/0733501330.json"
    cleaned = CleanedData()

    try:
        with open(file, 'r') as ref:
            references = References(json.load(ref))

        cleaned.company_info = {
            "enterprise_id": references.enterprise_id,
            "denomination": references.enterprise_name,
            "legal_situation": references.legal_situation
        }
    except Exception as e:
        pop_logger.log.error(
            f"Failed to load reference list for {file}. Error {e}"
        )
        continue

    # Step 2
    filings_list = [
        (d["filing_id"], d["account_year"])
        for d in references.filings_list
        ]

    str_tempfolder = "server{}/temp_filing/{}.json"

    for tupl in filings_list:
        try:
            with open(str_tempfolder.format(x, tupl[0]), 'r') as data:
                filing = Filing(json.load(data))
        except Exception as e:
            pop_logger.log.error(f"{e}")
            continue

        year = tupl[1]

        # 2.a Natural Persons
        for natural in filing.administrators["NaturalPersons"]:
            try:
                temp_person = Person(natural['Person'], country_codes_dct)
                t = fuzzy_keys(temp_person.key, cleaned.persons_dict.keys())

                if t[0]:
                    old_temp_person = cleaned.persons_dict[t[1]]
                    temp_person.id = old_temp_person.id
                    temp_person.description["person_uuid"] = old_temp_person.id
                    cleaned.persons_dict[t[1]] = temp_person
                else:
                    cleaned.persons_dict[temp_person.key] = temp_person

                admin_dct = {
                    "enterprise_id": references.enterprise_id,
                    "person_uuid": temp_person.id,
                    "account_year": year
                }
                cleaned.admin_nat_list.append(admin_dct)
            except Exception as e:
                pop_logger.log.error((
                    "Whilst retrieving Natural persons for "
                    f"{references.enterprise_id}. Error {e}"))
                continue

            if natural["Mandates"]:
                for mandate in natural["Mandates"]:
                    try:
                        man_dct = {
                            "person_uuid": temp_person.id,
                            "enterprise_id": references.enterprise_id,
                            "function_code": (
                                mandate["FunctionMandate"].replace(
                                    "fct:m", "")
                                if mandate.get("FunctionMandate")
                                else None
                                ),
                            "start_date": (
                                datetime.strptime(
                                    mandate["MandateDates"]["StartDate"],
                                    "%Y-%m-%d"
                                )
                                if mandate["MandateDates"].get("StartDate")
                                else None
                                ),
                            "end_date": (
                                datetime.strptime(
                                    mandate["MandateDates"]["EndDate"],
                                    "%Y-%m-%d"
                                )
                                if mandate["MandateDates"].get("EndDate")
                                else None
                                ),
                            "account_year": year
                        }
                    except Exception as e:
                        pop_logger.log.error((
                            f"Mandates: {references.enterprise_id}. "
                            f"Error: {e}"
                            ))
                        continue

                    cleaned.mandates_list.append(man_dct)

        # 2.b Legal Persons
        for legal in filing.administrators["LegalPersons"]:
            try:
                temp_entity = Entity(legal["Entity"], country_codes_dct)

                if temp_entity.key in cleaned.entities_dict.keys():
                    old_temp_entity = cleaned.entities_dict[temp_entity.key]
                    temp_entity.id = old_temp_entity.id
                    temp_entity.description['entity_uuid'] = old_temp_entity.id
                    cleaned.entities_dict[temp_entity.key] = temp_entity
                else:
                    cleaned.entities_dict[temp_entity.key] = temp_entity

            except Exception as e:
                pop_logger.log.error((
                    "Whilst retrieving Legal Persons 'entity' for "
                    f"{references.enterprise_id}. Error {e}"))
                continue

            for representative in legal["Representatives"]:
                try:
                    temp_person = Person(representative, country_codes_dct)
                    t = fuzzy_keys(
                        temp_person.key,
                        cleaned.persons_dict.keys()
                        )

                    if t[0]:
                        old_temp_person = cleaned.persons_dict[t[1]]
                        temp_person.id = old_temp_person.id
                        temp_person.description[
                            "person_uuid"] = old_temp_person.id
                        cleaned.persons_dict[t[1]] = temp_person
                    else:
                        cleaned.persons_dict[temp_person.key] = temp_person

                    admin_dct = {
                        "enterprise_id": references.enterprise_id,
                        "entity_uuid": temp_entity.id,
                        "person_uuid": temp_person.id,
                        "account_year": year
                    }
                    cleaned.admin_legal_list.append(admin_dct)

                    if legal["Mandates"]:
                        for mandate in legal["Mandates"]:
                            try:
                                man_dct = {
                                    "person_uuid": temp_person.id,
                                    "enterprise_id": references.enterprise_id,
                                    "function_code": (
                                        mandate["FunctionMandate"].replace(
                                            "fct:m", "")
                                        if mandate.get("FunctionMandate")
                                        else None
                                        ),
                                    "start_date": (
                                        datetime.strptime(
                                            mandate["MandateDates"]["StartDate"],
                                            "%Y-%m-%d"
                                        )
                                        if mandate["MandateDates"].get("StartDate")
                                        else None
                                        ),
                                    "end_date": (
                                        datetime.strptime(
                                            mandate["MandateDates"]["EndDate"],
                                            "%Y-%m-%d"
                                        )
                                        if mandate["MandateDates"].get("EndDate")
                                        else None
                                        ),
                                    "account_year": year
                                }
                            except Exception as e:
                                pop_logger.log.error((
                                    f"Mandates: {references.enterprise_id}. "
                                    f"Error: {e}"
                                    ))
                                continue

                            cleaned.mandates_list.append(man_dct)
                except Exception as e:
                    pop_logger.log.error((
                        "Whilst retrieving representative for "
                        f"{references.enterprise_id}. Error {e}"
                    ))

        # 2.c Participating Interests
        for partint in filing.participating_interests:
            try:
                temp_entity = Entity(partint["Entity"], country_codes_dct)

                if temp_entity.key in cleaned.entities_dict.keys():
                    old_temp_entity = cleaned.entities_dict[temp_entity.key]
                    temp_entity.id = old_temp_entity.id
                    temp_entity.description['entity_uuid'] = old_temp_entity.id
                    cleaned.entities_dict[temp_entity.key] = temp_entity
                else:
                    cleaned.entities_dict[temp_entity.key] = temp_entity

            except Exception as e:
                pop_logger.log.error((
                    "Partint / Entity for "
                    f"{references.enterprise_id}. Error: {e}"
                    ))
                continue

            try:
                base_dct = {
                    "enterprise_id": references.enterprise_id,
                    "entity_uuid": temp_entity.id,
                    "account_year": year,
                    "account_date": (
                        datetime.strptime(
                            partint["AccountDate"],
                            "%Y-%m-%d"
                        )
                        if partint.get("AccountDate")
                        else None
                        ),
                    "currency": (
                        partint["Currency"].replace("ccy:m", "")
                        if partint.get("Currency")
                        else None
                        ),
                    "equity": int(float(partint.get("Equity"))),
                    "net_result": int(float(partint.get("NetResult")))
                }

                for p in partint["ParticipatingInterestHeld"]:
                    temp_dct = base_dct.copy()

                    add_dct = {
                        "nature": p.get("Nature"),
                        "line": p.get("Line"),
                        "amount": p.get("Number"),
                        "percentage_held": p.get("PercentageDirectlyHeld"),
                        "percentage_subsidiary": p.get(
                            "PercentageSubsidiaries")
                    }

                    temp_dct.update(add_dct)
                    cleaned.part_interest_list.append(temp_dct)

            except Exception as e:
                pop_logger.log.error(
                    f"PartIntHeld for {references.enterprise_id}. Error: {e}")
                continue

        # 2.d Shareholders
        if filing.shareholders.get("EntityShareHolders"):
            for entity in filing.shareholders["EntityShareHolders"]:
                try:
                    temp_entity = Entity(entity["Entity"], country_codes_dct)

                    if temp_entity.key in cleaned.entities_dict.keys():
                        old_temp_entity = cleaned.entities_dict[
                            temp_entity.key]
                        temp_entity.id = old_temp_entity.id
                        temp_entity.description[
                            'entity_uuid'] = old_temp_entity.id
                        cleaned.entities_dict[temp_entity.key] = temp_entity
                    else:
                        cleaned.entities_dict[temp_entity.key] = temp_entity
                except Exception as e:
                    pop_logger.log.error((
                        "Shareholders / Entity for "
                        f"{references.enterprise_id}. Error: {e}"
                        ))
                    continue

                try:
                    base_dct = {
                        "enterprise_id": references.enterprise_id,
                        "entity_uuid": temp_entity.id,
                        "account_year": year,
                    }
                    for s in entity["RightsHeld"]:
                        temp_dct = base_dct.copy()

                        add_dct = {
                            "nature_rights": s.get("Nature"),
                            "line_rights": s.get("Line"),
                            "securities_attached": s.get(
                                "NumberSecuritiesAttached"),
                            "not_securities_attached": s.get(
                                "not_securities_attached"),
                            "percentage": s.get("Percentage")
                        }

                        temp_dct.update(add_dct)
                        cleaned.shareholders_list.append(temp_dct)

                except Exception as e:
                    pop_logger.log.error((
                        f"PartIntHeld for {references.enterprise_id}. "
                        f"Error: {e}"
                        ))
                    continue

        # 2.e Rubrics
        try:
            for r in filing.rubrics:
                if r["Period"] == "N":
                    code = str(r.get("Code"))
                    cleaned.accounting_codes.append({
                        "accountcode_id": code, "denomination": code
                        })
                    cleaned.facts_list.append({
                        "account_year": year,
                        "filing_id": filing.reference_number,
                        "accountcode_id": code,
                        "book_value": r.get("Value")
                    })
                else:
                    # if period is NM1
                    pass
        except Exception as e:
            pop_logger.log.error((
                f"Rubrics {references.enterprise_id}, filing id "
                f"{filing.reference_number}. Error {e}"
            ))
            continue

    # Step 3
    statements_to_execute = []
    if cleaned.company_info:
        stmt0 = insert(table_company_info).values(cleaned.company_info)
        stmt0 = stmt0.on_conflict_do_update(
            index_elements=["enterprise_id"],
            set_={
                "denomination": stmt0.excluded.denomination,
                "legal_situation": stmt0.excluded.legal_situation
            }
        )
        statements_to_execute.append(stmt0)

    if references.filings_list:
        stmt_statements = insert(table_statements).values(
            references.filings_list)
        stmt_statements = stmt_statements.on_conflict_do_update(
            index_elements=[
                "enterprise_id", "start_date", "end_date"
            ],
            set_={
                "filing_id": stmt_statements.excluded.filing_id,
                "account_year": stmt_statements.excluded.account_year,
                "deposit_date": stmt_statements.excluded.deposit_date,
                "deposit_type": stmt_statements.excluded.deposit_type,
                "legal_form": stmt_statements.excluded.legal_form,
                "activity_code": stmt_statements.excluded.activity_code,
                "model_type": stmt_statements.excluded.model_type,
                "last_update": stmt_statements.excluded.last_update
            }
        )
        statements_to_execute.append(stmt_statements)

    if cleaned.persons_dict:
        stmt1 = insert(table_natural_persons).values([
            v.description
            for v in cleaned.persons_dict.values()
            ])
        stmt1 = stmt1.on_conflict_do_update(
            index_elements=[
                "first_name", "last_name", "street", "street_number"
            ],
            set_={
                "person_uuid": stmt1.excluded.person_uuid,
                "zipcode": stmt1.excluded.zipcode,
                "country_code": stmt1.excluded.country_code
            }
        )
        statements_to_execute.append(stmt1)

    if cleaned.entities_dict:
        stmt4 = insert(table_entities).values([
            v.description
            for v in cleaned.entities_dict.values()
        ])
        stmt4 = stmt4.on_conflict_do_update(
            index_elements=[
                "entity_id", "country_code"
            ],
            set_={
                "entity_uuid": stmt4.excluded.entity_uuid,
                "denomination": stmt4.excluded.denomination,
                "street": stmt4.excluded.street,
                "street_number": stmt4.excluded.street_number,
                "zipcode": stmt4.excluded.zipcode
            }
        )
        statements_to_execute.append(stmt4)

    if cleaned.admin_nat_list:
        stmt2 = insert(table_administrators_natural).values(
            cleaned.admin_nat_list)
        stmt2 = stmt2.on_conflict_do_nothing(
            # index_elements=[
            #     "enterprise_id", "person_uuid", "account_year"
            # ],
            # set_={
            #     "enterprise_id": stmt2.excluded.enterprise_id,
            #     "person_uuid": stmt2.excluded.person_uuid,
            #     "account_year": stmt2.excluded.account_year
            # }
        )
        statements_to_execute.append(stmt2)

    if cleaned.admin_legal_list:
        stmt3 = insert(table_administrators_legal).values(
            cleaned.admin_legal_list)
        stmt3 = stmt3.on_conflict_do_nothing(
            # index_elements=[
            #     "enterprise_id", "entity_uuid", "person_uuid", "account_year"
            # ],
            # set_={
            #     "enterprise_id": stmt3.excluded.enterprise_id,
            #     "entity_uuid": stmt3.excluded.entity_uuid,
            #     "person_uuid": stmt3.excluded.person_uuid,
            #     "account_year": stmt3.excluded.account_year,
            # }
        )
        statements_to_execute.append(stmt3)

    if cleaned.mandates_list:
        stmt_man = insert(table_mandates).values(cleaned.mandates_list)
        stmt_man = stmt_man.on_conflict_do_nothing()
        statements_to_execute.append(stmt_man)

    if cleaned.part_interest_list:
        stmt5 = insert(table_part_int).values(
            cleaned.part_interest_list)
        stmt5 = stmt5.on_conflict_do_update(
            index_elements=[
                "enterprise_id", "entity_uuid", "account_year"
            ],
            set_={
                "account_date": stmt5.excluded.account_date,
                "currency": stmt5.excluded.currency,
                "equity": stmt5.excluded.equity,
                "net_result": stmt5.excluded.net_result,
                "nature": stmt5.excluded.nature,
                "line": stmt5.excluded.line,
                "amount": stmt5.excluded.amount,
                "percentage_held": stmt5.excluded.percentage_held,
                "percentage_subsidiary": stmt5.excluded.percentage_subsidiary
            }
        )
        statements_to_execute.append(stmt5)

    if cleaned.shareholders_list:
        stmt6 = insert(table_shareholders).values(
            cleaned.shareholders_list)
        stmt6 = stmt6.on_conflict_do_update(
            index_elements=[
                "enterprise_id", "entity_uuid", "person_uuid",
                "account_year"
            ],
            set_={
                "nature_rights": stmt6.excluded.nature_rights,
                "line_rights": stmt6.excluded.line_rights,
                "securities_attached": stmt6.excluded.securities_attached,
                "not_securities_attached": (
                    stmt6.excluded.not_securities_attached),
                "percentage": stmt6.excluded.percentage
            }
        )
        statements_to_execute.append(stmt6)

    if cleaned.accounting_codes:
        stmt_acc_codes = insert(table_accounting_codes).values(
            cleaned.accounting_codes)
        stmt_acc_codes = stmt_acc_codes.on_conflict_do_nothing()
        statements_to_execute.append(stmt_acc_codes)

    if cleaned.facts_list:
        stmt_facts = insert(table_facts).values(
            cleaned.facts_list)
        stmt_facts = stmt_facts.on_conflict_do_update(
            index_elements=[
                "account_year", "filing_id", "accountcode_id"
            ],
            set_={"book_value": stmt_facts.excluded.book_value}
        )
        statements_to_execute.append(stmt_facts)

    try:
        with nbb.engine.begin() as conn:
            for stmt in statements_to_execute:
                conn.execute(stmt)
    except Exception as e:
        pop_logger.log.error((
            f"Failed uploading data to DB of {references.enterprise_id} - "
            f"Error: {e}"
        ))
