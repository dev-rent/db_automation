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
import re
import json
import time
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

from log_config import ScriptLogger
from nbb_data.models import (
    table_accounting_codes, table_company_info,
    table_entities, table_facts, table_legal_persons, table_natural_persons,
    table_part_int, table_shareholders, table_statements
)
from nbb_data.classes import NBBConnector, References, Filing, Person, Entity


start = time.time_ns()

load_dotenv()

pop_logger = ScriptLogger('logs/population.log', level=20)
quit = "Quiting script..."
pop_logger.log.info("Initialising log...")

nbb = NBBConnector(echo=True)

with nbb.engine.begin() as conn:
    query = conn.execute(text("SELECT dutch_name, a_2 FROM country_codes;"))

country_codes_dct = {
    str(q[0]).title(): str(q[1]).upper()
    for q in query
}

# Step 1:
temp_references = "temp_references"

with os.scandir(temp_references) as it:
    ref_file_lst = [
        entry.path
        for entry in it
        if entry.is_file() and entry.name.endswith(".json")
        ]

# 1.a Get basic company info
for file in ref_file_lst[:1]:
    file = "temp_references/0400032156.json"

    try:
        with open(file, 'r') as ref:
            references = References(json.load(ref))

        company_info_dct = {
            "enterprise_id": references.enterprise_id,
            "denomination": references.enterprise_name,
            "legal_situation": references.legal_situation
        }
    except Exception as e:
        pop_logger.log.error(
            f"Failed to load reference list for {file}. Error {e}"
        )
        continue

    stmt_company_info = insert(table_company_info).values(company_info_dct)
    stmt_company_info = stmt_company_info.on_conflict_do_update(
        index_elements=["enterprise_id"],
        set_={
            "denomination": stmt_company_info.excluded.denomination,
            "legal_situation": stmt_company_info.excluded.legal_situation
        })

    stmt_statements = insert(
        table_statements
        ).values(
            references.initial_list
            ).on_conflict_do_nothing()

    with nbb.engine.begin() as conn:
        conn.execute(stmt_company_info)
        conn.execute(stmt_statements)

    del stmt_company_info
    del stmt_statements

    # Step 2
    filings_list = [
        (d["filing_id"], d["account_year"])
        for d in references.initial_list
        ]

    str_tempfolder = "temp_filing/{}.json"

    for tupl in filings_list:
        try:
            with open(str_tempfolder.format(tupl[0]), 'r') as data:
                filing = Filing(json.load(data))
        except Exception as e:
            pop_logger.log.error(f"{e}")
            continue

        year = tupl[1]

        # 2.a Natural Persons
        # Do not change because list needs to be empty for every filing
        persons_list = []
        # Exclusively for the next for-loop. If a persons appears in Natural
        # Persons as well as Legal Persons, they can't be updated within the
        # same transaction in the database. So they need to executed separatly.
        natural_persons_list = []
        for natural in filing.administrators["NaturalPersons"]:
            try:
                temporary_person = Person(natural['Person'], country_codes_dct)
            except Exception as e:
                pop_logger.log.error((
                    "Whilst retrieving Natural persons for "
                    f"{references.enterprise_id}. Error {e}"))
                continue

            natural_persons_list.append(temporary_person.description)

        if natural_persons_list:
            with nbb.engine.begin() as conn:
                stmt = insert(table_natural_persons).values(
                    natural_persons_list)
                stmt = stmt.on_conflict_do_update(
                        index_elements=[
                            "first_name", "last_name",
                            "street", "street_number"
                        ],
                        set_={"person_id": stmt.excluded.person_id}
                        )
                conn.execute(stmt)

        # 2.b Legal Persons
        entities_list = []
        legal_persons_list = []
        for legal in filing.administrators["LegalPersons"]:
            try:
                temporary_entity = Entity(legal["Entity"], country_codes_dct)
            except Exception as e:
                pop_logger.log.error((
                    "Whilst retrieving Legal Persons 'entity' for "
                    f"{references.enterprise_id}. Error {e}"))
                continue

            entities_list.append(temporary_entity.description)

            for representative in legal["Representatives"]:
                temporary_person = Person(representative, company_info_dct)
                persons_list.append(temporary_person.description)
                base_dct = {
                    "enterprise_id": references.enterprise_id,
                    "entity_id": temporary_entity.id,
                    "natural_person": temporary_person.id,
                }
                if legal["Mandates"]:
                    for mandate in legal["Mandates"]:
                        try:
                            temp_dct = base_dct.copy()
                            add_dct = {
                                "function_code": mandate.get(
                                    "FunctionMandate").replace("fct:m", ""),
                                "start_date": datetime.strptime(
                                    mandate["MandateDates"].get(
                                        "StartDate"
                                    ), "%Y-%m-%d"),
                                "end_date": datetime.strptime(
                                    mandate["MandateDates"].get(
                                        "StartDate"
                                    ), "%Y-%m-%d")
                            }
                        except Exception as e:
                            pop_logger.log.error((
                                f"Mandates: {references.enterprise_id}. "
                                f"Error: {e}"
                                ))
                            continue

                        temp_dct.update(add_dct)
                        legal_persons_list.append(temp_dct)
                else:
                    legal_persons_list.append(base_dct)

        # 2.c Participating Interests
        part_int_list = []
        for partint in filing.participating_interests:
            try:
                temporary_entity = Entity(
                    partint["Entity"], country_codes_dct)
                entities_list.append(temporary_entity.description)
            except Exception as e:
                pop_logger.log.error((
                    "Partint / Entity for "
                    f"{references.enterprise_id}. Error: {e}"
                    ))
                continue
            try:
                base_dct = {
                    "enterprise_id": references.enterprise_id,
                    "entity_id": re.sub(
                        r"\D", "", partint["Entity"].get("Identifier")),
                    "account_year": year,
                    "account_date": datetime.strptime(
                        partint.get("AccountDate"),
                        "%Y-%m-%d"
                        ),
                    "currency": partint.get("Currency").replace("ccy:m", ""),
                    "equity": int(partint.get("Equity")),
                    "net_result": int(partint.get("NetResult"))
                }

                for p in partint["ParticipatingInterestHeld"]:
                    temp_dct = base_dct.copy()

                    add_dct = {
                        "nature": p.get("Nature"),
                        "line": p.get("Line"),
                        "amount": p.get("Number"),
                        "percentage_held": p.get("PercentageDirectlyHeld"),
                        "percentage_subsidiary": p.get("PercentageSubsidiaries")
                    }

                    temp_dct.update(add_dct)
                    part_int_list.append(temp_dct)

            except Exception as e:
                pop_logger.log.error(
                    f"PartIntHeld for {references.enterprise_id}. Error: {e}")
                continue

        # 2.d Shareholders
        shareholders_list = []
        if filing.shareholders.get("EntityShareHolders"):
            for entity in filing.shareholders["EntityShareHolders"]:
                try:
                    temporary_entity = Entity(
                        entity["Entity"], country_codes_dct)
                    entities_list.append(temporary_entity.description)
                except Exception as e:
                    pop_logger.log.error((
                        "Shareholders / Entity for "
                        f"{references.enterprise_id}. Error: {e}"
                        ))
                    continue

                try:
                    base_dct = {
                        "enterprise_id": references.enterprise_id,
                        "entity_id": temporary_entity.id,
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
                        shareholders_list.append(temp_dct)

                except Exception as e:
                    pop_logger.log.error((
                        f"PartIntHeld for {references.enterprise_id}. "
                        f"Error: {e}"
                        ))
                    continue

        if filing.shareholders.get("IndividualShareHolders"):
            for natural in filing.shareholders["IndividualShareHolders"]:
                try:
                    temporary_person = Person(natural, country_codes_dct)
                    persons_list.append(temporary_person.description)
                except Exception as e:
                    pop_logger.log.error((
                        f"Indiv Shareholder for {references.enterprise_id}. "
                        f"Error {e}"
                        ))
                    continue

                try:
                    base_dct = {
                        "enterprise_id": references.enterprise_id,
                        "person_id": temporary_person.id,
                        "account_year": year,
                    }
                    for s in natural["RightsHeld"]:
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
                        shareholders_list.append(temp_dct)

                except Exception as e:
                    pop_logger.log.error((
                        f"PartIntHeld for {references.enterprise_id}. "
                        f"Error: {e}"
                        ))
                    continue

        # 2.e Rubrics
        codes = []
        rubrics = []
        try:
            for r in filing.rubrics:
                if r["Period"] == "N":
                    code = str(r.get("Code"))
                    codes.append({"accountcode_id": code, "denomination": code})
                    rubrics.append({
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

        # Step 3: Prepare available statements for execution
        stmts_to_execute = []
        if persons_list:
            stmt_natural = insert(
                table_natural_persons
                ).values(
                    persons_list
                    )
            stmt_natural = stmt_natural.on_conflict_do_update(
                index_elements=[
                    "first_name", "last_name", "street", "street_number"
                ],
                set_={"person_id": stmt_natural.excluded.person_id}
                )
            stmts_to_execute.append(stmt_natural)

        if entities_list:
            stmt_entities = insert(
                table_entities
                ).values(
                    entities_list
                    )
            stmt_entities = stmt_entities.on_conflict_do_update(
                index_elements=[
                    "entity_id", "country_code"
                ],
                set_={"identifier": stmt_entities.excluded.identifier}
            )
            stmts_to_execute.append(stmt_entities)

        if part_int_list:
            stmt_part_int = insert(
                table_part_int
                ).values(
                    part_int_list
                    ).on_conflict_do_nothing()
            stmts_to_execute.append(stmt_part_int)

        if legal_persons_list:
            stmt_legal = insert(
                table_legal_persons
                ).values(
                    legal_persons_list
                    ).on_conflict_do_nothing()
            stmts_to_execute.append(stmt_legal)

        if shareholders_list:
            stmt_share = insert(
                table_shareholders
                ).values(
                    shareholders_list
                    ).on_conflict_do_nothing()
            stmts_to_execute.append(stmt_share)

        stmt_codes = insert(
            table_accounting_codes
            ).values(
                codes
                ).on_conflict_do_nothing()

        stmt_rubrics = insert(
            table_facts
            ).values(
                rubrics
                ).on_conflict_do_nothing()

        with nbb.engine.begin() as conn:
            for stmt in stmts_to_execute:
                conn.execute(stmt)
            conn.execute(stmt_codes)
            conn.execute(stmt_rubrics)

pop_logger.log.info(f"Finished in {(time.time_ns() - start)/1_000_000} ms.")
