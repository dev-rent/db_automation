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
    table_accounting_codes, table_administrators_natural,
    table_administrators_legal, table_company_info, table_entities,
    table_facts, table_natural_persons, table_part_int, table_shareholders,
    table_statements
)
from nbb_data.classes import NBBConnector, References, Filing, Person, Entity

x = '1'
start = time.time_ns()

load_dotenv()

pop_logger = ScriptLogger(f"logs/population_{datetime.now()}.log", level=20)
quit = "Quiting script..."
pop_logger.log.info("Initialising log...")

nbb = NBBConnector(echo=False)

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
for file in ref_file_lst[:1]:
    file = "temp_references/0240277017.json"
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

    str_tempfolder = "server{}/temp_filing/{}.json"
    str_tempfolder = "temp_filing/{}.json"

    for tupl in filings_list:
        # Every time a person or entity is captured, it needs to be comitted
        # to the databse immediately within the same block. Both entities can
        # appear in later blocks and need to be ready for update.
        try:
            with open(str_tempfolder.format(tupl[0]), 'r') as data:
                filing = Filing(json.load(data))
        except Exception as e:
            pop_logger.log.error(f"{e}")
            continue

        year = tupl[1]

        # 2.a Natural Persons
        # Do not change because list needs to be empty for every filing.
        administrators_natural_list = []
        natural_persons_list = []
        for natural in filing.administrators["NaturalPersons"]:
            try:
                temporary_person = Person(natural['Person'], country_codes_dct)
                print(temporary_person.key)
                base_dct = {
                    "enterprise_id": references.enterprise_id,
                    "person_id": temporary_person.id,
                    "year": year
                }
                if natural["Mandates"]:
                    for mandate in natural["Mandates"]:
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
                                        "EndDate"
                                    ), "%Y-%m-%d")
                            }
                        except Exception as e:
                            pop_logger.log.error((
                                f"Mandates: {references.enterprise_id}. "
                                f"Error: {e}"
                                ))
                            continue

                        temp_dct.update(add_dct)
                        administrators_natural_list.append(temp_dct)
                else:
                    administrators_natural_list.append(base_dct)
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

        if administrators_natural_list:
            with nbb.engine.begin() as conn:
                stmt_admin_nat = insert(
                    table_administrators_natural
                    ).values(
                        administrators_natural_list
                        )
                stmt_admin_nat = stmt_admin_nat.on_conflict_do_update(
                    index_elements=[
                        "enterprise_id", "person_id"
                    ],
                    set_={
                        "function_code": stmt_admin_nat.excluded.function_code,
                        "start_date": stmt_admin_nat.excluded.start_date,
                        "end_date": stmt_admin_nat.excluded.end_date,
                        "year": stmt_admin_nat.excluded.year

                    }
                )
        del natural_persons_list
        del administrators_natural_list

        # 2.b Legal Persons
        persons_list = []
        entities_list = []
        administrators_legal_list = []
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
                    "person_id": temporary_person.id,
                    "year": year
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
                                        "EndDate"
                                    ), "%Y-%m-%d")
                            }
                        except Exception as e:
                            pop_logger.log.error((
                                f"Mandates: {references.enterprise_id}. "
                                f"Error: {e}"
                                ))
                            continue

                        temp_dct.update(add_dct)
                        administrators_legal_list.append(temp_dct)
                else:
                    administrators_legal_list.append(base_dct)

        if persons_list:
            stmt = insert(table_natural_persons).values(persons_list)
            stmt = stmt.on_conflict_do_update(
                    index_elements=[
                        "first_name", "last_name",
                        "street", "street_number"
                    ],
                    set_={"person_id": stmt.excluded.person_id}
                    )
            with nbb.engine.begin() as conn:
                conn.execute(stmt)
            del stmt

        if entities_list:
            stmt = insert(table_entities).values(entities_list)
            stmt = stmt.on_conflict_do_update(
                    index_elements=[
                        "entity_id", "country_code"
                    ],
                    set_={"identifier": stmt.excluded.identifier}
                    )
            with nbb.engine.begin() as conn:
                conn.execute(stmt)
            del stmt

        if administrators_legal_list:
            stmt_legal = insert(
                table_administrators_legal
                ).values(
                    administrators_legal_list
                    )
            stmt_legal = stmt_legal.on_conflict_do_update(
                index_elements=[
                    "enterprise_id", "person_id"
                ],
                set_={
                    "function_code": stmt_legal.excluded.function_code,
                    "start_date": stmt_legal.excluded.start_date,
                    "end_date": stmt_legal.excluded.end_date,
                    "year": stmt_legal.excluded.year

                }
            )
            with nbb.engine.begin() as conn:
                conn.execute(stmt_legal)
        del entities_list
        del persons_list

        # 2.c Participating Interests
        part_int_list = []
        entities_list = []
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

        if entities_list:
            stmt = insert(table_entities).values(entities_list)
            stmt = stmt.on_conflict_do_update(
                    index_elements=[
                        "entity_id", "country_code"
                    ],
                    set_={"identifier": stmt.excluded.identifier}
                    )
            with nbb.engine.begin() as conn:
                conn.execute(stmt)
            del stmt

        if part_int_list:
            stmt_part_int = insert(
                table_part_int
                ).values(
                    part_int_list
                    ).on_conflict_do_nothing()
            with nbb.engine.begin() as conn:
                conn.execute(stmt_part_int)
        del entities_list

        # 2.d Shareholders
        shareholders_list = []
        entities_list = []
        persons_list = []
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

            if entities_list:
                with nbb.engine.begin() as conn:
                    stmt = insert(table_entities).values(entities_list)
                    stmt = stmt.on_conflict_do_update(
                            index_elements=[
                                "entity_id", "country_code"
                            ],
                            set_={"identifier": stmt.excluded.identifier}
                            )
                    conn.execute(stmt)
                del stmt
            del entities_list

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

            if persons_list:
                stmt = insert(table_natural_persons).values(persons_list)
                stmt = stmt.on_conflict_do_update(
                        index_elements=[
                            "first_name", "last_name",
                            "street", "street_number"
                        ],
                        set_={"person_id": stmt.excluded.person_id}
                        )
                with nbb.engine.begin() as conn:
                    conn.execute(stmt)
                del stmt
            del persons_list

        if shareholders_list:
            stmt_share = insert(
                table_shareholders
                ).values(
                    shareholders_list
                    ).on_conflict_do_nothing()
            with nbb.engine.begin() as conn:
                conn.execute(stmt_share)

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

        try:
            with nbb.engine.begin() as conn:
                conn.execute(stmt_codes)
                conn.execute(stmt_rubrics)
        except Exception as e:
            pop_logger.log.error((
                f"Failed inserting data for {references.enterprise_id} - "
                f"{tupl[0]}. Error {e}"
            ))
            continue

pop_logger.log.info(f"Finished in {(time.time_ns() - start)/1_000_000} ms.")
