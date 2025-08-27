from sqlalchemy import (
    MetaData, Table, Column, String, Integer, Float, Uuid, Date)

metadata = MetaData()

table_accounting_codes = Table(
    "accounting_codes", metadata,
    Column("accountcode_id", String, primary_key=True),
    Column("denomination", String)
    )

table_administrators_natural = Table(
    "administrators_natural", metadata,
    Column("enterprise_id", String),
    Column("person_id", String),
    Column("function_code", String),
    Column("start_date", Date),
    Column("end_date", Date),
    Column("year", Integer)
    )

table_administrators_legal = Table(
    "administrators_legal", metadata,
    Column("enterprise_id", String),
    Column("entity_id", Uuid),
    Column("person_id", Uuid),
    Column("function_code", String),
    Column("start_date", Date),
    Column("end_date", Date),
    Column("year", Integer)
    )

table_company_info = Table(
    "company_info", metadata,
    Column("enterprise_id", String, primary_key=True),
    Column("denomination", String),
    Column("legal_situation", String)
    )

table_country_codes = Table(
    "country_codes", metadata,
    Column("english_name", String),
    Column("dutch_name", String),
    Column("a_2", String),
    Column("a_3", String),
    Column("numeric_code", String),
    Column("iso_3166_2", String)
    )

table_entities = Table(
    "entities", metadata,
    Column("identifier", Uuid, primary_key=True),
    Column("entity_id", String),
    Column("country_code", String),
    Column("denomination", String),
    Column("street", String),
    Column("street_number", String),
    Column("zipcode", String),
    )


table_natural_persons = Table(
    "natural_persons", metadata,
    Column("person_id", Uuid),
    Column("first_name", String),
    Column("last_name", String),
    Column("street", String),
    Column("street_number", String),
    Column("zipcode", String),
    Column("country_code", String)
    )

table_part_int = Table(
    "participating_interests", metadata,
    Column("enterprise_id", String),
    Column("entity_id", String),
    Column("account_year", Integer),
    Column("account_date", Date),
    Column("currency", String),
    Column("equity", Integer),
    Column("net_result", Integer),
    Column("nature", String),
    Column("line", String),
    Column("amount", String),
    Column("percentage_held", Float),
    Column("percentage_subsidiary", Float)
    )

table_shareholders = Table(
    "shareholders", metadata,
    Column("enterprise_id", String),
    Column("entity_id", Uuid),
    Column("person_id", Uuid),
    Column("account_year", Integer),
    Column("denomination", String),
    Column("nature_rights", String),
    Column("line_rights", String),
    Column("securities_attached", Integer),
    Column("not_securities_attached", String),
    Column("percentage", Float)
    )

table_facts = Table(
    "statement_facts", metadata,
    Column("account_year", String),
    Column("filing_id", String),
    Column("accountcode_id", String),
    Column("book_value", Float),
    )

table_statements = Table(
    "statements", metadata,
    Column("enterprise_id", String),
    Column("start_date", Date),
    Column("end_date", Date),
    Column("filing_id", String),
    Column("account_year", Integer),
    Column("deposit_date", Date),
    Column("deposit_type", String),
    Column("legal_form", String),
    Column("activity_code", String),
    Column("model_type", String),
    Column("last_update", Date),
    )
