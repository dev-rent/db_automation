import os
import re
import uuid
from datetime import datetime, timedelta

from sqlalchemy import URL, create_engine
from dotenv import load_dotenv

from .functions import normalise_string


load_dotenv()


class URLgen_nbb:
    """
    Return URL based on selected params.

    Params:
    - db: 'authentic' requires reference / 'extracts'requires date < today.
    - request: 'ref' or 'accData'.
    - date: max date is date < today. Format: %Y-%m-%d
    - ref_id: either company ID or statement reference.
    """
    def __init__(self, db, request, *, date=None, ref_id=None):
        self.url = self._url_gen(
            db, request, date=date, ref_id=ref_id)

    def _url_gen(self, db, request, date, ref_id):
        base_url = "https://ws.cbso.nbb.be/"
        url_map = {
            "authentic": {
                "ref": "authentic/legalEntity/{}/references",
                "accData": "authentic/deposit/{}/accountingData"
            },
            "extracts": {
                "ref": "extracts/batch/{}/references",
                "accData": "extracts/batch/{}/accountingData"
            }
        }
        url = base_url + url_map[db][request]

        if ref_id and db == 'authentic':
            url = url.format(ref_id)

        elif (datetime.strptime(date, "%Y-%m-%d")
              < datetime.today() - timedelta(days=1)
              and db == 'extracts'):
            url = url.format(date)
        else:
            raise ValueError(
                "Instance was not created correctly. Check param!")
        return url


class NBBConnector:
    """
    """
    def __init__(self, *, isolation="SERIALIZABLE", echo=False):
        self.url_object = URL.create(
            "postgresql",
            username=os.getenv("DB_USERNAME", ""),
            password=os.getenv("DB_PASSWORD", ""),
            host=os.getenv("DB_HOST", ""),
            port=int(os.getenv("DB_PORT", "5432")),
            database=os.getenv("DB_NBB", ""),
            )
        self.engine = create_engine(
            self.url_object,
            isolation_level=isolation,
            echo=echo,
            # pool_pre_ping=True
            )


class References:
    """
    Model dictionary to assemble data from references. The dictionaries under
    references are formatted as the database table.

    Attributes:
        - enterprise_id (str)
        - enterprise_name (str)
        - legal_situation (str)
        - references (all) (list[dict])
        - initial_list (all INITIAL refrences) (list[dict])
        - correction_list (all CORRECTION references) (list[dict])
    """
    def __init__(self, data: list):
        self.enterprise_id = re.sub(r"[^\d]", "", data[-1]["EnterpriseNumber"])
        self.enterprise_name = data[-1].get("EnterpriseName")
        self.legal_situation = data[-1].get("LegalSituation")
        self.references = [{
            "enterprise_id": self.enterprise_id,
            "start_date": datetime.strptime(
                d["ExerciseDates"]["startDate"], "%Y-%m-%d"),
            "end_date": datetime.strptime(
                d["ExerciseDates"]["endDate"], "%Y-%m-%d"),
            "filing_id": d["ReferenceNumber"],
            "account_year": int(d["ExerciseDates"]["endDate"][:4]) + 1,
            "deposit_date": d["DepositDate"],
            "deposit_type": d["DepositType"],
            "legal_form": d["LegalForm"],
            "activity_code": d.get("ActivityCode"),
            "model_type": d["ModelType"],
            "last_update": datetime.now()
            }
            for d in data
            ]

        self.initial_list = []
        self.correction_list = []
        for ref_dict in self.references:
            if ref_dict["deposit_type"] == "Initial":
                self.initial_list.append(ref_dict)
            elif ref_dict["deposit_type"] == "Correction":
                self.correction_list.append(ref_dict)
            else:
                continue


class Filing:
    """
    Represents a filing correspondending to a company.

    Attributes:
        - reference_number (str)
        - enterprise_name (str)
        - address (dict)
        - legal_form (dict)
        - joint_committies (list)
        - rubrics (list[dict])
        - administrators (dict)
        - participating_interests (list[dict])
        - shareholders (dict)
    """
    def __init__(self, data: dict):
        self.reference_number: str | None = data.get("ReferenceNumber")
        self.enterprise_name: str | None = data.get("EnterpriseName")
        self.address: dict = data.get("Address", {})
        self.legal_form: dict = data.get("LegalForm", {})
        self.joint_committees: list = data.get("JointCommittees", [])
        self.rubrics: list[dict] = data.get("Rubrics", [])
        self.administrators: dict = data.get("Administrators", {})
        self.participating_interests: list = data.get(
            "ParticipatingInterests", [])
        self.shareholders: dict = data.get("Shareholders", {})


class Person:
    """
    Represent a (natural) person. The description dictionary is formatted to
    the database table. First_name, last_name and street have to be lowered
    because they will uniquely identify a natural person.

    Attributes:
        - id (UUID)
        - description (dict)

    """
    def __init__(self, person: dict, country_dict: dict):
        self.id = uuid.uuid4()
        self.description = {
            "person_id": self.id,
            "first_name":
                person["FirstName"].lower()
                if person.get("FirstName") else None,
            "last_name":
                person["LastName"].lower()
                if person.get("LastName") else None,
            "street":
                person["Address"]["Street"].lower()
                if person["Address"].get("Street") else None,
            "street_number": person["Address"].get("Number"),
            "zipcode":
                person["Address"].get("City").replace("pcd:m", "")
                if person["Address"].get("City")
                else (person["Address"].get("OtherPostalCode") or "0000"),
            "country_code":
                person["Address"].get("Country").replace("cty:m", "")
                if person["Address"].get("Country")
                else country_dict.get(person["Address"].get("OtherCountry"))
                or "XX",
        }
        self.key = tuple(
            normalise_string(v.lower())
            for k, v in self.description.items()
            if k in {"first_name", "last_name", "street", "street_number"}
            )


class Entity:
    """
    Represent a legal person or entity. The description dictionary is formatted
    to the database table.

    Attributes:
        - id (UUID)
        - description (dict)
    """
    def __init__(self, entity, country_dict):
        self.id = uuid.uuid4()
        self.description = {
            "identifier": self.id,
            "entity_id": re.sub(r"[^\d]", "", entity.get("Identifier")),
            "country_code":
                entity["Address"].get("Country").replace("cty:m", "")
                if entity["Address"].get("Country")
                else country_dict.get(entity["Address"].get("OtherCountry"))
                or "XX",
            "denomination": entity.get("Name"),
            "street":
                entity["Address"]["Street"].lower()
                if entity["Address"].get("Street") else None,
            "street_number": entity["Address"].get("Number"),
            "zipcode":
                entity["Address"].get("City").replace("pcd:m", "")
                if entity["Address"].get("City")
                else entity["Address"].get("OtherPostalCode") or "0000"
        }
        self.key = self.description.get("entity_id")


class CleanedData:
    def __init__(self) -> None:
        self.company_info: dict[str, str] = {}
        self.persons_dict: dict = {}
        self.entities_dict: dict = {}
        self.admin_legal_list: list[dict] = []
        self.admin_nat_list: list[dict] = []
        self.part_interest_list: list[dict] = []
        self.shareholders_list: list = []
        self.accounting_codes: list = []
        self.statements_list: list = []
        self.facts_list: list = []
