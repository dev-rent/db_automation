import os
import uuid
from datetime import datetime, timedelta

from sqlalchemy import URL, create_engine
from dotenv import load_dotenv


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
    """
    def __init__(self, data: list):
        self.enterprise_id = data[0]["EnterpriseNumber"]
        self.enterprise_name = data[0].get("EnterpriseName")
        self.legal_situation = data[0].get("LegalSituation")
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
            "last_update": datetime.today()
            } for d in data]
        self.initial_list = []
        self.correction_list = []
        for ref_dict in self.references:
            if ref_dict["deposit_type"] == "Initial":
                self.initial_list.append(ref_dict)
            elif ref_dict["deposit_type"] == "Correction":
                self.correction_list.append(ref_dict)


class Filing:
    """Represents a filing correspondending to a company."""
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
    """"""
    def __init__(self, person, country_dict):
        self.id = uuid.uuid4()
        self.description = {
            "person_id": self.id,
            "first_name": person.get("FirstName"),
            "last_name": person.get("LastName"),
            "street": person["Address"].get("Street"),
            "street_number": person["Address"].get("Number"),
            "zipcode": person["Address"].get(
                "City",
                person["Address"].get("OtherPostalCode")
                ).replace("pcd:m", ""),
            "country_code": person["Address"].get(
                "Country",
                country_dict.get(
                    person["Address"].get("OtherCountry", "XX")
                )
            ).replace("cty:m", "")
        }


class Entity:
    """"""
    def __init__(self, entity, country_dict):
        self.id = uuid.uuid4()
        self.description = {
            "identifier": entity["Entity"].get("Identifier"),
            "country_code": entity["Entity"]["Address"].get(
                "Country",
                country_dict.get(
                    entity["Entity"]["Address"].get("OtherCountry", "XX")
                )
            ).replace("cty:m", ""),
            "denomination": entity["Entity"].get("Name"),
            "street": entity["Entity"]["Address"].get("Street"),
            "street_number": entity["Entity"]["Address"].get("Number"),
            "zipcode": entity["Entity"]["Address"].get(
                "City",
                entity["Entity"]["Address"].get("OtherPostalCode")
            ).replace("pcd:m", "")
        }
