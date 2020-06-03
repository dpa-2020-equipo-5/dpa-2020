from sodapy import Socrata
from nyc_ccci_etl.commons.configuration import get_app_token
from nyc_ccci_etl.utils.is_date_valid import is_date_valid
class InspectionsExtractor:
    def __init__(self, year, month, day):
        if is_date_valid(year, month, day) == False:
            raise ValueError
        self.date_param = "{}-{}-{}T00:00:00.000".format(year, month, day)
        self.socrataClient = Socrata("data.cityofnewyork.us", get_app_token()) 

    def execute(self):
        return self.socrataClient.get("dsg6-ifza", inspectiondate=self.date_param)
