import luigi

from .extraction_validations.extraction_date_validation import ExtractionDateValidation
from .extraction_validations.non_empty_extraction_validation import NonEmptyExtractionValidation
from .extraction_validations.is_json_validation import IsJsonValidation
from .extraction_validations.inspection_dates_match_request_date_validation import InspectionDatesMatchRequestDateValidation

class ExtractionValidationMetadata(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()

    _complete = False
    
    def run(self):
        yield (
            ExtractionDateValidation(self.year, self.month, self.day),
            NonEmptyExtractionValidation(self.year, self.month, self.day),
            IsJsonValidation(self.year, self.month, self.day),
            InspectionDatesMatchRequestDateValidation(self.year, self.month, self.day)
        )
        self._complete = True

    def complete(self):
        return self._complete