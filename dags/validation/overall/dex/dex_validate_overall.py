from validation.basic_validate.address_format_validate import AddressFormatValidate
from validation.overall.base_validate_overall import BaseValidateOverall


class DexValidateOverall(BaseValidateOverall):
    business_type = 'lending'

    def basic(self):
        return [
            {
                'cls': AddressFormatValidate,
                'config': {
                    'validate_table': 'footprint-etl.{chain}_{business_type}_{project}.{project}_{business_type}_borrow'.format(
                        chain=self.chain, business_type=self.business_type, project=self.project),
                    'validate_field': 'xxx',
                }
            }
        ]

    def inner(self):
        return []

    def outside(self):
        return []
