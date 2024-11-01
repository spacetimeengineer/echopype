"""
Unpack manufacturer-specific data files into an interoperable netCDF or Zarr format.

The current version supports:

- Simrad EK60 echosounder ``.raw`` data
- Simrad EK80 echosounder ``.raw`` data
- ASL Environmental Sciences AZFP echosounder ``.01A`` data
- ASL Environmental Sciences AZFP (USL6) echosounder ``.azfp`` data
"""

# flake8: noqa
from .parse_ad2cp import ParseAd2cp
from .parse_azfp import ParseAZFP
from .parse_azfp6 import ParseAZFP6
from .parse_base import ParseBase
from .parse_ek60 import ParseEK60, is_EK60
from .parse_ek80 import ParseEK80, is_EK80
from .set_groups_ad2cp import SetGroupsAd2cp
from .set_groups_azfp import SetGroupsAZFP
from .set_groups_azfp6 import SetGroupsAZFP6
from .set_groups_ek60 import SetGroupsEK60
from .set_groups_ek80 import SetGroupsEK80
