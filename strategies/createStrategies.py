from . import Glenns_Paired_Switching_Strategy
from . import Stokens_Active_Combined_Asset_Monthly
from . import Vigilant_Asset_Allocation_Aggressive


def create(path, allTicks):
    st_names = list(allTicks.keys())

    if "GPS" in st_names:
        Glenns_Paired_Switching_Strategy.build_strategey(allTicks["GPS"], path)

    if "SACAM" in st_names:
        Stokens_Active_Combined_Asset_Monthly.build_strategey(
            allTicks["SACAM"], path)

    if "VAAA" in st_names:
        Vigilant_Asset_Allocation_Aggressive.build_strategey(
            allTicks["VAAA"], path)

    # if "TEST" in st_names:
    #     Test_Strategy.build_strategey(allTicks["TEST"], path)
