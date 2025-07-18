# %% [markdown]
#
# This is an MWE for reproducing an issue with the `eval` method in `Traffic`.
# See #497: https://github.com/xoolive/traffic/issues/497
#
# %%
import logging
from typing import Any

from traffic.core import Flight, Traffic
from traffic.data import airports
from traffic.data.samples import zurich_airport as sample

logging.basicConfig(level=logging.DEBUG)


# %%
class TestFilter:
    airport = airports["ZRH"]  # doe not trigger the issue, but is fixed to ZRH

    def __init__(self, airport_code: str) -> None:
        # self.airport = airports[airport_code]  # does trigger the issue
        pass

    def on_ils(self, flight: Flight) -> bool:
        # does not matter what's happening here.
        return flight.aligned_on_ils(self.airport).next() is not None

    def filter(self, traffic: Traffic) -> Any:
        # does not matter if a class member or outside function
        return traffic.pipe(self.on_ils).eval(
            desc="filtering...",
            max_workers=2,  # does not trigger if < 2
        )


filter = TestFilter("ZRH")
filter.filter(sample)

# %%
