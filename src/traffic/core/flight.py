from __future__ import annotations

import ast
import logging
import re
import warnings
from datetime import datetime, timedelta, timezone
from functools import lru_cache, reduce
from itertools import combinations
from operator import attrgetter
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    Set,
    Tuple,
    TypedDict,
    Union,
    cast,
    overload,
)

import rich.repr
import rs1090
from impunity import impunity
from pitot import geodesy as geo
from rich.console import Console, ConsoleOptions, RenderResult
from typing_extensions import Self

import numpy as np
import pandas as pd
import pyproj
from pandas.core.internals.blocks import DatetimeTZBlock
from shapely.geometry import LineString, MultiPoint, Point, Polygon, base
from shapely.ops import transform

from ..algorithms import filters
from ..algorithms.douglas_peucker import douglas_peucker
from ..core import types as tt
from ..core.structure import Airport
from .intervals import Interval, IntervalCollection
from .iterator import FlightIterator, flight_iterator
from .mixins import (
    GeographyMixin,
    HBoxMixin,
    PointLike,
    PointMixin,
    ShapelyMixin,
)
from .time import deltalike, time_or_delta, timelike, to_datetime, to_timedelta

if TYPE_CHECKING:
    import altair as alt
    import plotly.graph_objects as go
    from cartopy import crs
    from cartopy.mpl.geoaxes import GeoAxes
    from ipyleaflet import Map as LeafletMap
    from ipyleaflet import Polyline as LeafletPolyline
    from matplotlib.artist import Artist
    from matplotlib.axes import Axes

    from ..algorithms.metadata import airports, flightplan
    from ..algorithms.navigation import (
        ApplyBase,
        ApplyIteratorBase,
        ApplyOptionalBase,
    )
    from ..algorithms.performance import EstimatorBase
    from ..algorithms.prediction import PredictBase
    from ..data.adsb.decode import RawData
    from ..data.basic.aircraft import Tail
    from ..data.basic.navaid import Navaids
    from .airspace import Airspace
    from .lazy import LazyTraffic
    from .structure import Navaid
    from .traffic import Traffic

_log = logging.getLogger(__name__)


class Entry(TypedDict, total=False):
    timestamp: pd.Timestamp
    timedelta: pd.Timedelta
    longitude: float
    latitude: float
    altitude: float
    name: str


def _tz_interpolate(
    data: DatetimeTZBlock, *args: Any, **kwargs: Any
) -> DatetimeTZBlock:
    coerced = data.coerce_to_target_dtype("int64")
    interpolated, *_ = coerced.interpolate(*args, **kwargs)
    return interpolated


DatetimeTZBlock.interpolate = _tz_interpolate


def _split(
    data: pd.DataFrame, value: Union[str, int], unit: Optional[str]
) -> Iterator[pd.DataFrame]:
    # This method helps splitting a flight into several.
    if data.shape[0] < 2:
        return
    diff = data.timestamp.diff()
    if unit is None:
        delta = pd.Timedelta(value)
    else:
        delta = pd.Timedelta(np.timedelta64(value, unit))  # type: ignore
    # There seems to be a change with numpy >= 1.18
    # max() now may return NaN, therefore the following fix
    max_ = diff.max()
    if max_ > delta:
        # np.nanargmax seems bugged with timestamps
        argmax = diff.argmax()
        yield from _split(data.iloc[:argmax], value, unit)
        yield from _split(data.iloc[argmax:], value, unit)
    else:
        yield data


default_angle_features = ["track", "heading"]


class Position(PointMixin, pd.core.series.Series):  # type: ignore
    def plot(
        self,
        ax: "Axes",
        text_kw: Optional[Mapping[str, Any]] = None,
        shift: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> List["Artist"]:  # coverage: ignore
        from ..visualize.markers import aircraft as aircraft_marker
        from ..visualize.markers import rotate_marker

        visualdict: dict[str, Any] = dict(s=300)
        if hasattr(self, "track"):
            visualdict["marker"] = rotate_marker(aircraft_marker, self.track)

        if text_kw is None:
            text_kw = dict()
        else:
            # since we may modify it, let's make a copy
            text_kw = {**text_kw}

        if "s" not in text_kw and hasattr(self, "callsign"):
            text_kw["s"] = self.callsign

        return super().plot(ax, text_kw, shift, **{**visualdict, **kwargs})


class MetaFlight(type):
    def __getattr__(cls, name: str) -> Callable[..., Any]:
        # if the string is callable, apply this on a flight
        # parsing the AST is a much safer option than raw eval()
        for node in ast.walk(ast.parse(name)):
            if isinstance(node, ast.Call):
                func_name = node.func.id  # type: ignore
                args = [ast.literal_eval(arg) for arg in node.args]
                kwargs = dict(
                    (keyword.arg, ast.literal_eval(keyword.value))
                    for keyword in node.keywords
                )
                return lambda flight: getattr(Flight, func_name)(
                    flight, *args, **kwargs
                )

        raise AttributeError


@rich.repr.auto()
class Flight(HBoxMixin, GeographyMixin, ShapelyMixin, metaclass=MetaFlight):
    """Flight is the most basic class associated to a trajectory.
    Flights are the building block of all processing methods, built on top of
    pandas DataFrame. The minimum set of required features are:

    - ``icao24``: the ICAO transponder ID of an aircraft;
    - ``callsign``: an identifier which may be associated with the
      registration of an aircraft, with its mission (VOR calibration,
      firefighting) or with a route (for a commercial aircraft);
    - ``timestamp``: timezone aware timestamps are preferable.
      Some methods may work with timezone naive timestamps but the behaviour
      is not guaranteed;
    - ``latitude``, ``longitude``: in degrees, WGS84 (EPSG:4326);
    - ``altitude``: in feet.

    .. note::

        The ``flight_id`` (identifier for a trajectory) may be used in place of
        a pair of (``icao24``, ``callsign``). More features may also be provided
        for further processing, e.g. ``groundspeed``, ``vertical_rate``,
        ``track``, ``heading``, ``IAS`` (indicated airspeed) or ``squawk``.

    .. tip::

        Read more about:

        - :ref:`arithmetic of trajectories
          <How to use arithmetic operators on trajectories?>`
        - :ref:`navigation specific methods <Navigation events>`

        - :ref:`sample flights <How to access sample trajectories?>` provided
          for testing purposes in the module ``traffic.data.samples``

    **Abridged contents:**

        - properties:
          :meth:`callsign`,
          :meth:`flight_id`,
          :meth:`icao24`,
          :meth:`number`,
          :meth:`start`,
          :meth:`stop`,

        - time related methods:
          :meth:`after`,
          :meth:`at`,
          :meth:`at_ratio`,
          :meth:`before`,
          :meth:`between`,
          :meth:`first`,
          :meth:`last`,
          :meth:`skip`,
          :meth:`shorten`

        - geometry related methods:
          :meth:`airborne`,
          :meth:`clip`,
          :meth:`compute_wind`,
          :meth:`compute_xy`,
          :meth:`distance`,
          :meth:`inside_bbox`,
          :meth:`intersects`,
          :meth:`project_shape`,
          :meth:`unwrap`

        - filtering and resampling methods:
          :meth:`filter`,
          :meth:`resample`,
          :meth:`simplify`,

        - navigation related events:
          :meth:`aligned`,
          :meth:`landing`,
          :meth:`takeoff`,
          :meth:`holding_pattern`,
          :meth:`point_merge`,
          :meth:`go_around`

        - airborne events:
          :meth:`emergency`,
          :meth:`phases`,
          :meth:`thermals`

        - ground trajectory methods:
          :meth:`aligned`,
          :meth:`movement`,
          :meth:`parking_position`,
          :meth:`pushback`

        - performance estimation methods:
          :meth:`fuelflow`,
          :meth:`emission`

        - metadata inference methods:
          :meth:`infer_airport`,
          :meth:`infer_flightplan`,

        - prediction methods:
          :meth:`predict`

        - visualisation with altair:
          :meth:`chart`,
          :meth:`geoencode`

        - visualisation with leaflet:
          :meth:`map_leaflet`

        - visualisation with plotly:
          :meth:`line_map` and others

        - visualisation with Matplotlib:
          :meth:`plot`,
          :meth:`plot_time`


    """

    __slots__ = ("data",)

    # --- Special methods ---

    def __add__(self, other: Literal[0] | Flight | "Traffic") -> "Traffic":
        """Concatenation operator.

        :param other: is the other Flight or Traffic.

        :return: The sum of two Flights returns a Traffic collection.
            Summing a Flight with 0 returns a Traffic collection with only one
            trajectory, for compatibility reasons with the sum() builtin.

        """
        # keep import here to avoid recursion
        from .traffic import Traffic

        if other == 0:
            # useful for compatibility with sum() function
            return Traffic(self.data)

        # This just cannot return None in this case.
        return Traffic.from_flights([self, other])  # type: ignore

    def __radd__(
        self, other: Union[Literal[0], Flight, "Traffic"]
    ) -> "Traffic":
        """
        As Traffic is thought as a collection of Flights, the sum of two Flight
        objects returns a Traffic object
        """
        return self + other

    @flight_iterator
    def __sub__(
        self, other: Flight | FlightIterator | Interval | IntervalCollection
    ) -> Iterator["Flight"]:
        """Difference operator.

        :param other: refers to anything having one or several start and end
            (a.k.a. stop) dates.

        :return: After intervals are pruned from a trajectory, unconnected
            segments may remain. You should iterate on the result of the ``-``
            operator.
        """
        right: Interval | IntervalCollection
        left = Interval(self.start, self.stop)

        if isinstance(other, (Interval, IntervalCollection)):
            right = other
        elif isinstance(other, Flight):
            right = Interval(other.start, other.stop)
        elif isinstance(other, FlightIterator):
            intervals = [
                Interval(segment.start, segment.stop) for segment in other
            ]
            if len(intervals) == 0:
                yield self
                return
            right = IntervalCollection(intervals)
        else:
            return NotImplemented

        difference = left - right
        if difference is None:
            return None
        yield from self[difference]

    def __and__(self, other: Flight) -> None | Flight:
        """Overlapping of trajectories.

        :param other:

        :return: the segment of trajectory that overlaps ``other``, if any.
        """
        left = Interval(self.start, self.stop)
        right = Interval(other.start, other.stop)
        concurrency = left & right
        if concurrency is None:
            return None
        return self[concurrency]

    def __len__(self) -> int:
        """Number of samples associated to a trajectory.

        The basic behaviour is to return the number of lines in the underlying
        DataFrame. However in some cases, as positions may be wrongly repeated
        in some database systems (e.g. OpenSky Impala shell), we take the
        `last_position` field into account for counting the number of unique
        detected positions.

        Note that when an aircraft is onground, `last_position` is a more
        relevant criterion than (`latitude`, `longitude`) since a grounded
        aircraft may be repeatedly emitting the same position.
        """

        if "last_position" in self.data.columns:
            data = self.data.drop_duplicates("last_position")
            return data.shape[0]  # type: ignore
        else:
            return self.data.shape[0]  # type: ignore

    @property
    def count(self) -> int:
        return len(self)

    def _info_html(self) -> str:
        title = "<h4><b>Flight</b>"
        if self.flight_id:
            title += f" {self.flight_id}"
        title += "</h4>"

        aircraft_fmt = "<code>%icao24</code> · %flag %registration (%typecode)"

        title += "<ul>"
        if self.callsign is not None:
            title += f"<li><b>callsign:</b> {self.callsign} {self.trip}</li>"
        if self.aircraft is not None:
            title += "<li><b>aircraft:</b> {aircraft}</li>".format(
                aircraft=format(self.aircraft, aircraft_fmt)
            )
        else:
            title += f"<li><b>aircraft:</b> <code>{self.icao24}</code></li>"
        title += f"<li><b>start:</b> {self.start}</li>"
        title += f"<li><b>stop:</b> {self.stop}</li>"
        title += f"<li><b>duration:</b> {self.duration}</li>"

        sampling_rate = self.data.timestamp.diff().mean().total_seconds()
        title += f"<li><b>sampling rate:</b> {sampling_rate:.0f} second(s)</li>"

        title += "</ul>"
        return title

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:
        aircraft_fmt = "%icao24 · %flag %registration (%typecode)"

        yield f"[bold blue]Flight {self.flight_id if self.flight_id else ''}"

        if self.callsign is not None:
            yield f"  - [b]callsign:[/b] {self.callsign} {self.trip}"
        if self.aircraft is not None:
            yield "  - [b]aircraft:[/b] {aircraft}".format(
                aircraft=format(self.aircraft, aircraft_fmt)
            )
        else:
            yield f"  - [b]aircraft:[/b] {self.icao24}"

        yield f"  - [b]start:[/b] {self.start:%Y-%m-%d %H:%M:%S}Z "
        yield f"  - [b]stop:[/b] {self.stop:%Y-%m-%d %H:%M:%S}Z"
        yield f"  - [b]duration:[/b] {self.duration}"

        sampling_rate = self.data.timestamp.diff().mean().total_seconds()
        yield f"  - [b]sampling rate:[/b] {sampling_rate:.0f} second(s)"

        features = set(self.data.columns) - {
            "start",
            "stop",
            "icao24",
            "callsign",
            "flight_id",
            "destination",
            "origin",
            "track_unwrapped",
            "heading_unwrapped",
        }
        yield "  - [b]features:[/b]"
        for feat in sorted(features):
            yield f"    o {feat}, [i]{self.data[feat].dtype}"

    def _repr_html_(self) -> str:
        title = self._info_html()
        no_wrap_div = '<div style="white-space: nowrap">{}</div>'
        return title + no_wrap_div.format(self._repr_svg_())

    @lru_cache()
    def _repr_svg_(self) -> Optional[str]:
        # even 25m should be enough to limit the size of resulting notebooks!
        if self.shape is None:
            return None

        if len(self.shape.coords) < 1000:
            return super()._repr_svg_()

        return super(Flight, self.resample("1s").simplify(25))._repr_svg_()

    def __rich_repr__(self) -> rich.repr.Result:
        if self.flight_id:
            yield self.flight_id
        yield "icao24", self.icao24
        if self.callsign:
            yield "callsign", self.callsign

    @property
    def __geo_interface__(self) -> Dict[str, Any]:
        if self.shape is None:
            # Returns an empty geometry
            return {"type": "GeometryCollection", "geometries": []}
        return self.shape.__geo_interface__  # type: ignore

    def keys(self) -> list[str]:
        # This is for allowing dict(Flight)
        keys = ["icao24", "aircraft", "start", "stop", "duration"]
        if self.callsign:
            keys = ["callsign", *keys]
        if self.flight_id:
            keys = ["flight_id", *keys]
        if self.origin:
            keys.append("origin")
        if self.destination:
            keys.append("destination")
        if self.diverted:
            keys.append("diverted")
        return keys

    @overload
    def __getitem__(self, key: Interval) -> Flight: ...

    @overload
    def __getitem__(self, key: IntervalCollection) -> FlightIterator: ...

    def __getitem__(self, key: str | Interval | IntervalCollection) -> Any:
        """Indexation of flights.

        :param key: the key parameter passed in the brackets

        :return:
            - if key is a string, the bracket operator is equivalent to the
              dot notation\\
              (e.g. ``flight["duration"]`` is equivalent to ``flight.duration``)
            - if key is an Interval, the bracket operator is equivalent to the
              :meth:`between` method
            - if key is an IntervalCollection, the operator iterates on all the
              intervals provided
        """
        if isinstance(key, Interval):
            return self.between(key.start, key.stop)

        if isinstance(key, IntervalCollection):

            @flight_iterator
            def yield_segments() -> Iterator["Flight"]:
                for interval in key:
                    segment = self.between(
                        interval.start, interval.stop, strict=False
                    )
                    if segment is not None:
                        yield segment

            return yield_segments()

        if isinstance(key, str) and key in self.keys():
            return getattr(self, key)

        raise NotImplementedError()

    def __getattr__(self, name: str) -> Any:
        """Helper to facilitate method chaining without lambda.

        Example usage:

        flight.altitude_max
            => flight.max('altitude')
        flight.vertical_rate_std
            => flight.std('vertical_rate')

        Flight.feature_gt("altitude_max", 10000)
            => lambda f: f.max('altitude') > 10000
        """
        msg = f"'{self.__class__.__name__}' has no attribute '{name}'"
        if "_" not in name:
            raise AttributeError(msg)
        *name_split, agg = name.split("_")
        feature = "_".join(name_split)
        if feature not in self.data.columns:
            raise AttributeError(msg)
        value = getattr(self.data[feature], agg)()
        if isinstance(value, np.float64):
            value = float(value)
        return value

    def pipe(
        self,
        func: str | Callable[..., None | Flight | bool],
        *args: Any,
        **kwargs: Any,
    ) -> None | Flight | bool:
        """
        Applies `func` to the object.

        .. warning::

            The logic is similar to that of :meth:`~pandas.DataFrame.pipe`
            method, but the function applies on T, not on the DataFrame.

        """

        if isinstance(func, str):
            func = eval(func)
            assert callable(func)

        return func(self, *args, **kwargs)

    def filter_if(self, test: Callable[[Flight], bool]) -> Optional[Flight]:
        _log.warning("Use Flight.pipe(...) instead", DeprecationWarning)
        return self if test(self) else None

    def has(
        self, method: Union[str, Callable[[Flight], Iterator[Flight]]]
    ) -> bool:
        """Returns True if flight.method() returns a non-empty iterator.

        Example usage:

        >>> flight.has("go_around")
        >>> flight.has("runway_change")
        >>> flight.has(lambda f: f.aligned_on_ils("LFBO"))
        """
        return self.next(method) is not None

    def sum(
        self, method: Union[str, Callable[[Flight], Iterator[Flight]]]
    ) -> int:
        """Returns the number of segments returned by flight.method().

        Example usage:

        >>> flight.sum("go_around")
        >>> flight.sum("runway_change")
        >>> flight.sum(lambda f: f.aligned_on_ils("LFBO"))
        """
        fun = (
            getattr(self.__class__, method)
            if isinstance(method, str)
            else method
        )
        return sum(1 for _ in fun(self))

    def label(
        self,
        method: Union[str, Callable[[Flight], Iterator[Flight]]],
        **kwargs: Any,
    ) -> Flight:
        """Returns the same flight with extra information from iterators.

        Every keyword argument will be used to create a new column in the Flight
        dataframe, filled by default with None values:

        - if the passed value is True, the default one is False
        - if the passed value is a string:
            - "{i}" will be replaced by the index of the segment;
            - "{segment}" will be replaced by the current piece of trajectory;
            - "{self}" will be replaced by the current flight instance

        If a function is passed it will be evaluated with:

        - the current piece of trajectory (segment) if the function or the
          lambda has one argument;
        - the index and the segment if the function or the lambda has two
          arguments;
        - the index, the segment and the flight if the function or the lamdba
          has three arguments;

        Before returning, the dataframe applies
        :meth:`~pandas.DataFrame.convert_dtypes` and so `None` values maybe
        replaced by `NaT` or `NaN` values.

        Example usage:

        - Add a column `holding` which is True when the trajectory follows a
          holding pattern

            .. code:: python

                flight.label(holding_pattern, holding=True)

        - Add a column `index` to enumerate holding patterns:

            .. code:: python

                flight.label(holding_pattern, index="{i}")

        - More complicated enriching:

            .. code:: python

                flight.label(
                    "aligned_on_ils('LSZH')",
                    aligned=True,
                    label="{self.flight_id}_{i}",
                    index=lambda i, segment: i,
                    start=lambda segment: segment.start,
                    altitude_max=lambda segment: segment.altitude_max
                )

        """

        fun = (
            getattr(self.__class__, method)
            if isinstance(method, str)
            else method
        )

        result = self.assign(
            **dict(
                (key, False if value is True else pd.NA)
                for key, value in kwargs.items()
            )
        )

        for i, segment in enumerate(fun(self)):
            mask = result.data.timestamp >= segment.start
            mask &= result.data.timestamp <= segment.stop
            for key, value in kwargs.items():
                if isinstance(value, str):
                    if re.match("^lambda", value):
                        code = ast.parse(value)
                        if any(
                            isinstance(piece, ast.Lambda)
                            for piece in ast.walk(code)
                        ):
                            value = eval(value)
                if callable(value):
                    if value.__code__.co_argcount == 1:
                        value = value(segment)
                    elif value.__code__.co_argcount == 2:
                        value = value(i, segment)
                    elif value.__code__.co_argcount == 3:
                        value = value(i, segment, self)
                if isinstance(value, str):
                    value = value.format(i=i, segment=segment, self=self)
                result.data.loc[mask, key] = value

        return result

    def all(
        self,
        method: Union[str, Callable[[Flight], Iterator[Flight]]],
        flight_id: None | str = None,
    ) -> Optional[Flight]:
        """Returns the concatenation of segments returned by flight.method().

        Example usage:

        >>> flight.all("go_around")
        >>> flight.all("runway_change")
        >>> flight.all('aligned_on_ils("LFBO")')
        >>> flight.all(lambda f: f.aligned_on_ils("LFBO"))
        """
        fun = (
            getattr(self.__class__, method)
            if isinstance(method, str)
            else method
        )
        if flight_id is None:
            t = sum(
                flight.assign(index_=i) for i, flight in enumerate(fun(self))
            )
        else:
            t = sum(
                flight.assign(flight_id=flight_id.format(self=flight, i=i))
                for i, flight in enumerate(fun(self))
            )
        if t == 0:
            return None
        return Flight(t.data)

    def next(
        self,
        method: Union[str, Callable[[Flight], Iterator[Flight]]],
    ) -> Optional[Flight]:
        """
        Returns the first segment of trajectory yielded by flight.method()

        >>> flight.next("go_around")
        >>> flight.next("runway_change")
        >>> flight.next(lambda f: f.aligned_on_ils("LFBO"))
        """
        fun = (
            getattr(self.__class__, method)
            if isinstance(method, str)
            else method
        )
        return next(fun(self), None)

    def final(
        self,
        method: Union[str, Callable[[Flight], Iterator[Flight]]],
    ) -> Optional[Flight]:
        """
        Returns the final (last) segment of trajectory yielded by
        flight.method()

        >>> flight.final("go_around")
        >>> flight.final("runway_change")
        >>> flight.final(lambda f: f.aligned_on_ils("LFBO"))
        """
        fun: Callable[[Flight], Iterator[Flight]] = (
            getattr(self.__class__, method)
            if isinstance(method, str)
            else method
        )
        segment = None
        for segment in fun(self):
            continue
        return segment

    # --- Iterators ---

    @property
    def timestamp(self) -> Iterator[pd.Timestamp]:
        yield from self.data.timestamp

    @property
    def coords(self) -> Iterator[Tuple[float, float, float]]:
        data = self.data.query("longitude.notnull()")
        if "altitude" not in data.columns:
            data = data.assign(altitude=0)
        yield from zip(
            data["longitude"].to_numpy(),
            data["latitude"].to_numpy(),
            # This is a bit more robust to the new dtypes with pd.NA
            data["altitude"].astype(float).to_numpy(),
        )

    def coords4d(self, delta_t: bool = False) -> Iterator[Entry]:
        data = self.data.query("longitude.notnull()")
        if delta_t:
            time = (data.timestamp - data.timestamp.min()).dt.total_seconds()
        else:
            time = data["timestamp"]

        for t, longitude, latitude, altitude in zip(
            time, data["longitude"], data["latitude"], data["altitude"]
        ):
            if delta_t:
                yield {
                    "timedelta": t,
                    "longitude": longitude,
                    "latitude": latitude,
                    "altitude": altitude,
                }
            else:
                yield {
                    "timestamp": t,
                    "longitude": longitude,
                    "latitude": latitude,
                    "altitude": altitude,
                }

    @property
    def xy_time(self) -> Iterator[Tuple[float, float, float]]:
        self_filtered = self.query("longitude.notnull()")
        if self_filtered is None:
            return None
        iterator = iter(zip(self_filtered.coords, self_filtered.timestamp))
        while True:
            next_ = next(iterator, None)
            if next_ is None:
                return
            coords, time = next_
            yield (coords[0], coords[1], time.to_pydatetime().timestamp())

    # --- Properties (and alike) ---

    def min(self, feature: str) -> Any:
        """Returns the minimum value of given feature.

        >>> flight.min('altitude')  # dummy example
        24000
        """
        return self.data[feature].min()

    def max(self, feature: str) -> Any:
        """Returns the maximum value of given feature.

        >>> flight.max('altitude')  # dummy example
        35000
        """
        return self.data[feature].max()

    def mean(self, feature: str) -> Any:
        """Returns the average value of given feature.

        >>> flight.mean('vertical_rate')  # dummy example
        -1000
        """
        return self.data[feature].mean()

    def feature_gt(
        self,
        feature: Union[str, Callable[[Flight], Any]],
        value: Any,
        strict: bool = True,
    ) -> bool:
        """Returns True if feature(flight) is greater than value.

        This is fully equivalent to `f.longer_than("1 minute")`:

        >>> f.feature_gt("duration", pd.Timedelta('1 minute'))
        True

        This is equivalent to `f.max('altitude') > 35000`:

        >>> f.feature_gt(lambda f: f.max("altitude"), 35000)
        True

        The second one can be useful for stacking operations during
        lazy evaluation.
        """
        if isinstance(feature, str):
            feature = attrgetter(feature)
        attribute = feature(self)
        if pd.isna(attribute):
            return False
        if strict:
            return attribute > value  # type: ignore
        return attribute >= value  # type: ignore

    def feature_lt(
        self,
        feature: Union[str, Callable[[Flight], Any]],
        value: Any,
        strict: bool = True,
    ) -> bool:
        """Returns True if feature(flight) is less than value.

        This is fully equivalent to `f.shorter_than("1 minute")`:

        >>> f.feature_lt("duration", pd.Timedelta('1 minute'))
        True

        This is equivalent to `f.max('altitude') < 35000`:

        >>> f.feature_lt(lambda f: f.max("altitude"), 35000)
        True

        The second one can be useful for stacking operations during
        lazy evaluation.
        """
        if isinstance(feature, str):
            feature = attrgetter(feature)
        attribute = feature(self)
        if pd.isna(attribute):
            return False
        if strict:
            return attribute < value  # type: ignore
        return attribute <= value  # type: ignore

    def shorter_than(
        self, value: Union[str, timedelta, pd.Timedelta], strict: bool = True
    ) -> bool:
        """Returns True if flight duration is shorter than value."""
        if isinstance(value, str):
            value = pd.Timedelta(value)
        return self.feature_lt(attrgetter("duration"), value, strict)

    def longer_than(
        self, value: Union[str, timedelta, pd.Timedelta], strict: bool = True
    ) -> bool:
        """Returns True if flight duration is longer than value."""
        if isinstance(value, str):
            value = pd.Timedelta(value)
        return self.feature_gt(attrgetter("duration"), value, strict)

    def abs(
        self, features: Union[None, str, List[str]] = None, **kwargs: Any
    ) -> Flight:
        """Assign absolute versions of features to new columns.

        >>> flight.abs("track")

        The two following commands are equivalent:

        >>> flight.abs(["track", "heading"])
        >>> flight.abs(track="track_abs", heading="heading_abs")

        """
        assign_dict = dict()
        if features is None:
            features = []
        if isinstance(features, str):
            features = [features]
        if isinstance(features, Iterable):
            for feature in features:
                assign_dict[feature + "_abs"] = self.data[feature].abs()
        for key, value in kwargs.items():
            assign_dict[value] = self.data[key].abs()
        return self.assign(**assign_dict)

    def diff(
        self,
        features: Union[None, str, List[str]] = None,
        **kwargs: Any,
    ) -> Flight:
        """Assign differential versions of features to new columns.

        >>> flight.diff("track")

        The two following commands are equivalent:

        >>> flight.diff(["track", "heading"])
        >>> flight.diff(track="track_diff", heading="heading_diff")

        """
        assign_dict = dict()
        if features is None:
            features = []
        if isinstance(features, str):
            features = [features]
        if isinstance(features, Iterable):
            for feature in features:
                assign_dict[feature + "_diff"] = self.data[feature].diff()
        for key, value in kwargs.items():
            assign_dict[value] = self.data[key].diff()
        return self.assign(**assign_dict)

    @property
    def start(self) -> pd.Timestamp:
        """Returns the minimum value of timestamp."""
        return self.min("timestamp")

    @property
    def stop(self) -> pd.Timestamp:
        """Returns the maximum value of timestamp."""
        return self.max("timestamp")

    @property
    def duration(self) -> pd.Timedelta:
        """Returns the duration of the flight."""
        return self.stop - self.start

    def _get_unique(
        self, field: str, warn: bool = True
    ) -> Union[str, Set[str], None]:
        if field not in self.data.columns:
            return None
        tmp = self.data[field].unique()
        tmp = list(elt for elt in tmp if not pd.isna(elt))
        if len(tmp) == 0:
            return None
        if len(tmp) == 1:
            return tmp[0]  # type: ignore
        if warn:
            _log.warning(f"Several {field}s for one flight, consider splitting")
        return set(tmp)

    @property
    def callsign(self) -> Union[str, Set[str], None]:
        """Returns the unique callsign value(s) associated to the Flight.

        A callsign is an identifier sent by an aircraft during its flight. It
        may be associated with the registration of an aircraft, its mission or
        with a route for a commercial aircraft.
        """
        callsign = self._get_unique("callsign")
        return callsign

    @property
    def number(self) -> Union[str, Set[str], None]:
        """Returns the unique number value(s) associated to the Flight.

        This field is reserved for the commercial number of the flight, prefixed
        by the two letter code of the airline.
        For instance, AFR292 is the callsign and AF292 is the flight number.

        Callsigns are often more complicated as they are designed to limit
        confusion on the radio: hence DLH02X can be the callsign associated
        to flight number LH1100.
        """
        return self._get_unique("number")

    @property
    def flight_id(self) -> Union[str, Set[str], None]:
        """Returns the unique flight_id value(s) of the DataFrame.

        Neither the icao24 (the aircraft) nor the callsign (the route) is a
        reliable way to identify trajectories. You can either use an external
        source of data to assign flight ids (for example DDR files by
        Eurocontrol, identifiers by FlightRadar24, etc.) or assign a flight_id
        by yourself (see ``Flight.assign_id(name: str)`` method).

        The ``Traffic.assign_id()`` method uses a heuristic based on the
        timestamps associated to callsign/icao24 pairs to automatically assign a
        ``flight_id`` and separate flights.

        """
        return self._get_unique("flight_id")

    @property
    def title(self) -> str:
        title = str(self.callsign)
        number = self.number
        flight_id = self.flight_id

        if number is not None:
            title += f" – {number}"  # noqa: RUF001

        if flight_id is not None:
            title += f" ({flight_id})"

        return title

    @property
    def trip(self) -> str:
        return (
            (
                "("
                if self.origin is not None or self.destination is not None
                else ""
            )
            + (f"{self.origin}" if self.origin else " ")
            + (
                " to "
                if self.origin is not None or self.destination is not None
                else ""
            )
            + (f"{self.destination}" if self.destination else " ")
            + (
                f" diverted to {self.diverted}"
                # it must not be None nor nan
                if self.diverted and self.diverted == self.diverted
                else ""
            )
            + (
                ")"
                if self.origin is not None or self.destination is not None
                else ""
            )
        )

    @property
    def origin(self) -> Union[str, Set[str], None]:
        """Returns the unique origin value(s),
        None if not available in the DataFrame.

        The origin airport is usually represented as a ICAO or a IATA code.

        The ICAO code of an airport is represented by 4 letters (e.g. EHAM for
        Amsterdam Schiphol International Airport) and the IATA code is
        represented by 3 letters and more familiar to the public (e.g. AMS for
        Amsterdam)

        """
        return self._get_unique("origin")

    @property
    def destination(self) -> Union[str, Set[str], None]:
        """Returns the unique destination value(s),
        None if not available in the DataFrame.

        The destination airport is usually represented as a ICAO or a IATA code.

        The ICAO code of an airport is represented by 4 letters (e.g. EHAM for
        Amsterdam Schiphol International Airport) and the IATA code is
        represented by 3 letters and more familiar to the public (e.g. AMS for
        Amsterdam)

        """
        return self._get_unique("destination")

    @property
    def diverted(self) -> Union[str, Set[str], None]:
        """Returns the unique diverted value(s),
        None if not available in the DataFrame.

        The diverted airport is usually represented as a ICAO or a IATA code.

        The ICAO code of an airport is represented by 4 letters (e.g. EHAM for
        Amsterdam Schiphol International Airport) and the IATA code is
        represented by 3 letters and more familiar to the public (e.g. AMS for
        Amsterdam)

        """
        return self._get_unique("diverted")

    @property
    def squawk(self) -> Set[str]:
        """Returns all the unique squawk values in the trajectory.

        A squawk code is a four-digit number assigned by ATC and set on the
        transponder. Some squawk codes are reserved for specific situations and
        emergencies, e.g. 7700 for general emergency, 7600 for radio failure or
        7500 for hijacking.
        """
        return set(self.data.squawk.unique())

    @property
    def icao24(self) -> Union[str, Set[str], None]:
        """Returns the unique icao24 value(s) of the DataFrame.

        icao24 (ICAO 24-bit address) is a unique identifier associated to a
        transponder. These identifiers correlate to the aircraft registration.

        For example icao24 code 'ac82ec' is associated to 'N905NA'.
        """
        icao24 = self._get_unique("icao24")
        if icao24 != icao24:
            raise ValueError("NaN appearing in icao24 field")
        return icao24

    @property
    def registration(self) -> Optional[str]:
        from ..data import aircraft

        reg = self._get_unique("registration")
        if isinstance(reg, str):
            return reg

        if not isinstance(self.icao24, str):
            return None
        res = aircraft.get(self.icao24)
        if res is None:
            return None
        return res.get("registration", None)

    @property
    def typecode(self) -> Optional[str]:
        from ..data import aircraft

        tc = self._get_unique("typecode")
        if isinstance(tc, str):
            return tc

        if not isinstance(self.icao24, str):
            return None
        res = aircraft.get(self.icao24)
        if res is None:
            return None
        return res.get("typecode", None)

    @property
    def aircraft(self) -> None | Tail:
        from ..data import aircraft

        if isinstance(self.icao24, str):
            return aircraft.get(self.icao24)

        return None

    def summary(self, attributes: list[str]) -> dict[str, Any]:
        """Returns a summary of the current Flight structure containing
        featured attributes.

        Example usage:

        >>> t.summary(['icao24', 'start', 'stop', 'duration'])

        Consider monkey-patching properties to the Flight class if you need more
        information in your summary dictionary.

        """
        return dict((key, getattr(self, key)) for key in attributes)

    # -- Time handling, splitting, interpolation and resampling --

    def skip(
        self, value: None | deltalike = None, **kwargs: Any
    ) -> Optional[Flight]:
        """Removes the first n days, hours, minutes or seconds of the Flight.

        The elements passed as kwargs as passed as is to the datetime.timedelta
        constructor.

        Example usage:

        >>> flight.skip(minutes=10)
        >>> flight.skip("1h")
        >>> flight.skip(10)  # seconds by default
        """
        delta = to_timedelta(value, **kwargs)
        bound = self.start + delta  # noqa: F841 => used in the query
        # full call is necessary to keep @bound as a local variable
        df = self.data.query("timestamp >= @bound")
        if df.shape[0] == 0:
            return None
        return self.__class__(df)

    def first(self, value: Optional[deltalike] = None, **kwargs: Any) -> Flight:
        """Returns the first n days, hours, minutes or seconds of the Flight.

        The elements passed as kwargs as passed as is to the datetime.timedelta
        constructor.

        Example usage:

        >>> flight.first(minutes=10)
        >>> flight.first("1h")
        >>> flight.first(10)  # seconds by default
        """
        delta = to_timedelta(value, **kwargs)
        bound = self.start + delta  # noqa: F841 => used in the query
        # full call is necessary to keep @bound as a local variable
        df = self.data.query("timestamp < @bound")
        if df.shape[0] == 0:
            # this shouldn't happen
            return None  # type: ignore
        return self.__class__(df)

    def shorten(
        self, value: Optional[deltalike] = None, **kwargs: Any
    ) -> Optional[Flight]:
        """Removes the last n days, hours, minutes or seconds of the Flight.

        The elements passed as kwargs as passed as is to the datetime.timedelta
        constructor.

        Example usage:

        >>> flight.shorten(minutes=10)
        >>> flight.shorten("1h")
        >>> flight.shorten(10)  # seconds by default
        """
        delta = to_timedelta(value, **kwargs)
        bound = self.stop - delta  # noqa: F841 => used in the query
        # full call is necessary to keep @bound as a local variable
        df = self.data.query("timestamp <= @bound")
        if df.shape[0] == 0:
            return None
        return self.__class__(df)

    def last(self, value: Optional[deltalike] = None, **kwargs: Any) -> Flight:
        """Returns the last n days, hours, minutes or seconds of the Flight.

        The elements passed as kwargs as passed as is to the datetime.timedelta
        constructor.

        Example usage:

        >>> flight.last(minutes=10)
        >>> flight.last("1h")
        >>> flight.last(10)  # seconds by default
        """
        delta = to_timedelta(value, **kwargs)
        bound = self.stop - delta  # noqa: F841 => used in the query
        # full call is necessary to keep @bound as a local variable
        df = self.data.query("timestamp > @bound")
        if df.shape[0] == 0:
            # this shouldn't happen
            return None  # type: ignore
        return self.__class__(df)

    def before(self, time: timelike, strict: bool = True) -> Optional[Flight]:
        """Returns the part of the trajectory flown before a given timestamp.

        - ``time`` can be passed as a string, an epoch, a Python datetime, or
          a Pandas timestamp.
        """
        return self.between(self.start, time, strict)

    def after(self, time: timelike, strict: bool = True) -> Optional[Flight]:
        """Returns the part of the trajectory flown after a given timestamp.

        - ``time`` can be passed as a string, an epoch, a Python datetime, or
          a Pandas timestamp.
        """
        return self.between(time, self.stop, strict)

    def between(
        self, start: timelike, stop: time_or_delta, strict: bool = True
    ) -> Optional[Flight]:
        """Returns the part of the trajectory flown between start and stop.

        - ``start`` and ``stop`` can be passed as a string, an epoch, a Python
          datetime, or a Pandas timestamp.
        - ``stop`` can also be passed as a timedelta.

        """

        # Corner cases when start or stop are None or NaT
        if start is None or start != start:
            return self.before(stop, strict=strict)

        if stop is None or stop != stop:
            return self.after(start, strict=strict)

        start = to_datetime(start)
        if isinstance(stop, timedelta):
            stop = start + stop
        else:
            stop = to_datetime(stop)

        # full call is necessary to keep @start and @stop as local variables
        # return self.query('@start < timestamp < @stop')  => not valid
        if strict:
            df = self.data.query("@start < timestamp < @stop")
        else:
            df = self.data.query("@start <= timestamp <= @stop")

        if df.shape[0] == 0:
            return None

        return self.__class__(df)

    def at(self, time: Optional[timelike] = None) -> Optional[Position]:
        """Returns the position in the trajectory at a given timestamp.

        - ``time`` can be passed as a string, an epoch, a Python datetime, or
          a Pandas timestamp.

        - If no time is passed (default), the last know position is returned.
        - If no position is available at the given timestamp, None is returned.
          If you expect a position at any price, consider :meth:`resample`

        """

        if time is None:
            return Position(self.data.ffill().iloc[-1])

        index = to_datetime(time)
        df = self.data.set_index("timestamp")
        if index not in df.index:
            id_ = getattr(self, "flight_id", self.callsign)
            _log.warning(f"No index {index} for flight {id_}")
            return None
        return Position(df.loc[index])

    def at_ratio(self, ratio: float = 0.5) -> Optional[Position]:
        """Returns a position on the trajectory.

        This method is convenient to place a marker on the trajectory in
        visualisation output.

        - ``Flight.at_ratio(0)`` is the first point in the trajectory.
        - ``Flight.at_ratio(1)`` is the last point of the trajectory
          (equivalent to ``Flight.at()``)
        """
        if ratio < 0 or ratio > 1:
            raise RuntimeError("ratio must be comprised between 0 and 1")

        subset = self.between(
            self.start, self.start + ratio * self.duration, strict=False
        )

        assert subset is not None
        return subset.at()

    @flight_iterator
    def sliding_windows(
        self, duration: deltalike, step: deltalike
    ) -> Iterator["Flight"]:
        duration_ = to_timedelta(duration)
        step_ = to_timedelta(step)

        first = self.first(duration_)
        if first is None:
            return

        yield first

        after = self.after(self.start + step_)
        if after is not None:
            yield from after.sliding_windows(duration_, step_)

    @overload
    def split(
        self,
        value: int,
        unit: str,
        condition: None | Callable[["Flight", "Flight"], bool] = None,
    ) -> FlightIterator: ...

    @overload
    def split(
        self,
        value: str,
        unit: None = None,
        condition: None | Callable[["Flight", "Flight"], bool] = None,
    ) -> FlightIterator: ...

    @flight_iterator
    def split(
        self,
        value: Union[int, str] = 10,
        unit: Optional[str] = None,
        condition: None | Callable[["Flight", "Flight"], bool] = None,
    ) -> Iterator["Flight"]:
        """Iterates on legs of a Flight based on the distribution of timestamps.

        By default, the method stops a flight and yields a new one after a gap
        of 10 minutes without data.

        The length of the gap (here 10 minutes) can be expressed:

        - in the NumPy style: ``Flight.split(10, 'm')`` (see
          ``np.timedelta64``);
        - in the pandas style: ``Flight.split('10 min')`` (see ``pd.Timedelta``)

        If the `condition` parameter is set, the flight is split between two
        segments only if `condition(f1, f2)` is verified.

        Example:

        .. code:: python

            def no_split_below_5000ft(f1, f2):
                first = f1.data.iloc[-1].altitude >= 5000
                second = f2.data.iloc[0].altitude >= 5000
                return first or second

            # would yield many segments
            belevingsvlucht.query('altitude > 2000').split('1 min')

            # yields only one segment
            belevingsvlucht.query('altitude > 2000').split(
                '1 min', condition = no_split_below_5000ft
            )

        """
        if isinstance(value, int) and unit is None:
            # default value is 10 m
            unit = "m"

        if condition is None:
            for data in _split(self.data, value, unit):
                yield self.__class__(data)

        else:
            previous = None
            for data in _split(self.data, value, unit):
                if previous is None:
                    previous = self.__class__(data)
                else:
                    latest = self.__class__(data)
                    if condition(previous, latest):
                        yield previous
                        previous = latest
                    else:
                        previous = self.__class__(
                            pd.concat([previous.data, data])
                        )
            if previous is not None:
                yield previous

    def max_split(
        self,
        value: Union[int, str] = "10 min",
        unit: Optional[str] = None,
        key: str = "duration",
    ) -> Optional[Flight]:
        """Returns the biggest (by default, longest) part of trajectory.

        Example usage:

        >>> from traffic.data.samples import elal747
        >>> elal747.query("altitude < 15000").max_split()
        Flight ELY1747
        aircraft: 738043 · 🇮🇱 4X-ELC (B744)
        origin: LIRF (2019-11-03 12:14:40+00:00)
        destination: LLBG (2019-11-03 14:13:00+00:00)

        In this example, the fancy part of the trajectory occurs below
        15,000 ft. The command extracts the plane pattern.

        """

        warnings.warn("Use split().max() instead.", DeprecationWarning)
        assert (isinstance(value, str) and unit is None) or (
            isinstance(value, int) and isinstance(unit, str)
        )
        return self.split(value, unit).max(key=key)  # type: ignore

    def apply_segments(
        self,
        fun: Callable[..., "LazyTraffic"],
        name: str,
        *args: Any,
        **kwargs: Any,
    ) -> Optional[Flight]:
        return getattr(self, name)(*args, **kwargs)(fun)  # type: ignore

    def apply_time(
        self,
        freq: str = "1 min",
        merge: bool = True,
        **kwargs: Any,
    ) -> Flight:
        """Apply features on time windows.

        The following is performed are performed in order.

        - a new column ``rounded`` rounds the timestamp at the given rate;
        - the groupby/apply is operated with parameters passed in apply;
        - if merge is True, the new column in merged into the Flight,
          otherwise a pd.DataFrame is returned.

        For example:

        >>> f.agg_time("10 min", straight=lambda df: Flight(df).distance())

        returns a Flight with a new column ``straight`` with the great circle
        distance between points sampled every 10 minutes.
        """

        if len(kwargs) == 0:
            raise RuntimeError("No feature provided for aggregation.")
        temp_flight = self.assign(
            rounded=lambda df: df.timestamp.dt.round(freq)
        )

        agg_data = None

        for label, fun in kwargs.items():
            agg_data = (
                agg_data.merge(
                    temp_flight.groupby("rounded")
                    .apply(lambda df: fun(self.__class__(df)))
                    .rename(label),
                    left_index=True,
                    right_index=True,
                )
                if agg_data is not None
                else temp_flight.groupby("rounded")
                .apply(lambda df: fun(self.__class__(df)))
                .rename(label)
                .to_frame()
            )

        if not merge:  # mostly for debugging purposes
            return agg_data  # type: ignore

        return temp_flight.merge(agg_data, left_on="rounded", right_index=True)

    def agg_time(
        self, freq: str = "1 min", merge: bool = True, **kwargs: Any
    ) -> Flight:
        """Aggregate features on time windows.

        The following is performed are performed in order.

        - a new column ``rounded`` rounds the timestamp at the given rate;
        - the groupby/agg is operated with parameters passed in kwargs;
        - if merge is True, the new column in merged into the Flight,
          otherwise a pd.DataFrame is returned.

        For example:

        >>> f.agg_time('3T', groundspeed='mean')

        returns a Flight with a new column groundspeed_mean with groundspeed
        averaged per intervals of 3 minutes.
        """

        def flatten(
            data: pd.DataFrame, how: Callable[..., Any] = "_".join
        ) -> pd.DataFrame:
            data.columns = (
                [
                    how(filter(None, map(str, levels)))
                    for levels in data.columns.to_numpy()
                ]
                if isinstance(data.columns, pd.MultiIndex)
                else data.columns
            )
            return data

        if len(kwargs) == 0:
            raise RuntimeError("No feature provided for aggregation.")
        temp_flight = self.assign(
            rounded=lambda df: df.timestamp.dt.round(freq)
        )

        # force the agg_data to be multi-indexed in columns
        kwargs_modified: Dict["str", List[Any]] = dict(
            (
                key,
                list(value)
                if any(isinstance(value, x) for x in [list, tuple])
                else [value],
            )
            for key, value in kwargs.items()
        )
        agg_data = flatten(temp_flight.groupby("rounded").agg(kwargs_modified))

        if not merge:
            # WARN: Return type is inconsistent but this is mostly for
            # debugging purposes
            return agg_data  # type: ignore

        return temp_flight.merge(agg_data, left_on="rounded", right_index=True)

    def handle_last_position(self) -> Flight:
        # The following is True for all data coming from the Impala shell.
        # The following is an attempt to fix #7
        # Note the fun/fast way to produce 1 or trigger NaN (division by zero)
        data = self.data.sort_values("timestamp")
        if "last_position" in self.data.columns:
            data = (
                data.assign(
                    _mark=lambda df: (
                        df.last_position != df.shift(1).last_position
                    ).astype(float)
                )
                .assign(
                    latitude=lambda df: df.latitude * (df._mark / df._mark),
                    longitude=lambda df: df.longitude * (df._mark / df._mark),
                    altitude=lambda df: df.altitude * (df._mark / df._mark),
                )
                # keeping last_position causes more problems (= Nan) than
                # anything. Safer to just remove it for now. Like it or not!
                .drop(columns=["_mark", "last_position"])
            )

        return self.__class__(data)

    def resample(
        self,
        rule: str | int = "1s",
        how: None | str | dict[str, Iterable[str]] = "interpolate",
        interpolate_kw: dict[str, Any] = {},
        projection: None | str | pyproj.Proj | "crs.Projection" = None,
    ) -> Flight:
        """Resample the trajectory at a given frequency or for a target number
        of samples.

        :param rule:

            - If the rule is a string representing
              :ref:`pandas:timeseries.offset_aliases` for time frequencies is
              passed, then the data is resampled along the timestamp axis, then
              interpolated (according to the ``how`` parameter).

            - If the rule is an integer, the trajectory is resampled to the
              given number of evenly distributed points per trajectory.

        :param how: (default: ``"interpolate"``)

            - When the parameter is a string, the method applies to all columns
            - When the parameter is a dictionary with keys as methods (e.g.
              ``"interpolate"``, ``"ffill"``) and names of columns as values.
              Columns not included in any value are left as is.

        :param interpolate_kw: (default: ``{}``)

            - A dictionary with keyword arguments that will be passed to the
              pandas interpolate method.

              Example usage:
              To specify a fifth-degree polynomial interpolation, you can
              pass the following dictionary:

              .. code-block:: python

                interpolate_kw = {"method": "polynomial", "order": 5}


        :param projection: (default: ``None``)

            - By default, lat/lon are resampled with a linear interpolation;
            - If a projection is passed, the linear interpolation is applied on
              the x and y dimensions, then lat/lon are reprojected back;
            - If the projection is a string parameter, e.g. ``"lcc"``, a
              projection is created on the fly, centred on the trajectory. This
              approach is helpful to fill gaps along a great circle.

        """
        if projection is not None:
            if isinstance(projection, str):
                projection = pyproj.Proj(
                    proj=projection,
                    ellps="WGS84",
                    lat_1=self.data.latitude.min(),
                    lat_2=self.data.latitude.max(),
                    lat_0=self.data.latitude.mean(),
                    lon_0=self.data.longitude.mean(),
                )
            self = self.compute_xy(projection=projection)

        if isinstance(rule, str):
            data = (
                self.handle_last_position()
                .unwrap()
                .data.set_index("timestamp")
                .resample(rule)
                .first()
                .reset_index(names="timestamp")
            )

            data = data.infer_objects(copy=False)

            if how is None:
                how = {}

            if isinstance(how, str):
                if how == "interpolate":
                    interpolable = data.select_dtypes(["float", "int"])
                    how = {
                        "interpolate": set(interpolable),
                        "ffill": set(
                            data.select_dtypes(
                                exclude=["float", "int", "bool", "datetime"]
                            )
                        ),
                    }
                else:
                    how = {how: set(data.columns) - {"timestamp"}}

            for meth, columns in how.items():
                if meth is not None:
                    idx = data.columns.get_indexer(columns)
                    kwargs = interpolate_kw if meth == "interpolate" else {}
                    value = getattr(data[list(columns)], meth)(**kwargs)
                    data[data.columns[idx]] = value

        elif isinstance(rule, int):
            # use pd.date_range to compute the best freq
            new_index = pd.date_range(self.start, self.stop, periods=rule)
            data = (
                self.handle_last_position()
                .unwrap()  # avoid filled gaps in track and heading
                .data.set_index("timestamp")
                .reindex(new_index, method="nearest")
                .reset_index(names="timestamp")
            )
        else:
            raise TypeError("rule must be a str or an int")

        if "track_unwrapped" in data.columns:
            data = data.assign(track=lambda df: df.track_unwrapped % 360)
        if "heading_unwrapped" in data.columns:
            data = data.assign(heading=lambda df: df.heading_unwrapped % 360)

        res = self.__class__(data)

        if projection is not None:
            res = res.compute_latlon_from_xy(projection=projection)

        return res

    def filter(
        self,
        filter: Literal["default", "aggressive"]
        | filters.FilterBase = "default",
        strategy: None
        | Callable[[pd.DataFrame], pd.DataFrame] = lambda x: x.bfill().ffill(),
        **kwargs: int | tuple[int],
    ) -> Flight:
        """Filters a trajectory with predefined methods.

        :param filter: (default:
            :class:`~traffic.algorithms.filters.FilterAboveSigmaMedian`) is one
            of the filters predefined in :ref:`traffic.algorithms.filters`
            or any filter implementing the
            :class:`~traffic.algorithms.filters.Filter` protocol.
            Use "aggressive" for an experimental filter by @krumjan

        :param strategy: (default: backward fill followed by forward fill)
            is applied after the filter to deal with resulting NaN values.

            - Explicitely specify to `None` if NaN values should be left as is.
            - ``lambda x: x.interpolate()`` may be a smart strategy

        More filters are available on the :ref:`traffic.algorithms.filters` page

        """
        from ..algorithms.filters import aggressive

        filter_dict = dict(
            default=filters.FilterAboveSigmaMedian(**kwargs),
            aggressive=filters.FilterMedian()
            | aggressive.FilterDerivative()
            | aggressive.FilterClustering()
            | filters.FilterMean(),
        )

        if isinstance(filter, str):
            filter = filter_dict.get(
                filter, filters.FilterAboveSigmaMedian(**kwargs)
            )

        preprocess = self
        if filter.projection is not None:
            preprocess = preprocess.compute_xy(filter.projection)

        new_data = filter.apply(
            preprocess.data.sort_values(by="timestamp")
            .reset_index(drop=True)
            .copy()
        )

        if strategy is not None:
            new_data = strategy(new_data)

        postprocess = self.__class__(new_data)
        if filter.projection is not None:
            postprocess = postprocess.compute_latlon_from_xy(filter.projection)
        return postprocess

    def filter_position(
        self, cascades: int = 2
    ) -> Optional[Flight]:  # DEPRECATED
        from ..algorithms.filters import FilterPosition

        warnings.warn(
            "Deprecated filter_position method, "
            "use .filter() with FilterPosition instead",
            DeprecationWarning,
        )

        return self.filter(FilterPosition(cascades))

    def predict(
        self,
        *args: Any,
        method: Literal["default", "straight", "flightplan"]
        | PredictBase = "default",
        **kwargs: Any,
    ) -> Flight:
        """Predicts the future trajectory based on the past data points.

        :param method: By default, the method propagates the trajectory in a
          straight line.

        If the method argument is passed as a string, then all args and kwargs
        argument of the landing method are passed to the constructor of the
        corresponding prediction class.

        The following table summarizes the available methods and their
        corresponding classes:

        - ``straight`` (default) uses
          :class:`~traffic.algorithms.prediction.straightline.StraightLinePredict`

        - ``flightplan`` uses
          :class:`~traffic.algorithms.prediction.flightplan.FlightPlanPredict`

        Example usage:

        >>> flight.predict(minutes=10, method="straight")
        >>> flight.before("2018-12-24 23:55").predict(minutes=10)  # Merry XMas!

        """
        from ..algorithms.prediction import PredictBase
        from ..algorithms.prediction.flightplan import FlightPlanPredict
        from ..algorithms.prediction.straightline import StraightLinePredict

        if len(args) and isinstance(args[0], PredictBase):
            method = args[0]
            args = tuple(*args[1:])

        method_dict = dict(
            default=StraightLinePredict,
            straight=StraightLinePredict,
            flightplan=FlightPlanPredict,
        )

        method = (
            method_dict[method](*args, **kwargs)
            if isinstance(method, str)
            else method
        )

        return method.predict(self)

    def forward(
        self,
        forward: Union[None, str, pd.Timedelta] = None,
        **kwargs: Any,
    ) -> "Flight":  # DEPRECATED
        from ..algorithms.prediction.straightline import StraightLinePredict

        warnings.warn(
            "Deprecated forward method, use .predict() instead",
            DeprecationWarning,
        )

        return StraightLinePredict(forward, **kwargs).predict(self)

    # -- Air traffic management --

    def assign_id(
        self, name: str = "{self.callsign}_{idx:>03}", idx: int = 0
    ) -> Flight:
        """Assigns a flight_id to a Flight.

        This method is more generally used by the corresponding Traffic and
        LazyTraffic methods but works fine on Flight as well.
        """
        if "callsign" not in self.data.columns and "callsign" in name:
            if name == "{self.callsign}_{idx:>03}":  # default arg
                name = "{self.icao24}_{idx:>03}"
            else:
                msg = "Specify a name argument without the `callsign` property"
                raise RuntimeError(msg)
        return self.assign(flight_id=name.format(self=self, idx=idx))

    @flight_iterator
    def emergency(self) -> Iterator["Flight"]:
        """Iterates on emergency segments of trajectory.

        An emergency is defined with a 7700 squawk code.
        """
        squawk7700 = self.query("squawk == '7700'")
        if squawk7700 is None:
            return
        yield from squawk7700.split("10 min")

    def onground(self) -> Optional[Flight]:
        if "altitude" not in self.data.columns:
            return self
        if "onground" in self.data.columns and self.data.onground.dtype == bool:
            return self.query("onground or altitude.isnull()")
        else:
            return self.query("altitude.isnull()")

    def airborne(self) -> Optional[Flight]:
        """Returns the airborne part of the Flight.

        The airborne part is determined by an ``onground`` flag or null values
        in the altitude column.
        """
        if "altitude" not in self.data.columns:
            return None
        if "onground" in self.data.columns and self.data.onground.dtype == bool:
            return self.query("not onground and altitude.notnull()")
        else:
            return self.query("altitude.notnull()")

    def unwrap(self, features: Union[None, str, List[str]] = None) -> Flight:
        """Unwraps angles in the DataFrame.

        All features representing angles may be unwrapped (through Numpy) to
        avoid gaps between 359° and 1°.

        The method applies by default to features ``track`` and ``heading``.
        More or different features may be passed in parameter.
        """
        if features is None:
            features = default_angle_features

        if isinstance(features, str):
            features = [features]

        reset = self.reset_index(drop=True)

        result_dict = dict()
        for feature in features:
            if feature not in reset.data.columns:
                continue
            series = reset.data[feature].astype(float)
            idx = ~series.isnull()
            result_dict[f"{feature}_unwrapped"] = pd.Series(
                np.degrees(np.unwrap(np.radians(series.loc[idx]))),
                index=series.loc[idx].index,
            )

        return reset.assign(**result_dict)

    def compute_TAS(self) -> Flight:
        """Computes the wind triangle for each timestamp.

        This method requires ``groundspeed``, ``track``, ``wind_u`` and
        ``wind_v`` (in knots) to compute true airspeed (``TAS``), and
        ``heading`` features. The groundspeed and the track angle are usually
        available in ADS-B messages; wind information may be included from a
        GRIB file using the :meth:`~traffic.core.Flight.include_grib` method.

        """

        if any(w not in self.data.columns for w in ["wind_u", "wind_v"]):
            raise RuntimeError(
                "No wind data in trajectory. Consider Flight.include_grib()"
            )

        return self.assign(
            tas_x=lambda df: df.groundspeed * np.sin(np.radians(df.track))
            - df.wind_u,
            tas_y=lambda df: df.groundspeed * np.cos(np.radians(df.track))
            - df.wind_v,
            TAS=lambda df: np.abs(df.tas_x + 1j * df.tas_y),
            heading_rad=lambda df: np.angle(df.tas_x + 1j * df.tas_y),
            heading=lambda df: (90 - np.degrees(df.heading_rad)) % 360,
        ).drop(columns=["tas_x", "tas_y", "heading_rad"])

    def compute_wind(self) -> Flight:
        """Computes the wind triangle for each timestamp.

        This method requires ``groundspeed``, ``track``, true airspeed
        (``TAS``), and ``heading`` features. The groundspeed and the track angle
        are usually available in ADS-B messages; the heading and the true
        airspeed may be decoded in EHS messages.

        .. note::

            Check the :meth:`query_ehs` method to find a way to enrich your
            flight with such features. Note that this data is not necessarily
            available depending on the location.

        """

        if any(w not in self.data.columns for w in ["heading", "TAS"]):
            raise RuntimeError(
                "No wind data in trajectory. Consider Flight.query_ehs()"
            )

        return self.assign(
            wind_u=self.data.groundspeed * np.sin(np.radians(self.data.track))
            - self.data.TAS * np.sin(np.radians(self.data.heading)),
            wind_v=self.data.groundspeed * np.cos(np.radians(self.data.track))
            - self.data.TAS * np.cos(np.radians(self.data.heading)),
        )

    def phases(
        self,
        *args: Any,
        method: Literal["default", "openap"] | ApplyBase = "default",
        **kwargs: Any,
    ) -> Flight:
        """Label phases in flight trajectories.

        An extra ``"phase"`` column is added to the DataFrame.

        The only available (default) method is provided by OpenAP.

        Usage:
        See: :ref:`How to find flight phases on a trajectory?`

        """
        from ..algorithms.navigation import phases

        method_dict = dict(
            default=phases.FlightPhasesOpenAP,
            openap=phases.FlightPhasesOpenAP,
        )

        method = (
            method_dict[method](*args, **kwargs)
            if isinstance(method, str)
            else method
        )

        return method.apply(self)

    # -- Metadata inference methods --

    def infer_airport(
        self,
        method: Literal["takeoff", "landing"] | airports.AirportInferenceBase,
        **kwargs: Any,
    ) -> Airport:
        """Infers the takeoff or landing airport from the trajectory data.

        :param method: selects the detection method

        - ``"landing"`` uses
          :class:`~traffic.algorithms.metadata.airports.LandingAirportInference`

        - ``"takeoff"`` uses
          :class:`~traffic.algorithms.metadata.airports.TakeoffAirportInference`

        Usage:

        >>> flight.infer_airport("takeoff")
        >>> flight.infer_airport("landing")

        Check the documentation of the classes for more options.

        """
        from ..algorithms.metadata import airports

        method_dict = dict(
            takeoff=airports.TakeoffAirportInference,
            landing=airports.LandingAirportInference,
        )

        # TODO check why typing doesn't work only on this function
        method = (
            method_dict[method](**kwargs)  # type: ignore
            if isinstance(method, str)
            else method
        )

        return method.infer(self)  # type: ignore

    def takeoff_from(self, airport: str | Airport) -> bool:
        """Returns True if the flight takes off from the given airport."""

        from ..data import airports

        _airport = (
            airport if isinstance(airport, Airport) else airports[airport]
        )

        return self.infer_airport("takeoff") == _airport

    def landing_at(self, airport: str | Airport) -> bool:
        """Returns True if the flight lands at the given airport.

        :param airport: Airport where the ILS is located
        """

        from ..data import airports

        _airport = (
            airport if isinstance(airport, Airport) else airports[airport]
        )

        return self.infer_airport("landing") == _airport

    def infer_flightplan(
        self,
        *args: Any,
        method: Literal["default"] | flightplan.FlightPlanBase = "default",
        **kwargs: Any,
    ) -> None | pd.DataFrame:
        """Infers a possible flight-plan from the trajectory data.

        :param method: selects the detection method

        The only available (default) method is
        :class:`~traffic.algorithms.metadata.flightplan.FlightPlanInference`.
        Check the documentation there for more details.

        Check the documentation of the class for more options.

        """
        from ..algorithms.metadata import flightplan

        if len(args) and isinstance(args[0], flightplan.FlightPlanBase):
            method = args[0]
            args = tuple(*args[1:])

        method_dict = dict(
            default=flightplan.FlightPlanInference,
        )

        method = (
            method_dict[method](*args, **kwargs)
            if isinstance(method, str)
            else method
        )

        return method.infer(self)

    # -- Navigation methods --

    @flight_iterator
    def aligned(
        self,
        *args: Any,
        method: Literal["default", "beacon", "ils", "runway"]
        | ApplyIteratorBase = "default",
        **kwargs: Any,
    ) -> Iterator["Flight"]:
        """Detects a geometric alignment with aeronautical infrastructure.

        :param method: By default, the method checks a geometric alignment with
          a navigational beacon (navaid).

        If the method argument is passed as a string, then all args and kwargs
        argument of the landing method are passed to the constructor of the
        corresponding landing detection class.

        The following table summarizes the available methods and their
        corresponding classes:

        - ``"beacon"`` (default) uses
          :class:`~traffic.algorithms.navigation.alignment.BeaconTrackBearingAlignment`
          and compares the track angle of the aircraft with the bearing to
          a given point.

        - ``"ils"`` uses
          :class:`~traffic.algorithms.navigation.landing.LandingAlignedOnILS`
          and detects segments on trajectory aligned with the ILS of a given
          airport.

        - ``"runway"``  uses
          :class:`~traffic.algorithms.ground.runway.RunwayAlignment`
          and detects segments aligned with one of the documented runways.

        Usage: Check the corresponding classes which are properly documented.

        """
        from ..algorithms.ground import runway
        from ..algorithms.navigation import (
            ApplyIteratorBase,
            alignment,
            landing,
        )

        if len(args) and isinstance(args[0], ApplyIteratorBase):
            method = args[0]
            args = tuple(*args[1:])

        method_dict = dict(
            default=alignment.BeaconTrackBearingAlignment,
            beacon=alignment.BeaconTrackBearingAlignment,
            ils=landing.LandingAlignedOnILS,
            runway=runway.RunwayAlignment,
        )

        method = (
            method_dict[method](*args, **kwargs)
            if isinstance(method, str)
            else method
        )

        yield from method.apply(self)

    @flight_iterator
    def go_around(
        self,
        *args: None,
        method: Literal["default"] | ApplyIteratorBase = "default",
        **kwargs: Any,
    ) -> Iterator["Flight"]:
        """Detects go-around situations in a trajectory.

        Usage:
        See: :ref:`How to select go-arounds from a set of trajectories?`
        """

        from ..algorithms.navigation import ApplyIteratorBase, go_around

        if len(args) and isinstance(args[0], ApplyIteratorBase):
            method = args[0]
            args = tuple(*args[1:])

        method_dict = dict(default=go_around.GoAroundDetection)

        method = (
            method_dict[method](*args, **kwargs)
            if isinstance(method, str)
            else method
        )

        yield from method.apply(self)

    @flight_iterator
    def holding_pattern(
        self,
        *args: Any,
        method: Literal["default"] | ApplyIteratorBase = "default",
        **kwargs: Any,
    ) -> Iterator["Flight"]:
        """Detects holding patterns in a trajectory.

        Usage:
        See :ref:`How to detect holding patterns in aircraft trajectories?`
        """
        from ..algorithms.navigation import holding_pattern

        if len(args) and isinstance(args[0], ApplyIteratorBase):
            method = args[0]
            args = tuple(*args[1:])

        method_dict = dict(default=holding_pattern.MLHoldingDetection)

        method = (
            method_dict[method](*args, **kwargs)
            if isinstance(method, str)
            else method
        )

        yield from method.apply(self)

    @flight_iterator
    def landing(
        self,
        *args: Any,
        method: Literal["default", "aligned_on_ils", "any", "runway_change"]
        | ApplyIteratorBase = "default",
        **kwargs: Any,
    ) -> Iterator["Flight"]:
        """Detects the landing phase in a trajectory.

        :param method:  By default, the method detects segments on trajectory
          aligned with the ILS of a given airport.

        If the method argument is passed as a string, then all args and kwargs
        argument of the landing method are passed to the constructor of the
        corresponding landing detection class.

        The following table summarizes the available methods and their
        corresponding classes:

        - ``"aligned_on_ils"`` (default) uses
          :class:`~traffic.algorithms.navigation.landing.LandingAlignedOnILS`
          and detects segments on trajectory aligned with the ILS of a given
          airport.

        - ``"anywhere"`` uses
          :class:`~traffic.algorithms.navigation.landing.LandingAnyAttempt`
          and detects the most plausible landing airport for all pieces of
          trajectories below a threshold altitude.

        - ``"runway_change"`` uses
          :class:`~traffic.algorithms.navigation.landing.LandingWithRunwayChange`
          and detects a specific subset of situations where aircraft are aligned
          on several runways during one landing phase.

        Usage:

        All the following calls are equivalent:

        >>> flight.landing("EHAM")  # returns a flight iterator
        >>> flight.landing("EHAM", method="default")
        >>> flight.landing(airport="EHAM", method="default")
        >>> flight.landing(method=LandingAlignedOnILS(airport="EHAM"))

        As with other :class:`~traffic.core.FlightIterator`, we can:

        - check whether an aircraft is landing at a given airport:

          >>> flight.landing("EHAM").has()
          >>> flight.has("landing('EHAM')")

        - get the first landing attempt:

          >>> flight.landing("EHAM").next()
          >>> flight.next("landing('EHAM')")

        More details in the specific documentation for each class.

        """
        from ..algorithms.navigation import ApplyIteratorBase, landing

        if len(args) and isinstance(args[0], ApplyIteratorBase):
            method = args[0]
            args = tuple(*args[1:])

        method_dict = dict(
            default=landing.LandingAlignedOnILS,
            aligned_on_ils=landing.LandingAlignedOnILS,
            any=landing.LandingAnyAttempt,
            runway_change=landing.LandingWithRunwayChange,
        )

        method = (
            method_dict[method](*args, **kwargs)
            if isinstance(method, str)
            else method
        )

        yield from method.apply(self)

    @flight_iterator
    def point_merge(
        self,
        *args: Any,
        method: Literal["default", "alignment"] | ApplyIteratorBase,
        **kwargs: Any,
    ) -> Iterator["Flight"]:
        """Detects point-merge structures in trajectories.

        Usage:
        See :ref:`How to implement point-merge detection?`

        """
        from ..algorithms.navigation import ApplyIteratorBase, point_merge

        if len(args) and isinstance(args[0], ApplyIteratorBase):
            method = args[0]
            args = tuple(*args[1:])

        method_dict = dict(
            default=point_merge.PointMerge,
            alignment=point_merge.PointMerge,
        )

        method = (
            method_dict[method](*args, **kwargs)
            if isinstance(method, str)
            else method
        )

        yield from method.apply(self)

    @flight_iterator
    def takeoff(
        self,
        *args: Any,
        method: Literal["default", "polygon_based", "track_based"]
        | ApplyIteratorBase = "default",
        **kwargs: Any,
    ) -> Iterator["Flight"]:
        """Detects the takeoff phase in a trajectory.

        :param method: By default, the method detects segments on trajectory
          maximizing their intersection with a trapeze shape with a small base
          at runway threshold.

        If the method argument is passed as a string, then all args and kwargs
        argument of the landing method are passed to the constructor of the
        corresponding landing detection class.

        The following table summarizes the available methods and their
        corresponding classes:

        - ``"polygon_based"`` (default) uses
          :class:`~traffic.algorithms.navigation.takeoff.PolygonBasedRunwayDetection`
          and detects segments on trajectory maximizing their intersection with
          a trapeze shape with a small base at runway threshold. This method
          performs better when trajectory data point is scarce at surface level.

        - ``"track_based"`` uses
          :class:`~traffic.algorithms.navigation.takeoff.TrackBasedRunwayDetection`
          and detects pieces of trajectory with a strong acceleration that is
          colinear to a documented runway. This method performs better when
          data is rich at surface level, with less false positive labelled with
          the wrong runway.

        Usage:

        >>> flight.takeoff("EHAM")  # returns a flight iterator
        >>> flight.takeoff("EHAM", method="default")
        >>> flight.takeoff(airport="EHAM", method="default")
        >>> flight.takeoff(PolygonBasedRunwayDetection(airport="EHAM"))

        More details in the specific documentation for each class.

        """
        from ..algorithms.navigation import ApplyIteratorBase, takeoff

        if len(args) and isinstance(args[0], ApplyIteratorBase):
            method = args[0]
            args = tuple(*args[1:])

        method_dict = dict(
            default=takeoff.PolygonBasedRunwayDetection,
            polygon_based=takeoff.PolygonBasedRunwayDetection,
            track_based=takeoff.TrackBasedRunwayDetection,
        )

        method = (
            method_dict[method](*args, **kwargs)
            if isinstance(method, str)
            else method
        )

        yield from method.apply(self)

    @flight_iterator
    def thermals(self) -> Iterator["Flight"]:
        """Detects pieces of trajectory where gliders are in thermals.

        The logic implemented detects trajectory ascending and turning at the
        same time.

        Usage:

        >>> flight_iterator = flight.thermals()
        """
        from ..algorithms.navigation.thermals import GliderThermal

        yield from GliderThermal().apply(self)

    @flight_iterator
    def parking_position(
        self,
        *args: Any,
        method: Literal["default", "geometry"] | ApplyIteratorBase = "default",
        **kwargs: Any,
    ) -> Iterator["Flight"]:
        """Detects pieces of trajectory matching documented parking positions.

        The only available (default) method,
        :class:`~traffic.algorithms.ground.parking_position.ParkingPositionGeometricIntersection`,
        looks at the intersection between the trajectory and a buffered version
        of the parking positions.

        Check the documentation of the corresponding class for more details.
        """
        from ..algorithms.ground import parking_position
        from ..algorithms.navigation import ApplyIteratorBase

        if len(args) and isinstance(args[0], ApplyIteratorBase):
            method = args[0]
            args = tuple(*args[1:])

        method_dict = dict(
            default=parking_position.ParkingPositionGeometricIntersection,
            geometry=parking_position.ParkingPositionGeometricIntersection,
        )

        method = (
            method_dict[method](*args, **kwargs)
            if isinstance(method, str)
            else method
        )

        yield from method.apply(self)

    def movement(
        self,
        *args: Any,
        method: Literal["default", "start_moving"]
        | ApplyOptionalBase = "default",
        **kwargs: Any,
    ) -> Optional["Flight"]:
        """Detects when the aircraft starts moving on the surface.

        The only available (default) method is
        :class:`~traffic.algorithms.ground.movement.StartMoving`. Check the
        documentation there for more details.

        :return: the trajectory trimmed from the non-moving part.
        """
        from ..algorithms.ground import movement
        from ..algorithms.navigation import ApplyOptionalBase

        if len(args) and isinstance(args[0], ApplyOptionalBase):
            method = args[0]
            args = tuple(*args[1:])

        method_dict = dict(
            default=movement.StartMoving,
            start_moving=movement.StartMoving,
        )

        method = (
            method_dict[method](*args, **kwargs)
            if isinstance(method, str)
            else method
        )

        return method.apply(self)

    def pushback(
        self,
        *args: Any,
        method: Literal["default", "parking_area", "parking_position"]
        | ApplyOptionalBase = "default",
        **kwargs: Any,
    ) -> Optional["Flight"]:
        """Detects the push-back phase of the trajectory.

        :param method: By default, the method identifies the start of the
          movement, the parking_position and the moment the aircraft suddenly
          changes direction the computed track angle.

        If the method argument is passed as a string, then all args and kwargs
        argument of the landing method are passed to the constructor of the
        corresponding landing detection class.

        The following table summarizes the available methods and their
        corresponding classes:

        - ``"parking_area"`` (default) uses
          :class:`~traffic.algorithms.ground.pushback.ParkingAreaBasedPushback`
          and identifies the start of the movement, an intersection with a
          documented apron area and the moment the aircraft suddenly changes
          direction in the computed track angle

        - ``"parking_position"`` (default) uses
          :class:`~traffic.algorithms.ground.pushback.ParkingPositionBasedPushback`
          and identifies the start of the movement, the parking_position
          and the moment the aircraft suddenly changes direction in the computed
          track angle.

        Usage:

        >>> flight.pushback(airport="LSZH", method="default")

        """
        from ..algorithms.ground import pushback
        from ..algorithms.navigation import ApplyOptionalBase

        if len(args) and isinstance(args[0], ApplyOptionalBase):
            method = args[0]
            args = tuple(*args[1:])

        method_dict = dict(
            default=pushback.ParkingAreaBasedPushback,
            parking_area=pushback.ParkingAreaBasedPushback,
            parking_position=pushback.ParkingPositionBasedPushback,
        )

        method = (
            method_dict[method](*args, **kwargs)
            if isinstance(method, str)
            else method
        )

        return method.apply(self)

    @flight_iterator
    def aligned_on_ils(
        self,
        airport: Union[str, "Airport"],
        angle_tolerance: float = 0.1,
        min_duration: deltalike = "1 min",
        max_ft_above_airport: float = 5000,
    ) -> Iterator["Flight"]:  # DEPRECATED
        from ..algorithms.navigation.landing import LandingAlignedOnILS

        warnings.warn(
            "Deprecated aligned_on_ils method, use .landing() instead",
            DeprecationWarning,
        )

        method = LandingAlignedOnILS(
            airport, angle_tolerance, min_duration, max_ft_above_airport
        )
        yield from method.apply(self)

    @flight_iterator
    def takeoff_from_runway(
        self,
        airport: Union[str, "Airport"],
        max_ft_above_airport: float = 5000,
        zone_length: int = 6000,
        little_base: int = 50,
        opening: float = 5,
    ) -> Iterator["Flight"]:  # DEPRECATED
        from ..algorithms.navigation.takeoff import PolygonBasedRunwayDetection

        warnings.warn(
            "Deprecated takeoff_from_runway method, use .takeoff() instead",
            DeprecationWarning,
        )

        method = PolygonBasedRunwayDetection(
            airport, max_ft_above_airport, zone_length, little_base, opening
        )
        yield from method.apply(self)

    @flight_iterator
    def aligned_on_navpoint(
        self,
        points: Union[str, "PointLike", Iterable["PointLike"]],
        angle_precision: int = 1,
        time_precision: str = "2 min",
        min_time: str = "30s",
        min_distance: int = 80,
    ) -> Iterator["Flight"]:  # DEPRECATED
        from ..algorithms.navigation.alignment import (
            BeaconTrackBearingAlignment,
        )

        warnings.warn(
            "Deprecated aligned_on_navpoint method, use .aligned() instead",
            DeprecationWarning,
        )

        method = BeaconTrackBearingAlignment(
            points, angle_precision, time_precision, min_time, min_distance
        )
        yield from method.apply(self)

    @flight_iterator
    def aligned_on_runway(
        self, airport: str | Airport
    ) -> Iterator["Flight"]:  # DEPRECATED
        from ..algorithms.ground.runway import RunwayAlignment

        warnings.warn(
            "Deprecated aligned_on_runway method, use .aligned() instead",
            DeprecationWarning,
        )

        method = RunwayAlignment(airport=airport)
        yield from method.apply(self)

    # -- End of navigation and ground methods --

    # -- Performance estimation methods --

    def fuelflow(
        self,
        *args: Any,
        method: Literal["default", "openap"] | EstimatorBase = "default",
        **kwargs: Any,
    ) -> Flight:
        """Estimate the mass and fuel flow of the aircraft based on its
        trajectory.

        :param method: At the moment, only the OpenAP implementation is
          available, and is the default implementation.

        If the method argument is passed as a string, then all args and kwargs
        argument of the landing method are passed to the constructor of the
        corresponding fuel flow class.

        """
        from ..algorithms.performance import EstimatorBase, openap

        if len(args) and isinstance(args[0], EstimatorBase):
            method = args[0]
            args = tuple(*args[1:])

        method_dict = dict(
            default=openap.FuelflowEstimation,
            openap=openap.FuelflowEstimation,
        )

        method = (
            method_dict[method](*args, **kwargs)
            if isinstance(method, str)
            else method
        )

        return method.estimate(self)

    def emission(
        self,
        *args: Any,
        method: Literal["default", "openap"] | EstimatorBase = "default",
        **kwargs: Any,
    ) -> Flight:
        """Estimate the pollutants emitted by the aircraft based on its
        trajectory.

        :param method: At the moment, only the OpenAP implementation is
          available, and is the default implementation.

        If the method argument is passed as a string, then all args and kwargs
        argument of the landing method are passed to the constructor of the
        corresponding fuel flow class.

        """
        from ..algorithms.performance import openap

        if len(args) and isinstance(args[0], EstimatorBase):
            method = args[0]
            args = tuple(*args[1:])

        method_dict = dict(
            default=openap.PollutantEstimation,
            openap=openap.PollutantEstimation,
        )

        method = (
            method_dict[method](*args, **kwargs)
            if isinstance(method, str)
            else method
        )

        return method.estimate(self)

    # -- End of performance estimation methods --

    def plot_wind(
        self,
        ax: "GeoAxes",
        resolution: Union[int, str, Dict[str, float], None] = "5 min",
        filtered: bool = False,
        **kwargs: Any,
    ) -> List["Artist"]:  # coverage: ignore
        """Plots the wind field seen by the aircraft on a Matplotlib axis.

        The Flight supports Cartopy axis as well with automatic projection. If
        no projection is provided, a default :meth:`~cartopy.crs.PlateCarree`
        is applied.

        The `resolution` argument may be:

            - None for a raw plot;
            - an integer or a string to pass to a :meth:`resample` method as a
              preprocessing before plotting;
            - or a dictionary, e.g dict(latitude=4, longitude=4), if you
              want a grid with a resolution of 4 points per latitude and
              longitude degree.

        Example usage:

        .. code:: python

            from cartes.crs import Mercator
            fig, ax = plt.subplots(1, subplot_kw=dict(projection=Mercator()))
            (
                flight
                .resample("1s")
                .query('altitude > 10000')
                .compute_wind()
                .plot_wind(ax, alpha=.5)
            )

        """

        from cartopy.crs import PlateCarree

        if "projection" in ax.__dict__ and "transform" not in kwargs:
            kwargs["transform"] = PlateCarree()

        if any(w not in self.data.columns for w in ["wind_u", "wind_v"]):
            raise RuntimeError(
                "No wind data in trajectory. Consider Flight.compute_wind()"
            )

        copy_self: Optional[Flight] = self

        if filtered:
            copy_self = self.filter(roll=17)
            if copy_self is None:
                return []
            copy_self = copy_self.query("roll.abs() < .5")
            if copy_self is None:
                return []
            copy_self = copy_self.filter(wind_u=17, wind_v=17)

        if copy_self is None:
            return []

        if resolution is not None:
            if isinstance(resolution, (int, str)):
                data = copy_self.resample(resolution).data

            if isinstance(resolution, dict):
                r_lat = resolution.get("latitude", None)
                r_lon = resolution.get("longitude", None)

                if r_lat is not None and r_lon is not None:
                    data = (
                        copy_self.assign(
                            latitude=lambda x: (
                                (r_lat * x.latitude).round() / r_lat
                            ),
                            longitude=lambda x: (
                                (r_lon * x.longitude).round() / r_lon
                            ),
                        )
                        .groupby(["latitude", "longitude"])
                        .agg(dict(wind_u="mean", wind_v="mean"))
                        .reset_index()
                    )

        return ax.barbs(  # type: ignore
            data.longitude.to_numpy(),
            data.latitude.to_numpy(),
            data.wind_u.to_numpy(),
            data.wind_v.to_numpy(),
            **kwargs,
        )

    # -- Distances --

    def bearing(self, other: PointLike, column_name: str = "bearing") -> Flight:
        # temporary, should implement full stuff
        size = self.data.shape[0]
        return self.assign(
            **{
                column_name: geo.bearing(
                    self.data.latitude.to_numpy(),
                    self.data.longitude.to_numpy(),
                    (other.latitude * np.ones(size)).astype(np.float64),
                    (other.longitude * np.ones(size)).astype(np.float64),
                )
                % 360
            }
        )

    @overload
    def distance(
        self, other: None = None, column_name: str = "distance"
    ) -> float: ...

    @overload
    def distance(
        self,
        other: Union["Airspace", Polygon, PointLike],
        column_name: str = "distance",
    ) -> Flight: ...

    @overload
    def distance(
        self, other: Flight, column_name: str = "distance"
    ) -> Optional[pd.DataFrame]: ...

    @impunity(ignore_warnings=True)
    def distance(
        self,
        other: Union[None, "Flight", "Airspace", Polygon, PointLike] = None,
        column_name: str = "distance",
    ) -> Union[None, float, "Flight", pd.DataFrame]:
        """Computes the distance from a Flight to another entity.

        The behaviour is different according to the type of the second
        element:

        - if the other element is None (i.e. flight.distance()), the method
          returns a distance in nautical miles between the first and last
          recorded positions in the DataFrame.

        - if the other element is a Flight, the method returns a pandas
          DataFrame with corresponding data from both flights, aligned
          with their timestamps, and two new columns with `lateral` and
          `vertical` distances (resp. in nm and ft) separating them.

        - otherwise, the same Flight is returned enriched with a new
          column (by default, named "distance") with the distance of each
          point of the trajectory to the geometrical element.

        .. warning::

            - An Airspace is (currently) considered as its flattened
              representation
            - Computing a distance to a polygon is quite slow at the moment.
              Consider a strict resampling (e.g. one point per minute, "1 min")
              before calling the method.

        """

        if other is None:
            with_position = self.query("latitude.notnull()")
            if with_position is None:
                return 0
            first = with_position.at_ratio(0)
            last = with_position.at_ratio(1)
            if first is None or last is None:
                return 0
            result: tt.distance = geo.distance(
                first.latitude,
                first.longitude,
                last.latitude,
                last.longitude,
            )
            return result

        distance_vec: tt.distance_array

        if isinstance(other, PointLike):
            size = self.data.shape[0]
            distance_vec = geo.distance(
                self.data.latitude.to_numpy(),
                self.data.longitude.to_numpy(),
                (other.latitude * np.ones(size)).astype(np.float64),
                (other.longitude * np.ones(size)).astype(np.float64),
            )
            return self.assign(**{column_name: distance_vec})

        from .airspace import Airspace

        if isinstance(other, Airspace):
            other = other.flatten()

        if isinstance(other, Polygon):
            bounds = other.bounds

            projection = pyproj.Proj(
                proj="aea",  # equivalent projection
                lat_1=bounds[1],
                lat_2=bounds[3],
                lat_0=(bounds[1] + bounds[3]) / 2,
                lon_0=(bounds[0] + bounds[2]) / 2,
            )

            transformer = pyproj.Transformer.from_proj(
                pyproj.Proj("epsg:4326"), projection, always_xy=True
            )
            projected_shape = transform(transformer.transform, other)

            self_xy = self.compute_xy(projection)

            return self.assign(
                **{
                    column_name: list(
                        projected_shape.exterior.distance(p)
                        * (-1 if projected_shape.contains(p) else 1)
                        for p in MultiPoint(
                            list(zip(self_xy.data.x, self_xy.data.y))
                        ).geoms
                    )
                }
            )

        assert isinstance(other, Flight)
        start = max(self.start, other.start)
        stop = min(self.stop, other.stop)
        f1, f2 = (self.between(start, stop), other.between(start, stop))
        if f1 is None or f2 is None:
            return None

        cols = ["timestamp", "latitude", "longitude", "altitude", "icao24"]
        if "callsign" in f1.data.columns:
            cols.append("callsign")
        if "flight_id" in f1.data.columns:
            cols.append("flight_id")
        table = f1.data[cols].merge(f2.data[cols], on="timestamp")

        distance_vec = geo.distance(
            table.latitude_x.to_numpy(),
            table.longitude_x.to_numpy(),
            table.latitude_y.to_numpy(),
            table.longitude_y.to_numpy(),
        )
        return table.assign(
            lateral=distance_vec,
            vertical=(table.altitude_x - table.altitude_y).abs(),
        )

    def closest_point(self, points: List[PointLike] | PointLike) -> pd.Series:
        """Selects the closest point of the trajectory with respect to
        a point or list of points.

        The pd.Series returned by the function is enriched with two fields:
        distance (in meters) and point (containing the name of the closest
        point to the trajectory)

        Example usage:

        .. code:: python

            >>> item = belevingsvlucht.between(
            ...     "2018-05-30 16:00", "2018-05-30 17:00"
            ... ).closest_point(  # type: ignore
            ...     [
            ...         airports["EHLE"],  # type: ignore
            ...         airports["EHAM"],  # type: ignore
            ...         navaids["NARAK"],  # type: ignore
            ...     ]
            ... )
            >>> f"{item.point}, {item.distance:.2f}m"
            "Lelystad Airport, 49.11m"

        """
        from .distance import closest_point

        if not isinstance(points, list):
            points = [points]

        return min(
            (closest_point(self.data, point) for point in points),
            key=attrgetter("distance"),
        )

    def compute_DME_NSE(
        self,
        dme: "Navaids" | Tuple["Navaid", "Navaid"],
        column_name: str = "NSE",
    ) -> Flight:
        """Adds the DME/DME Navigation System Error.

        Computes the max Navigation System Error using DME-DME navigation. The
        obtained NSE value corresponds to the 2 :math:`\\sigma` (95%)
        requirement in nautical miles.

        Source: EUROCONTROL Guidelines for RNAV 1 Infrastructure Assessment

        :param dme:

            - when the parameter is of type Navaids, only the pair of Navaid
              giving the smallest NSE are used;
            - when the parameter is of type tuple, the NSE is computed using
              only the pair of specified Navaid.

        :param column_name: (default: ``"NSE"``), the name of the new column
            containing the computed NSE

        """

        from ..data.basic.navaid import Navaids

        sigma_dme_1_sis = sigma_dme_2_sis = 0.05

        def sigma_air(df: pd.DataFrame, column_name: str) -> Any:
            values = df[column_name] * 0.125 / 100
            return np.where(values < 0.085, 0.085, values)

        def angle_from_bearings_deg(
            bearing_1: float, bearing_2: float
        ) -> float:
            # Returns the subtended given by 2 bearings.
            angle = np.abs(bearing_1 - bearing_2)
            return np.where(angle > 180, 360 - angle, angle)  # type: ignore

        if isinstance(dme, Navaids):
            flight = reduce(
                lambda flight, dme_pair: flight.compute_DME_NSE(
                    dme_pair, f"nse_{dme_pair[0].name}_{dme_pair[1].name}"
                ),
                combinations(dme, 2),
                self,
            )
            nse_colnames = list(
                column
                for column in flight.data.columns
                if column.startswith("nse_")
            )
            return (
                flight.assign(
                    NSE=lambda df: df[nse_colnames].min(axis=1),
                    NSE_idx=lambda df: df[nse_colnames].idxmin(axis=1).str[4:],
                )
                .rename(
                    columns=dict(
                        NSE=column_name,
                        NSE_idx=f"{column_name}_idx",
                    )
                )
                .drop(columns=nse_colnames)
            )

        dme1, dme2 = dme
        extra_cols = [
            "b1",
            "b2",
            "d1",
            "d2",
            "sigma_dme_1_air",
            "sigma_dme_2_air",
            "angle",
        ]

        return (
            self.distance(dme1, "d1")
            .bearing(dme1, "b1")
            .distance(dme2, "d2")
            .bearing(dme2, "b2")
            .assign(angle=lambda df: angle_from_bearings_deg(df.b1, df.b2))
            .assign(
                angle=lambda df: np.where(
                    (df.angle >= 30) & (df.angle <= 150), df.angle, np.nan
                )
            )
            .assign(
                sigma_dme_1_air=lambda df: sigma_air(df, "d1"),
                sigma_dme_2_air=lambda df: sigma_air(df, "d2"),
                NSE=lambda df: (
                    2
                    * np.sqrt(
                        df.sigma_dme_1_air**2
                        + df.sigma_dme_2_air**2
                        + sigma_dme_1_sis**2
                        + sigma_dme_2_sis**2
                    )
                )
                / np.sin(np.deg2rad(df.angle)),
            )
            .drop(columns=extra_cols)
            .rename(columns=dict(NSE=column_name))
        )

    @impunity(ignore_warnings=True)
    def cumulative_distance(
        self,
        compute_gs: bool = True,
        compute_track: bool = True,
        *,
        reverse: bool = False,
        **kwargs: Any,
    ) -> "Flight":
        """Enrich the structure with new ``cumdist`` column computed from
        latitude and longitude columns.

        The first ``cumdist`` value is 0, then distances are computed (in
        **nautical miles**) and summed between consecutive positions. The last
        value is the total length of the trajectory.

        When the ``compute_gs`` flag is set to True (default), an additional
        ``compute_gs`` is also added. This value can be compared with the
        decoded ``groundspeed`` value in ADSB messages.

        When the ``compute_track`` flag is set to True (default), an additional
        ``compute_track`` is also added. This value can be compared with the
        decoded ``track`` value in ADSB messages.

        """

        if "compute_groundspeed" in kwargs:
            warnings.warn("Use compute_gs argument", DeprecationWarning)
            compute_gs = kwargs["compute_groundspeed"]

        cur_sorted = self.sort_values("timestamp", ascending=not reverse)
        coords = cur_sorted.data[["timestamp", "latitude", "longitude"]]

        delta = pd.concat([coords, coords.add_suffix("_1").diff()], axis=1)
        delta_1 = delta.iloc[1:]
        distance_nm: tt.distance_array
        distance_nm = geo.distance(
            (delta_1.latitude - delta_1.latitude_1).to_numpy(),
            (delta_1.longitude - delta_1.longitude_1).to_numpy(),
            delta_1.latitude.to_numpy(),
            delta_1.longitude.to_numpy(),
        )

        res = cur_sorted.assign(
            cumdist=np.pad(distance_nm.cumsum(), (1, 0), "constant")
        )

        if compute_gs:
            secs: tt.seconds_array = delta_1.timestamp_1.dt.total_seconds()
            groundspeed: tt.speed_array = distance_nm / secs
            res = res.assign(
                compute_gs=np.abs(np.pad(groundspeed, (1, 0), "edge"))
            )

        if compute_track:
            track = geo.bearing(
                (delta_1.latitude - delta_1.latitude_1).to_numpy(),
                (delta_1.longitude - delta_1.longitude_1).to_numpy(),
                delta_1.latitude.to_numpy(),
                delta_1.longitude.to_numpy(),
            )
            track = np.where(track > 0, track, 360 + track)
            res = res.assign(
                compute_track=np.abs(np.pad(track, (1, 0), "edge"))
            )

        return res.sort_values("timestamp", ascending=True)

    # -- Geometry operations --

    @property
    def linestring(self) -> Optional[LineString]:
        # longitude is implicit I guess
        if "latitude" not in self.data.columns:
            return None
        coords = list(self.coords)
        if len(coords) < 2:
            return None
        return LineString(coords)

    @property
    def shape(self) -> Optional[LineString]:
        return self.linestring

    @property
    def point(self) -> Optional[Position]:
        return self.at()

    def simplify(
        self,
        tolerance: float,
        altitude: Optional[str] = None,
        z_factor: float = 3.048,
    ) -> Flight:
        """Simplifies a trajectory with Douglas-Peucker algorithm.

        The method uses latitude and longitude, projects the trajectory to a
        conformal projection and applies the algorithm. If x and y features are
        already present in the DataFrame (after a call to :meth:`compute_xy`
        for instance) then this projection is taken into account.

        The tolerance parameter must be defined in meters.

        - By default, a 2D version of the algorithm is called, unless you pass a
          column name for ``altitude``.
        - You may scale the z-axis for more relevance (``z_factor``). The
          default value works well in most situations.

        The method returns a :class:`~traffic.core.Flight` or a 1D mask if you
        specify ``return_mask=True``.

        **See also**: :ref:`How to simplify or resample a trajectory?`

        """

        if "x" in self.data.columns and "y" in self.data.columns:
            kwargs = dict(x="x", y="y")
        else:
            kwargs = dict(lat="latitude", lon="longitude")

        mask = douglas_peucker(
            df=self.data,
            tolerance=tolerance,
            z=altitude,
            z_factor=z_factor,
            **kwargs,
        )

        return self.__class__(self.data.loc[mask])

    def intersects(  # type: ignore
        self,
        shape: Union[ShapelyMixin, base.BaseGeometry],
    ) -> bool:
        # implemented and monkey-patched in airspace.py
        # given here for consistency in types
        ...

    @flight_iterator
    def clip_iterate(
        self, shape: Union[ShapelyMixin, base.BaseGeometry], strict: bool = True
    ) -> Iterator["Flight"]:
        list_coords = list(self.xy_time)
        if len(list_coords) < 2:
            return None

        linestring = LineString(list_coords)
        if not isinstance(shape, base.BaseGeometry):
            shape = shape.shape

        intersection = linestring.intersection(shape)

        if intersection.is_empty:
            return None

        if isinstance(intersection, Point):
            return None

        if isinstance(intersection, LineString):
            time_list = list(
                datetime.fromtimestamp(t, timezone.utc)
                for t in np.stack(intersection.coords)[:, 2]
            )
            between = self.between(
                min(time_list), max(time_list), strict=strict
            )
            if between is not None:
                yield between
            return None

        def _clip_generator() -> Iterable[Tuple[datetime, datetime]]:
            for segment in intersection.geoms:
                times: List[datetime] = list(
                    datetime.fromtimestamp(t, timezone.utc)
                    for t in np.stack(segment.coords)[:, 2]
                )
                yield min(times), max(times)

        # it is actually not so simple because of self intersecting trajectories
        prev_t1, prev_t2 = None, None

        for t1, t2 in _clip_generator():
            if prev_t2 is not None and t1 > prev_t2:
                between = self.between(prev_t1, prev_t2, strict=strict)
                if between is not None:
                    yield between
                prev_t1, prev_t2 = t1, t2
            elif prev_t2 is None:
                prev_t1, prev_t2 = t1, t2
            else:
                prev_t1, prev_t2 = min(prev_t1, t1), max(prev_t2, t2)  # type: ignore

        if prev_t2 is not None:
            between = self.between(prev_t1, prev_t2, strict=strict)
            if between is not None:
                yield between

    def clip(
        self, shape: Union[ShapelyMixin, base.BaseGeometry], strict: bool = True
    ) -> Optional[Flight]:
        """Clips the trajectory to a given shape.

        For a shapely Geometry, the first time of entry and the last time of
        exit are first computed before returning the part of the trajectory
        between the two timestamps.

        Most of the time, aircraft do not repeatedly come out and in an
        airspace, but computation errors may sometimes give this impression.
        As a consequence, the clipped trajectory may have points outside the
        shape.

        .. warning::

            Altitudes are not taken into account.

        """

        t1 = None
        for segment in self.clip_iterate(shape, strict=strict):
            if t1 is None:
                t1 = segment.start
            t2 = segment.stop

        if t1 is None:
            return None

        clipped_flight = self.between(t1, t2, strict=strict)

        if clipped_flight is None or clipped_flight.shape is None:
            return None

        return clipped_flight

    def query_opensky(self, **kwargs: Any) -> Optional[Flight]:
        """Returns data from the same Flight as stored in OpenSky database.

        This may be useful if you write your own parser for data from a
        different channel. The method will use the ``callsign`` and ``icao24``
        attributes to build a request for current Flight in the OpenSky Network
        database.

        The kwargs argument helps overriding arguments from the query, namely
        start, stop, callsign and icao24.

        Returns None if no data is found.

        """

        from ..data import opensky

        query_params = {
            "start": self.start,
            "stop": self.stop,
            "icao24": self.icao24,
            "return_flight": True,
            **kwargs,
        }
        if self.callsign is not None:
            query_params["callsign"] = self.callsign
        return cast(Optional[Flight], opensky.history(**query_params))

    def query_ehs(
        self,
        data: Union[None, pd.DataFrame, "RawData"] = None,
        failure_mode: str = "info",
        **kwargs: Any,
    ) -> Flight:
        """Extends data with extra columns from EHS messages.

        By default, raw messages are requested from the OpenSky Network
        database.

        .. warning::

            Making a lot of small requests can be very inefficient and may look
            like a denial of service. If you get the raw messages using a
            different channel, you can provide the resulting dataframe as a
            parameter.

        The data parameter expect three columns: ``icao24``, ``rawmsg`` and
        ``mintime``, in conformance with the OpenSky API.

        """

        from ..data import opensky

        if not isinstance(self.icao24, str):
            raise RuntimeError("Several icao24 for this flight")

        if self.callsign is None:
            raise RuntimeError("No callsign for this flight")

        if not isinstance(self.callsign, str):
            raise RuntimeError("Several callsigns for this flight")

        def fail_warning() -> Flight:
            """Called when nothing can be added to data."""
            id_ = self.flight_id
            if id_ is None:
                id_ = self.callsign
            _log.warning(f"No data found on OpenSky database for flight {id_}.")
            return self

        def fail_info() -> Flight:
            """Called when nothing can be added to data."""
            id_ = self.flight_id
            if id_ is None:
                id_ = self.callsign
            _log.info(f"No data found on OpenSky database for flight {id_}.")
            return self

        def fail_silent() -> Flight:
            return self

        failure_dict = dict(
            warning=fail_warning, info=fail_info, silent=fail_silent
        )
        failure = failure_dict[failure_mode]

        if data is None:
            ext = opensky.extended(
                self.start, self.stop, icao24=self.icao24, **kwargs
            )
            df = ext.data if ext is not None else None
        else:
            df = data if isinstance(data, pd.DataFrame) else data.data
            df = df.query(
                "icao24 == @self.icao24 and "
                "@self.start.timestamp() < mintime < @self.stop.timestamp()"
            )

        if df is None or df.shape[0] == 0:
            return failure()

        timestamped_df = df.sort_values("mintime").assign(
            timestamp=lambda df: df.mintime
        )
        timestamp_s = self.data.timestamp.dt.as_unit("s").astype(int)

        referenced_df = (
            timestamped_df.merge(
                self.data.assign(timestamp=timestamp_s),
                on="timestamp",
                how="outer",
            )
            .sort_values("timestamp")
            .rename(
                columns=dict(
                    altitude="alt",
                    altitude_y="alt",
                    groundspeed="spd",
                    track="trk",
                )
            )[["timestamp", "latitude", "longitude", "alt", "spd", "trk"]]
            .ffill()
            .drop_duplicates()  # bugfix! NEVER ERASE THAT LINE!
            .merge(
                timestamped_df[["timestamp", "icao24", "rawmsg"]],
                on="timestamp",
                how="right",
            )
        )

        decoded = rs1090.decode(
            referenced_df.rawmsg,
            referenced_df.timestamp.astype("int64"),
        )

        if len(decoded) == 0:
            return failure()

        df = pd.concat(
            # 5000 is a good batch size for fast loading!
            pd.DataFrame.from_records(d)
            for d in rs1090.batched(decoded, 5000)
        )
        df = df.convert_dtypes(dtype_backend="pyarrow").assign(
            timestamp=pd.to_datetime(df.timestamp, unit="s", utc=True)
        )
        extended = Flight(df)

        # fix for https://stackoverflow.com/q/53657210/1595335
        if "last_position" in self.data.columns:
            extended = extended.assign(last_position=pd.NaT)
        if "start" in self.data.columns:
            extended = extended.assign(start=pd.NaT)
        if "stop" in self.data.columns:
            extended = extended.assign(stop=pd.NaT)

        aggregate = extended + self
        if "flight_id" in self.data.columns:
            aggregate.data.flight_id = self.flight_id

        # sometimes weird callsigns are decoded and should be discarded
        # so it seems better to filter on callsign rather than on icao24
        flight = aggregate[self.icao24]
        if flight is None:
            return failure()

        if self.callsign is not None:
            flight = flight.assign(callsign=self.callsign)
        if self.number is not None:
            flight = flight.assign(number=self.number)
        if self.origin is not None:
            flight = flight.assign(origin=self.origin)
        if self.destination is not None:
            flight = flight.assign(destination=self.destination)

        return flight.sort_values("timestamp")

    # -- Visualisation --

    def plot(
        self, ax: "GeoAxes", **kwargs: Any
    ) -> List["Artist"]:  # coverage: ignore
        """Plots the trajectory on a Matplotlib axis.

        The Flight supports Cartopy axis as well with automatic projection. If
        no projection is provided, a default `PlateCarree
        <https://scitools.org.uk/cartopy/docs/v0.15/crs/projections.html#platecarree>`_
        is applied.

        Example usage:

        .. code:: python

            from cartes.crs import Mercator
            fig, ax = plt.subplots(1, subplot_kw=dict(projection=Mercator())
            flight.plot(ax, alpha=.5)


        .. note::

            See also :meth:`geoencode` for the altair equivalent.

        """

        from cartopy.crs import PlateCarree

        if "projection" in ax.__dict__ and "transform" not in kwargs:
            kwargs["transform"] = PlateCarree()
        if self.shape is not None:
            return ax.plot(*self.shape.xy, **kwargs)  # type: ignore
        return []

    def chart(self, *features: str) -> "alt.Chart":  # coverage: ignore
        """
        Initializes an altair Chart based on Flight data.

        The features passed in parameters are dispatched to allow plotting
        multiple features on the same graph.

        Example usage:

        .. code:: python

            # Most simple usage
            flight.chart().encode(alt.Y("altitude"))

            # With some configuration
            flight.chart().encode(
                alt.X(
                    "utcyearmonthdatehoursminutes(timestamp)",
                    axis=alt.Axis(title=None, format="%H:%M"),
                ),
                alt.Y("altitude", title="altitude (in ft)"),
                alt.Color("callsign")
            )

        For a more complex graph plotting similar physical quantities on the
        same graph, and other quantities on a different graph, the following
        snippet may be of use.

        .. code:: python

            # More advanced with several plots on the same graph
            base = (
                flight.chart("altitude", "groundspeed", "IAS")
                .encode(
                    alt.X(
                        "utcyearmonthdatehoursminutesseconds(timestamp)",
                        axis=alt.Axis(title=None, format="%H:%M"),
                    )
                )
                .properties(height=200)
            )

            alt.vconcat(
                base.transform_filter('datum.variable != "altitude"').encode(
                    alt.Y(
                        "value:Q",
                        axis=alt.Axis(title="speed (in kts)"),
                        scale=alt.Scale(zero=False),
                    )
                ),
                base.transform_filter('datum.variable == "altitude"').encode(
                    alt.Y("value:Q", title="altitude (in ft)")
                ),
            )


        .. note::

            See also :meth:`plot_time` for the Matplotlib equivalent.

        """
        import altair as alt

        base = alt.Chart(self.data).encode(
            alt.X("utcyearmonthdatehoursminutesseconds(timestamp)"),
        )
        if len(features) > 0:
            base = base.transform_fold(
                list(features), as_=["variable", "value"]
            ).encode(alt.Y("value:Q"), alt.Color("variable:N"))

        return base.mark_line()  # type: ignore

    # -- Visualize with Leaflet --

    def leaflet(self, **kwargs: Any) -> "Optional[LeafletPolyline]":
        raise ImportError(
            "Install ipyleaflet or traffic with the leaflet extension"
        )

    def map_leaflet(
        self,
        *,
        zoom: int = 7,
        highlight: Optional[
            Dict[
                str,
                Union[
                    str,
                    Flight,
                    Callable[[Flight], None | Flight | Iterable[Flight]],
                ],
            ]
        ] = None,
        airport: Union[None, str, Airport] = None,
        **kwargs: Any,
    ) -> "Optional[LeafletMap]":
        raise ImportError(
            "Install ipyleaflet or traffic with the leaflet extension"
        )

    # -- Visualize with Plotly --

    def line_geo(self, **kwargs: Any) -> "go.Figure":
        raise ImportError("Install plotly or traffic with the plotly extension")

    def line_map(
        self, map_style: str = "carto-positron", **kwargs: Any
    ) -> "go.Figure":
        raise ImportError("Install plotly or traffic with the plotly extension")

    def scatter_geo(self, **kwargs: Any) -> "go.Figure":
        raise ImportError("Install plotly or traffic with the plotly extension")

    def scatter_map(
        self, map_style: str = "carto-positron", **kwargs: Any
    ) -> "go.Figure":
        raise ImportError("Install plotly or traffic with the plotly extension")

    def plot_time(
        self,
        y: Union[str, List[str]],
        ax: Union[None, "Axes"] = None,
        secondary_y: Union[None, str, List[str]] = None,
        **kwargs: Any,
    ) -> None:  # coverage: ignore
        """Plots the given features according to time.

        The method ensures:

        - only non-NaN data are displayed (no gap in the plot);
        - the timestamp is naively converted to UTC if not localized.

        Example usage:

        .. code:: python

            ax = plt.axes()
            # most simple version
            flight.plot_time(ax, 'altitude')
            # or with several comparable features and twin axes
            flight.plot_time(
                ax, ['altitude', 'groundspeed', 'IAS', 'TAS'],
                secondary_y=['altitude']
            )

        .. note::

            See also :meth:`chart` for the altair equivalent.

        """
        if isinstance(y, str):
            y = [y]
        if isinstance(secondary_y, str):
            secondary_y = [secondary_y]
        if secondary_y is None:
            secondary_y = []

        if ax is None:
            import matplotlib.pyplot as plt

            ax = plt.gca()

        localized = self.data.timestamp.dt.tz is not None
        for column in y:
            kw = {
                **kwargs,
                **dict(
                    y=column,
                    secondary_y=column if column in secondary_y else "",
                ),
            }
            subtab = self.data.query(f"{column}.notnull()")

            if localized:
                (
                    subtab.assign(
                        timestamp=lambda df: df.timestamp.dt.tz_convert("utc")
                    ).plot(ax=ax, x="timestamp", **kw)
                )
            else:
                (
                    subtab.assign(
                        timestamp=lambda df: df.timestamp.dt.tz_localize(
                            datetime.now(tz=None).astimezone().tzinfo
                        ).dt.tz_convert("utc")
                    ).plot(ax=ax, x="timestamp", **kw)
                )

    @classmethod
    def from_fr24(cls, filename: Union[Path, str]) -> Flight:
        from ..data.datasets.flightradar24 import FlightRadar24

        return FlightRadar24.from_file(filename)

    @classmethod
    def from_readsb(cls, filename: Union[Path, str]) -> Self:
        """Parses data in readsb format.

        Reference:
        https://github.com/wiedehopf/readsb/blob/dev/README-json.md#trace-jsons

        :param filename: a json file with ADSB traces
        :return: a regular Flight object.
        """

        # TODO to be improved later, but good enough for the test for now
        trace_columns = [
            "seconds_after_timestamp",
            "latitude",
            "longitude",
            "altitude",
            "groundspeed",
            "track",
            "flags",
            "vertical_rate",
            "aircraft",
            "type",
            "geometric_altitude",
            "geometric_vrate",
            "ias",
            "roll",
        ]

        readsb_data = pd.read_json(filename).rename(columns={"icao": "icao24"})
        trace_data = pd.DataFrame.from_records(
            readsb_data.trace, columns=trace_columns
        )
        aircraft_data = pd.json_normalize(trace_data.aircraft)

        readsb_data = (
            readsb_data.assign(
                position_time_utc=readsb_data.timestamp
                + trace_data["seconds_after_timestamp"].map(
                    lambda s: timedelta(seconds=s)
                )
            )
            .drop(columns=["timestamp", "trace"])
            .join(trace_data.drop(columns=["seconds_after_timestamp"]))
            .rename(columns={"position_time_utc": "timestamp"})
            .join(
                aircraft_data.rename(
                    columns={c: "aircraft_" + c for c in aircraft_data.columns}
                )
            )
        )

        return cls(readsb_data)

    @classmethod
    def from_file(cls, filename: Union[Path, str], **kwargs: Any) -> Self:
        """Read data from various formats.

        This class method dispatches the loading of data in various format to
        the proper ``pandas.read_*`` method based on the extension of the
        filename.

        - .pkl and .pkl.gz dispatch to ``pandas.read_pickle``;
        - .parquet and .parquet.gz dispatch to ``pandas.read_parquet``;
        - .json and .json.gz dispatch to ``pandas.read_json``;
        - .csv and .csv.gz dispatch to ``pandas.read_csv``;
        - .h5 dispatch to ``pandas.read_hdf``.

        Other extensions return ``None``.
        Specific arguments may be passed to the underlying ``pandas.read_*``
        method with the kwargs argument.

        Example usage:

        >>> from traffic.core import Flight
        >>> t = Flight.from_file("example_flight.csv")
        """

        tentative = super().from_file(filename, **kwargs)

        # Special treatment for flights to download from flightradar24
        cols_fr24 = {
            "Altitude",
            "Callsign",
            "Direction",
            "Position",
            "Speed",
            "Timestamp",
            "UTC",
        }
        if set(tentative.data.columns) != cols_fr24:
            return tentative

        latlon = tentative.data.Position.str.split(pat=",", expand=True)
        return (
            tentative.assign(
                latitude=latlon[0].astype(float),
                longitude=latlon[1].astype(float),
                timestamp=lambda df: pd.to_datetime(df.UTC),
            )
            .rename(
                columns={
                    "UTC": "timestamp",
                    "Altitude": "altitude",
                    "Callsign": "callsign",
                    "Speed": "groundspeed",
                    "Direction": "track",
                }
            )
            .drop(columns=["Timestamp", "Position"])
        )


def patch_plotly() -> None:
    from ..visualize.plotly import (
        Scattergeo,
        Scattermap,
        line_geo,
        line_map,
        scatter_geo,
        scatter_map,
    )

    Flight.line_map = line_map  # type: ignore
    Flight.scatter_map = scatter_map  # type: ignore
    Flight.Scattermap = Scattermap  # type: ignore
    Flight.line_geo = line_geo  # type: ignore
    Flight.scatter_geo = scatter_geo  # type: ignore
    Flight.Scattergeo = Scattergeo  # type: ignore


try:
    patch_plotly()
except Exception:
    pass


def patch_leaflet() -> None:
    from ..visualize.leaflet import flight_leaflet, flight_map_leaflet

    Flight.leaflet = flight_leaflet  # type: ignore
    Flight.map_leaflet = flight_map_leaflet  # type: ignore


try:
    patch_leaflet()
except Exception:
    pass
