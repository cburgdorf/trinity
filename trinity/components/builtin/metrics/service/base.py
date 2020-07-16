import time
from abc import abstractmethod
from http.client import HTTPException

from async_service import Service
from pyformance.reporters import InfluxReporter

from trinity.components.builtin.metrics.abc import MetricsServiceAPI
from trinity.components.builtin.metrics.registry import HostMetricsRegistry
from trinity._utils.logging import get_logger


class BaseMetricsService(Service, MetricsServiceAPI):
    """
    A service to provide a registry where metrics instruments can be registered and retrieved from.
    It continuously reports metrics to the specified InfluxDB instance.
    """

    MIN_SECONDS_BETWEEN_REPORTED_ERRORS = 60

    def __init__(self,
                 influx_server: str,
                 influx_user: str,
                 influx_password: str,
                 influx_database: str,
                 host: str,
                 port: int,
                 protocol: str,
                 reporting_frequency: int):
        self._last_error_at: float = None
        self._influx_server = influx_server
        self._reporting_frequency = reporting_frequency
        self._registry = HostMetricsRegistry(host)
        self._reporter = InfluxReporter(
            registry=self._registry,
            database=influx_database,
            username=influx_user,
            password=influx_password,
            protocol=protocol,
            port=port,
            server=influx_server
        )

    logger = get_logger('trinity.components.builtin.metrics.MetricsService')

    @property
    def registry(self) -> HostMetricsRegistry:
        """
        Return the :class:`trinity.components.builtin.metrics.registry.HostMetricsRegistry` at which
        metrics instruments can be registered and retrieved.
        """
        return self._registry

    async def run(self) -> None:
        self.logger.info("Reporting metrics to %s", self._influx_server)
        self.manager.run_daemon_task(self.continuously_report)
        await self.manager.wait_finished()

    async def report_now(self) -> None:
        try:
            self._reporter.report_now()
        except (HTTPException, ConnectionError) as exc:

            # This method is usually called every few seconds. If there's an issue with the
            # connection we do not want to flood the log and tame down warnings.
            is_justified = self._last_error_at is None or (
                time.monotonic() - self._last_error_at > self.MIN_SECONDS_BETWEEN_REPORTED_ERRORS
            )

            if is_justified:
                self.logger.warning("Unable to report metrics: %s", exc)

    @abstractmethod
    async def continuously_report(self) -> None:
        ...
