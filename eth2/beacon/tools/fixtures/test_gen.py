import itertools
from typing import Any, Dict, NewType, Optional, Tuple

from eth_utils.toolz import thread_last

from eth2.beacon.tools.fixtures.config_types import ConfigType
from eth2.beacon.tools.fixtures.parser import parse_test_suite
from eth2.beacon.tools.fixtures.test_case import TestCase
from eth2.beacon.tools.fixtures.test_handler import TestHandler
from eth2.beacon.tools.fixtures.test_types import TestType

# NOTE: ``pytest`` does not export the ``Metafunc`` class so we
# make a new type here to stand in for it.
Metafunc = NewType("Metafunc", object)
TestSuiteDescriptor = Tuple[Tuple[TestType, TestHandler], ConfigType]


def pytest_from_eth2_fixture(config: Dict[str, Any]):
    """
    This function attaches the ``config`` to the ``func`` via the
    ``decorator``. The idea here is to just communicate this data to
    later stages of the test generation.
    """

    def decorator(func):
        func.__eth2_fixture_config = config
        return func

    return decorator


def _read_request_from_metafunc(metafunc: Metafunc) -> Dict[str, Any]:
    fn = metafunc.function
    return fn.__eth2_fixture_config


def _generate_test_suite_descriptors_from(
    eth2_fixture_request: Dict[str, Any]
) -> Tuple[TestSuiteDescriptor, ...]:
    if "config_types" in eth2_fixture_request:
        config_types = eth2_fixture_request["config_types"]
        if len(config_types) != 1:
            raise Exception(
                "only run one config type per process, due to overwriting SSZ bounds"
            )
    else:
        config_types = (None,)

    test_types = eth2_fixture_request["test_types"]

    # special case only one handler, "core"
    if not isinstance(test_types, Dict):
        test_types = {
            _type: lambda handler: handler.name == "core" for _type in test_types
        }

    selected_handlers = tuple()
    for test_type, handler_filter in test_types.items():
        for handler in test_type.handlers:
            if handler_filter(handler) or handler.name == "core":
                selected_handler = (test_type, handler)
                selected_handlers += selected_handler
    return itertools.product((selected_handlers,), config_types)


def _generate_pytest_case_from(
    test_type: TestType,
    handler_type: TestHandler,
    config_type: ConfigType,
    test_case: TestCase,
) -> Tuple[TestCase, str]:
    # special case only one handler "core"
    test_name = test_type.name
    if len(test_type.handlers) == 1 or handler_type.name == "core":
        handler_name = ""
    else:
        handler_name = handler_type.name

    if config_type:
        config_name = config_type.name
    else:
        config_name = ""

    test_id_prefix = thread_last(
        (test_name, handler_name, config_name),
        (filter, lambda component: component != ""),
        lambda components: "_".join(components),
    )
    test_id = f"{test_id_prefix}.yaml:{test_case.index}"

    if test_case.description:
        test_id += f":{test_case.description}"
    return test_case, test_id


def _generate_pytest_cases_from_test_suite_descriptors(
    test_suite_descriptors: Tuple[TestSuiteDescriptor, ...]
):
    for (test_type, handler_type), config_type in test_suite_descriptors:
        test_suite = parse_test_suite(test_type, handler_type, config_type)
        for test_case in test_suite:
            yield _generate_pytest_case_from(
                test_type, handler_type, config_type, test_case
            )


def generate_pytests_from_eth2_fixture(metafunc: Metafunc) -> None:
    """
    Generate all the test cases requested by the config (attached to ``metafunc``'s
    function object) and inject them via ``metafunc.parametrize``.
    """
    eth2_fixture_request = _read_request_from_metafunc(metafunc)
    test_suite_descriptors = _generate_test_suite_descriptors_from(eth2_fixture_request)
    pytest_cases = tuple(
        _generate_pytest_cases_from_test_suite_descriptors(test_suite_descriptors)
    )
    if pytest_cases:
        argvals, ids = zip(*pytest_cases)
    else:
        argvals, ids = (), ()

    metafunc.parametrize("test_case", argvals, ids=ids)
