"""Start one Sparkless server in the controller and pass its URL to workers."""

import pytest
from pyspark.sql import SparkSession
from sparkless_server import SparklessServer

server_key = pytest.StashKey[SparklessServer]()


def pytest_configure(config):
    if not hasattr(config, "workerinput"):
        server = SparklessServer()
        config.add_cleanup(server.stop)
        config.stash[server_key] = server.start()


@pytest.hookimpl(optionalhook=True)
def pytest_configure_node(node):
    node.workerinput["sparkless_url"] = node.config.stash[server_key].spark_connect_url


@pytest.fixture(scope="session")
def spark(pytestconfig):
    if hasattr(pytestconfig, "workerinput"):
        url = pytestconfig.workerinput["sparkless_url"]
    else:
        url = pytestconfig.stash[server_key].spark_connect_url
    session = SparkSession.builder.remote(url).create()
    try:
        yield session
    finally:
        session.stop()
