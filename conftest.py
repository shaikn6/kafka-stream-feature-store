"""Root conftest — configure pytest-asyncio mode."""
import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "asyncio: mark test as async"
    )
