"""Test config for pytest-asyncio."""
import pytest


def pytest_collection_modifyitems(config, items):
    # Mark every async test with asyncio so we don't need decorators everywhere.
    for item in items:
        if "asyncio" in item.keywords:
            continue
        if "async def" in (item.function.__code__.co_consts.__repr__() if hasattr(item, "function") else ""):
            item.add_marker(pytest.mark.asyncio)
