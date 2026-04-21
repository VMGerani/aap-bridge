"""Tests for inventory foreign-key normalization."""

import pytest

from aap_migration.utils.inventory_fk import (
    ensure_credential_id_on_inventory_source,
    ensure_inventory_id_on_inventory_source,
    normalize_input_inventories_to_source_ids,
    parse_credential_id_from_api_value,
    parse_inventory_id_from_api_value,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (42, 42),
        ("/api/v2/inventories/7/", 7),
        ("/api/controller/v2/inventories/99/", 99),
        ({"id": 3}, 3),
        ({"url": "https://x/api/v2/inventories/12/"}, 12),
    ],
)
def test_parse_inventory_id_from_api_value(value, expected):
    assert parse_inventory_id_from_api_value(value) == expected


def test_ensure_fills_from_related():
    data = {
        "related": {"inventory": "/api/v2/inventories/5/"},
    }
    ensure_inventory_id_on_inventory_source(data)
    assert data["inventory"] == 5


def test_ensure_fills_from_related_dict():
    data = {"related": {"inventory": {"id": 44, "name": "Inv"}}}
    ensure_inventory_id_on_inventory_source(data)
    assert data["inventory"] == 44


def test_ensure_prefers_top_level_int():
    data = {"inventory": 9}
    ensure_inventory_id_on_inventory_source(data)
    assert data["inventory"] == 9


def test_normalize_input_inventories_mixed_and_dedupes():
    assert normalize_input_inventories_to_source_ids(
        [1, {"id": 2}, "/api/v2/inventories/3/", 1]
    ) == [1, 2, 3]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (10, 10),
        ("/api/v2/credentials/10/", 10),
        ("/api/controller/v2/credentials/10/", 10),
        ({"id": 10}, 10),
        ({"url": "https://x/api/v2/credentials/12/"}, 12),
    ],
)
def test_parse_credential_id_from_api_value(value, expected):
    assert parse_credential_id_from_api_value(value) == expected


def test_ensure_credential_fills_from_related():
    data = {
        "related": {"credential": "/api/controller/v2/credentials/10/"},
    }
    ensure_credential_id_on_inventory_source(data)
    assert data["credential"] == 10
