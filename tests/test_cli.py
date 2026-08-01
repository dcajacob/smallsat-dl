from argparse import Namespace
import asyncio
from io import StringIO

import pytest
from rich.console import Console

import smallsat_dl.cli as cli
from smallsat_dl.cli import (
    defaults_to_latest,
    normalize_requested_years,
    parse_year_list,
    prompt_for_years,
    selected_years_from_args,
)


def args(**overrides):
    values = {
        "interactive": False,
        "latest": False,
        "years": None,
        "all": False,
        "from_year": None,
        "to_year": None,
    }
    values.update(overrides)
    return Namespace(**values)


def test_latest_selects_newest_available_year():
    assert selected_years_from_args(args(latest=True), [2023, 2024, 2025]) == [2025]


def test_repeated_years_are_normalized_newest_first():
    assert selected_years_from_args(args(years=[2024, 2023, 2024]), [2023, 2024, 2025]) == [
        2024,
        2023,
    ]


def test_range_can_be_supplied_in_either_order():
    assert selected_years_from_args(
        args(from_year=2025, to_year=2023),
        [2022, 2023, 2024, 2025],
    ) == [2025, 2024, 2023]


def test_no_selector_defaults_to_latest():
    assert defaults_to_latest(args())
    assert selected_years_from_args(args(), [2024, 2025]) is None


def test_interactive_disables_latest_default():
    assert not defaults_to_latest(args(interactive=True))


def test_multiple_selectors_are_rejected():
    with pytest.raises(ValueError, match="choose only one"):
        selected_years_from_args(args(latest=True, years=[2025]), [2025])

    with pytest.raises(ValueError, match="choose only one"):
        selected_years_from_args(args(interactive=True, latest=True), [2025])


def test_unavailable_years_are_rejected():
    with pytest.raises(ValueError, match="2022"):
        normalize_requested_years([2022, 2025], [2024, 2025])


def test_parse_year_list_accepts_commas_and_spaces():
    assert parse_year_list("2025, 2024 2023") == [2025, 2024, 2023]


def test_interactive_latest_uses_latest_year_with_pdf(monkeypatch):
    monkeypatch.setattr("builtins.input", lambda _: "")

    assert prompt_for_years([2024, 2025, 2026], latest_year_with_pdf=2025) == [2025]


def test_archive_search_prints_message_before_operation(monkeypatch):
    output = StringIO()
    monkeypatch.setattr(cli, "console", Console(file=output, force_terminal=False))

    async def operation():
        return 2025

    assert asyncio.run(cli.search_archive("Searching now...", operation)) == 2025
    assert "Searching now..." in output.getvalue()
