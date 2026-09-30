import numpy as np

from postgres_helpers import clean_row


def test_clean_row_converts_nan_to_none():
    row = (2023, 2, "UNSPECIFIED", float("nan"), 5)
    result = clean_row(row)
    assert result[3] is None


def test_clean_row_converts_numpy_nan_to_none():
    row = (np.float64("nan"), "BRONX")
    result = clean_row(row)
    assert result[0] is None


def test_clean_row_preserves_valid_values():
    row = (2023, 1, "BRONX", 357.19, 24060)
    assert clean_row(row) == row


def test_clean_row_preserves_zero_and_empty_string():
    row = (0, 0.0, "")
    assert clean_row(row) == (0, 0.0, "")


def test_clean_row_preserves_existing_none():
    row = (None, "BRONX")
    assert clean_row(row) == (None, "BRONX")