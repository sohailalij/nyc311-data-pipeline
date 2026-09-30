import numpy as np


def clean_row(row):
    """Convert Pandas/NumPy NaN (from Spark NULL) into a proper Python None,
    so psycopg2 inserts a SQL NULL instead of the literal string 'NaN'."""
    return tuple(
        None if (isinstance(v, (float, np.floating)) and np.isnan(v)) else v
        for v in row
    )