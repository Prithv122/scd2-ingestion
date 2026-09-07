"""scd2-ingestion: order-independent SCD Type 2 dimension ingestion.

Late-arriving and out-of-order change events are handled by the same code path as
ordinary forward-in-time updates -- both are "split the interval that currently covers
this event's effective time." See scd2.py's module docstring for the full argument, and
tests/test_order_independence.py for the randomized proof.
"""

__version__ = "0.1.0"
