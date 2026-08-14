""" The dialog for building one rule.

Split three ways, because the single file that held all of it mixed three
concerns that change for different reasons:

- `payloads.py`       — form output -> the dict the domain stores. No I/O, no
                        lookups; everything it needs is already in the payload.
- `field_builders.py` — which fields a type shows and what they start out
                        holding. These *do* hit the network, which is why the
                        dialog defers them.
- `modal.py`          — the dialog itself: opening, deferring, and swapping the
                        fields out when the chosen type changes.
"""

from .modal import FilterModal, filter_type_options_for

__all__ = ["FilterModal", "filter_type_options_for"]
