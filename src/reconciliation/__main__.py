"""Entrypoint for `python -m reconciliation`."""
import sys
from reconciliation.cli import main

if __name__ == "__main__":
    sys.exit(main())
