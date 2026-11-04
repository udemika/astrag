"""AstraG primary backend entrypoint."""
from astra_clone_backend import *  # noqa: F401,F403
from astra_clone_backend import app, main

if __name__ == "__main__":
    raise SystemExit(main())
