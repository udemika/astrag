"""AstraG server entrypoint."""
import argparse
from astra_clone_backend import HOST, PORT, app

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AstraG media & control server")
    parser.add_argument("-p", "--port", type=int, default=PORT, help="Primary HTTP/IPTV port (default: %(default)s)")
    parser.add_argument("--host", default=HOST, help="Bind address (default: %(default)s)")
    args, _ = parser.parse_known_args()
    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port, loop="asyncio")
