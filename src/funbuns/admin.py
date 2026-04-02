"""
Administrative entry point: database management, web server, notebook.

Separates infrastructure concerns from the mathematical analysis in __main__.py.
"""

import argparse
import sys


def main():
    parser = argparse.ArgumentParser(
        description="funbuns infrastructure: database, web server, notebook",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # --- db ---
    db_parser = sub.add_parser("db", help="Database management")
    db_sub = db_parser.add_subparsers(dest="db_action", required=True)

    db_sub.add_parser("build", help="Build DuckDB index from parquet (one-time)")
    db_sub.add_parser("sync", help="Sync DuckDB with new parquet blocks")
    sync_blocks_parser = db_sub.add_parser(
        "sync-blocks", help="Sync specific blocks by number"
    )
    sync_blocks_parser.add_argument(
        "blocks", type=int, nargs="+", metavar="N",
        help="Block numbers to sync (e.g. 999 1000 1001)"
    )
    db_sub.add_parser("status", help="Show database status")

    # --- serve ---
    serve_parser = sub.add_parser("serve", help="Launch partition browser web server")
    serve_parser.add_argument("--port", type=int, default=None, metavar="PORT",
                              help="Port (default: from pixi.toml web_port, or 8081)")

    # --- notebook ---
    nb_parser = sub.add_parser("notebook", help="Launch Jupyter notebook server")
    nb_parser.add_argument("--port", type=int, default=None, metavar="PORT",
                           help="Port (default: from pixi.toml notebook_port, or 8888)")

    args = parser.parse_args()

    if args.command == "db":
        _handle_db(args)
    elif args.command == "serve":
        _handle_serve(args)
    elif args.command == "notebook":
        _handle_notebook(args)


def _handle_db(args):
    from .querydb import QueryDB

    if args.db_action == "build":
        db = QueryDB(read_only=False)
        with db:
            db.build()
    elif args.db_action == "sync":
        db = QueryDB(read_only=False)
        with db:
            db.sync()
    elif args.db_action == "sync-blocks":
        db = QueryDB(read_only=False)
        with db:
            db.sync_blocks(args.blocks)
    elif args.db_action == "status":
        db = QueryDB(read_only=True)
        with db:
            db.status()


def _handle_serve(args):
    from .utils import get_config
    from .webserver import serve

    config = get_config()
    port = args.port or config.get("web_port", 8081)
    serve(port=int(port))


def _handle_notebook(args):
    import subprocess

    from .utils import get_config
    config = get_config()
    port = args.port or config.get("notebook_port", 8888)

    cmd = [
        sys.executable, "-m", "jupyter", "notebook",
        "--no-browser",
        f"--port={int(port)}",
        "--notebook-dir=notebooks",
    ]
    print(f"Starting Jupyter on port {port}...")
    subprocess.run(cmd)


if __name__ == "__main__":
    main()
