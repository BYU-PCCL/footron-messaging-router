import sys

import uvicorn


def main():
    uvicorn.run("footron_router.dev_server:app", host="127.0.0.1", port=8089, log_level="info")


if __name__ == "__main__":
    sys.exit(main())
