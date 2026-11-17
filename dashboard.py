"""Entry point for the custom Waypoints dashboard."""

import uvicorn


def main() -> None:
    uvicorn.run(
        "waypoints.dashboard.app:app",
        host="127.0.0.1",
        port=8000,
        reload=False,
    )


if __name__ == "__main__":
    main()
