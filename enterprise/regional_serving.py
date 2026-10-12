"""Run the database-free, catalog-backed regional S3 endpoint."""
from fabric_shortcut_proxy import config


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "enterprise.control.regional_serving_app:app_from_environment",
        factory=True, host=config.CONTROL_HOST, port=config.CONTROL_PORT,
    )
