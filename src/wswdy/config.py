from functools import lru_cache
from pydantic import HttpUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    env: str = Field("dev", alias="WSWDY_ENV")
    base_url: str = Field("http://localhost:8000", alias="WSWDY_BASE_URL")
    log_dir: str = Field("./logs", alias="WSWDY_LOG_DIR")
    db_path: str = Field("./dccrime.db", alias="WSWDY_DB_PATH")

    hmac_secret: str
    admin_token: str

    maptiler_api_key: str

    mpd_feed_url: HttpUrl = Field(
        "https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/FeatureServer/39/query"
        "?outFields=*&where=1%3D1&f=geojson",
        alias="MPD_FEED_URL",
    )
    fixture_mpd_path: str | None = Field(None, alias="WSWDY_FIXTURE_MPD_PATH")

    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_pass: str = ""
    smtp_from: str = "wswdy <noreply@iandmuir.com>"
    admin_email: str = "iandmuir@gmail.com"

    whatsapp_mcp_url: str = ""
    whatsapp_mcp_token: str = ""
    whatsapp_from_number: str = "+12024682709"

    ha_webhook_url: str = ""


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
