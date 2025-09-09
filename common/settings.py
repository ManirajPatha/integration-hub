from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    hub_port: int = Field(8080, alias="HUB_PORT")
    d365_org_url: str = Field(..., alias="D365_ORG_URL")
    d365_tenant_id: str = Field(..., alias="D365_TENANT_ID")
    d365_client_id: str = Field(..., alias="D365_CLIENT_ID")
    d365_client_secret: str = Field(..., alias="D365_CLIENT_SECRET")

    # Tell Pydantic to read from .env automatically
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)

settings = Settings()