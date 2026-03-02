from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    GROQ_API_KEY: str = ""
    DATABASE_URL: str = "sqlite:///./datapilot.db"
    REDIS_URL: str = "redis://localhost:6379/0"
    ANALYTICS_KEY: str = ""

    class Config:
        env_file = ".env"
        extra = "allow"


settings = Settings()
