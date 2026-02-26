import os

# Environment-configurable service URLs
# Default to production URLs, override with environment variables for local dev
DATABASE_SERVICE_URL = os.getenv(
    "DATABASE_SERVICE_URL",
    "https://database.financeapp.lucas.engineering"
)

YFINANCE_SERVICE_URL = os.getenv(
    "YFINANCE_SERVICE_URL",
    "https://yfinance.financeapp.lucas.engineering"
)
