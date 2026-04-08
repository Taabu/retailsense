import os

# Force PostgreSQL
SQLALCHEMY_DATABASE_URI = os.environ.get("SQLALCHEMY_DATABASE_URI")

# CRITICAL: Disable the default SQLite fallback
# This prevents Superset from looking for superset.db in the home directory
SQLALCHEMY_TRACK_MODIFICATIONS = True

# Secret key
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY")

# Disable the default home path logic that creates SQLite
# We force the home directory to a temp location to avoid file conflicts
SUPERSET_HOME = "/tmp/superset_home"

# Ensure the home directory exists
os.makedirs(SUPERSET_HOME, exist_ok=True)
