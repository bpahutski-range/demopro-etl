import os
import yaml
from dotenv import load_dotenv

# -----------------------------------------------------------------
# 1. Load .env into the environment
#    This makes DEMOPRO_USERNAME etc. available to os.getenv().
#    If .env doesn't exist (e.g. on a server where env vars are set directly), load_dotenv() does nothing — which is correct.
# -----------------------------------------------------------------
load_dotenv()

# -----------------------------------------------------------------
# 2. Load config.yaml
#    Opens the file, parses it into a Python dictionary, and stores
#    it in _cfg. The underscore prefix is a convention meaning "internal to this file — don't import this directly."
# -----------------------------------------------------------------
with open("config.yaml", "r") as f:
    _cfg = yaml.safe_load(f)

# -----------------------------------------------------------------
# 3. Pull secrets from environment and validate immediately.
#    If either is missing, the pipeline crashes here with a clear
#    message rather than somewhere deep in an API call.
# -----------------------------------------------------------------
DEMOPRO_USERNAME = os.getenv("DEMOPRO_USERNAME")
DEMOPRO_PASSWORD = os.getenv("DEMOPRO_PASSWORD")

if not DEMOPRO_USERNAME:
    raise EnvironmentError("DEMOPRO_USERNAME is not set. Check your .env file.")
if not DEMOPRO_PASSWORD:
    raise EnvironmentError("DEMOPRO_PASSWORD is not set. Check your .env file.")

# -----------------------------------------------------------------
# 4. Pull settings from the YAML dictionary and expose them as
#    clean, flat constants. Other files import these directly —
#    they never touch _cfg themselves.
# -----------------------------------------------------------------
BASE_URL            = _cfg["api"]["base_url"]
POLL_INTERVAL       = _cfg["api"]["poll_interval_seconds"]
MAX_WAIT            = _cfg["api"]["max_wait_seconds"]
TAXONOMY_SECTIONS   = _cfg["api"]["taxonomy_sections"]
STALENESS_DAYS      = _cfg["pipeline"]["staleness_days"]