from dotenv import load_dotenv
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from storage.snowflake_storage import SnowflakeStorage

def main() ->None:
    """
    Create the Snowflake tables needed by Quicksilver.
    """

    # Load environment variables from local .env file.
    load_dotenv()
    storage = SnowflakeStorage()

    try:
        storage.create_tables()
        print("Snowflake tables are ready.")

    finally:
        storage.close()

if __name__ == "__main__":
    main()
