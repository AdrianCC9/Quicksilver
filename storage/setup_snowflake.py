from dotenv import load_dotenv

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
