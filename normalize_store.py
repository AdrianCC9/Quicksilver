import os
import json
import hashlib
import logging
from datetime import datetime, timezone
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from create_db import Base, Headline

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

DB_PATH = ".data/quicksilver.db"
RAW_DIR = ".data/raw"

# Corrected variable name
engine = create_engine(f"sqlite:///{DB_PATH}", echo=False, future=True)
SessionLocal = sessionmaker(bind=engine)

def normalize_record(item):
    # Check if record contains all required fields, if not, skip record
    required = ["headline", "related", "source", "url", "datetime"]
    if not all(k in item and item[k] for k in required):
        return None
    try:
        # Convert timestamp → ISO UTC string
        dt = datetime.fromtimestamp(item["datetime"], tz=timezone.utc)
        published_at_utc = dt.isoformat()

        # Compute hash for dedupe
        dedupe_key = f"{item['url']}|{published_at_utc}"
        hash_value = hashlib.sha256(dedupe_key.encode()).hexdigest()

        return {
            "ticker": item["related"].strip(),
            "source": item["source"].strip(),
            "title": item["headline"].strip(),
            "url": item["url"].strip(),
            "published_at_utc": published_at_utc,
            "raw_json": json.dumps(item, ensure_ascii=False),
            "hash": hash_value,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        logging.warning(f"Skipping bad record: {e}")
        return None

def process_raw_jsonl():
    # Create database session
    session = SessionLocal()
    inserted = 0
    skipped = 0
    # For each file in RAW data
    for fname in os.listdir(RAW_DIR):
        # if the file does not end with .json, skip file and move on to next
        if not fname.endswith(".jsonl"):
            continue

        # Create path to the individual file
        path = os.path.join(RAW_DIR, fname)

        # Print processing label
        logging.info(f"Processing {path}")

        # For each record in the file, convert to python dictionary
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    item = json.loads(line.strip())
                except json.JSONDecodeError:
                    continue
                # normalize record
                record = normalize_record(item)
                if not record:
                    skipped += 1
                    continue
                # Insert record into headline table
                # **record is the unpackaged dictionary perfect so SQL inserting
                headline = Headline(**record)
                try:
                    session.add(headline)
                    session.commit()
                    inserted += 1
                except Exception as e:
                    # Undo any uncommited database changes
                    session.rollback()
                    if "UNIQUE constraint" in str(e):
                        skipped += 1
                    else:
                        logging.error(f"Insert error: {e}")
    logging.info(f"Inserted: {inserted}, Skipped: {skipped}")
    session.close()

if __name__ == "__main__":
    process_raw_jsonl()
