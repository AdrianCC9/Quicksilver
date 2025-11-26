import time
import torch
import logging
import pandas as pd
from datetime import datetime, timezone
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from create_db import Base, Headline, Sentiment

# Configs
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

DB_PATH = ".data/quicksilver.db"
MODEL_REPO = "ProsusAI/finbert"
BATCH_SIZE = 8
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

# Create datbase connection
engine = create_engine(f"sqlite:///{DB_PATH}", echo=False, future=True)

# Load Model and Tokenizer
logging.info(f"Loading FinBERT model ({MODEL_REPO}) on {DEVICE}...")
tokenizer = AutoTokenizer.from_pretrained(MODEL_REPO)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_REPO).to(DEVICE)
model.eval()

# Get Headlines not yet analyzed for sentiment (smtmt)
def get_unanalyzed_headlines(session):
    stmt = """
        SELECT h.id, h.title
        FROM headlines h
        LEFT JOIN sentiment s on s.headline_id=h.id
        WHERE s.headline_id is NULL
        LIMIT 200;
        """
    return session.execute(stmt).fetchall()

# Run FinBERT on a batch of headlines
def analyze_batch(batch):
    inputs = tokenizer(batch, padding=True, truncation=True, max_length=128, return_tensors="pt").to(DEVICE)
    with torch.no_grad():
        start = time.time()
        outputs = model(**inputs)
        elapsed_ms = int((time.time() - start) * 1000)
        probs = torch.nn.functional.softmax(outputs.logits, dim=-1).cpu().numpy()
    return probs, elapsed_ms

# Main Routine
def run_inference():
    # Start session
    session = Session(engine)
    inserted = 0
    unanalyzed = get_unanalyzed_headlines(session)

    # If list is empty, close session
    if not unanalyzed:
        logging.info("No new headlines to analyze")
        session.close()
        return
    
    # Logs how many headlines will be processed
    logging.info(f"Found {len(unanalyzed)} unanalyzed headlines.")

    # For all unanalyzed headlines, iterate over steps of batch size
    for i in range(0, len(unanalyzed), BATCH_SIZE):
        batch = unanalyzed[i:i + BATCH_SIZE]
        ids = [row[0] for row in batch]
        texts = [row[1] for row in batch]

        # Run FinBERT on that batch of texts to get array of probabilites and elapsed time
        probs, elapsed_ms = analyze_batch(texts)

        # Loops over rows in probs
        for j, (p_neg, p_neu, p_pos) in enumerate(probs):   # FIXED ORDER
            # Argmax gives index in FinBERT's order: [negative, neutral, positive]
            label_idx = torch.tensor([p_neg, p_neu, p_pos]).argmax().item()

            # Correct FinBERT label mapping
            label = ["negative", "neutral", "positive"][label_idx]

            # Add info to Sentiment table
            sentiment = Sentiment(
                headline_id=ids[j],
                label=label,
                score_pos=float(p_pos),
                score_neu=float(p_neu),
                score_neg=float(p_neg),
                model_version=MODEL_REPO,
                inference_ms=elapsed_ms,
                created_at=datetime.now(timezone.utc).isoformat(),
            )
            session.add(sentiment)

        session.commit()
        inserted += len(ids)
        logging.info(f"Processed {inserted}/{len(unanalyzed)} headlines so far...")

    session.close()
    logging.info(f"Finished inference: {inserted} sentiment rows inserted.")

if __name__ == "__main__":
    run_inference()
