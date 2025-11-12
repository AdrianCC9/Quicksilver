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

engine = create_engine(f"sqlite:///{DB_PATH}", eccho=False, future=True)

# Load Model and Tokenizer
logging.info(f"Loading FinBERT model ({MODEL_REPO}) on {DEVICE}...")
tokenizer = AutoTokenizer.from_pretrained(MODEL_REPO)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_REPO).to(DEVICE)
model.eval()

# Get Headlines not yet analyzed
def get_unanalyzed_headlines(session):
    """Return headlines that don't yet have sentiment results."""
    stmt = """
        SELECT h.id, h.title
        FROM headlines h
        LEFT JOIN sentiment s on s.headline_id=h.id
        WHERE s.headline_id is NULL
        LIMIT 200;
        """
    return session.execute(stmt).fetchall()

# Run FinBERT inference
def analyze_batch(batch):
    """Run rinbert on a batch of headline texts."""
    inputs = tokenizer(batch, padding=True, truncation=True, max_length=128, return_tensors="pt").to(DEVICE)
    with torch.no_grad():
        start = time.time()
        outputs = model(**inputs)
        elapsed_ms = int((time.time() - start) * 1000)
        probs = torch.nn.functional.softmax(outputs.logits, dim=-1).cpu().numpy()
    return probs, elapsed_ms

# Main Routine
def run_inference():
    session = Session(enginer)
    inserted = 0
    unanalyzed = get_unanalyzed_headlines(session)