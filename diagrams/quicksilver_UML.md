```mermaid
flowchart TD

    %% --- LANE 1: Data Ingestion ---
    A[Fetch Raw Data<br/>- Finnhub API<br/>- Store JSON] --> B[Normalize & Store Data<br/>- Time normalization<br/>- Insert into HEADLINES]

    %% --- LANE 2: NLP Scoring ---
    B --> C[FinBERT Scoring<br/>- Run model<br/>- Save pos/neu/neg scores<br/>- Insert into SENTIMENT]

    %% --- LANE 3: Feature Engineering ---
    C --> D[Sentiment Analysis<br/>- Z-scores<br/>- Volume<br/>- Polarity<br/>- Confidence<br/>- Rolling windows<br/>- Store FEATURES]

    %% --- LANE 4: Alerts ---
    D --> E[Alerts<br/>- Apply alert rules<br/>- Create notifications]

    %% --- LANE 5: Dashboard ---
    E --> F[GUI (Streamlit)<br/>- Display metrics<br/>- Visualize trends]

```
