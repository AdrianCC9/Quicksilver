```mermaid
classDiagram

    %% ───────── Core ORM entities (from ERD) ─────────
    class Headline {
        +int id
        +string ticker
        +string source
        +string title
        +string url
        +string published_at_utc
        +string raw_json
        +string hash
        +string created_at
    }

    class Sentiment {
        +int id
        +int headline_id
        +string label
        +float score_pos
        +float score_neu
        +float score_neg
        +string model_version
        +int inference_ms
        +string created_at
    }

    class Feature {
        +int id
        +string ticker
        +string window
        +string ts_utc
        +float sent_mean
        +float sent_z
        +float vol_z
        +int headlines_n
        +string created_at
    }

    class Alert {
        +int id
        +string ticker
        +string kind
        +string window
        +string threshold
        +string payload_json
        +string fired_at
    }

    %% ───────── Current pipeline / service classes ─────────
    class FinnhubClient {
        +fetch_company_news(ticker, start, end)
        -api_key : string
        -base_url : string
    }

    class RawNewsStore {
        -raw_dir : string
        +save_raw_jsonl(ticker, articles)
        +list_raw_files()
    }

    class NewsNormalizer {
        +process_raw_jsonl()
        +normalize_article(raw)
        -session_factory
    }

    class SentimentModel {
        +load_model()
        +predict(texts)
        -tokenizer
        -model
        -device
    }

    class SentimentPipeline {
        +run_inference()
        +get_unscored_headlines()
        +analyze_batch(batch)
        +store_sentiment(results)
        -session_factory
    }

    %% ───────── Planned / future components ─────────
    class FeatureEngine {
        <<planned>>
        +compute_features(ticker, window)
        +update_features()
    }

    class AlertEngine {
        <<planned>>
        +evaluate_rules()
        +create_alerts()
    }

    class AlertChannel {
        <<planned>>
        +send(message)
    }

    class SlackAlertChannel {
        <<planned>>
        +send(message)
    }

    class EmailAlertChannel {
        <<planned>>
        +send(message)
    }

    class DashboardApp {
        <<planned>>
        +run()
    }

    class Scheduler {
        <<planned>>
        +start()
        +schedule_jobs()
    }

    %% ───────── Entity relationships (matching ERD) ─────────
    Headline "1" o-- "1" Sentiment : "1 to 1"
    Sentiment "1" --> "0..*" Feature : "1 window → many features"
    Feature  "1" --> "0..*" Alert : "1 feature → many alerts"

    %% ───────── Service dependencies ─────────
    FinnhubClient ..> RawNewsStore : writes_raw
    RawNewsStore ..> NewsNormalizer : provides_raw
    NewsNormalizer ..> Headline : inserts

    SentimentPipeline ..> SentimentModel : uses
    SentimentPipeline ..> Headline : reads
    SentimentPipeline ..> Sentiment : writes

    FeatureEngine ..> Sentiment : reads
    FeatureEngine ..> Feature : writes

    AlertEngine ..> Feature : reads
    AlertEngine ..> Alert : writes
    AlertEngine ..> AlertChannel : sends_via
    SlackAlertChannel ..|> AlertChannel
    EmailAlertChannel ..|> AlertChannel

    DashboardApp ..> Feature
    DashboardApp ..> Sentiment
    DashboardApp ..> Alert

    Scheduler ..> FinnhubClient
    Scheduler ..> NewsNormalizer
    Scheduler ..> SentimentPipeline
    Scheduler ..> FeatureEngine
    Scheduler ..> AlertEngine
