```mermaid
flowchart LR

    subgraph config["config/"]
        config_files["settings.py
.env
logging_config.py"]
    end

    subgraph models["models/"]
        raw_headline["raw_headline.py"]
        scored_headline["scored_headline.py"]
        sentiment_result["sentiment_result.py"]
        anomaly_event["anomaly_event.py"]
        analytics_result["analytics_result.py"]
    end

    subgraph ingestion["ingestion/"]
        finnhub_client["finnhub_client.py"]
        fetch_news_job["fetch_news_job.py"]
    end

    subgraph streaming["streaming/"]
        news_producer["news_producer.py"]
        sentiment_consumer["sentiment_consumer.py"]
        kafka_topic["Kafka Topic"]
    end

    subgraph sentiment["sentiment/"]
        finbert_scorer["finbert_scorer.py"]
    end

    subgraph storage["storage/"]
        snowflake_loader["snowflake_loader.py"]
        warehouse_tables["raw_news / scored_news tables"]
    end

    subgraph transformations["transformations/"]
        dbt_runner["dbt_runner.py"]
        dbt_models["dbt models"]
    end

    subgraph analytics["analytics/"]
        analytics_service["analytics_service.py"]
        anomaly_detection["anomaly_detection.py"]
    end

    subgraph alerts["alerts/"]
        alert_engine["alert_engine.py"]
        notification_service["notification_service.py"]
    end

    subgraph dashboard["dashboard/"]
        dashboard_service["dashboard_service.py"]
        streamlit_app["streamlit_app.py"]
    end

    subgraph orchestration["orchestration/"]
        airflow_dag["airflow_dag.py"]
    end

    subgraph tests["tests/"]
        test_modules["unit + integration tests"]
    end

    config --> ingestion
    config --> streaming
    config --> sentiment
    config --> storage
    config --> analytics
    config --> alerts
    config --> dashboard
    config --> orchestration

    models --> ingestion
    models --> streaming
    models --> sentiment
    models --> storage
    models --> analytics
    models --> alerts
    models --> dashboard

    ingestion --> streaming
    news_producer --> kafka_topic
    kafka_topic --> sentiment_consumer
    sentiment_consumer --> finbert_scorer
    sentiment_consumer --> storage
    ingestion --> storage

    storage --> transformations
    dbt_runner --> dbt_models
    transformations --> analytics

    analytics --> alerts
    analytics --> dashboard

    alerts --> notification_service
    orchestration --> ingestion
    orchestration --> streaming
    orchestration --> storage
    orchestration --> transformations
    orchestration --> analytics
    orchestration --> alerts
    orchestration --> dashboard

    tests --> ingestion
    tests --> streaming
    tests --> sentiment
    tests --> storage
    tests --> analytics
    tests --> alerts
```
