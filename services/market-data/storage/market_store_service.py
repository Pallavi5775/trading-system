import logging
from datetime import datetime
from sqlalchemy.orm import Session
from storage.postgres import SessionLocal
from models.models import MarketData, MarketDataHistory

# 🔹 Logger setup
logger = logging.getLogger("market_store_service")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)


def has_changed(existing, new):
    tolerance = 1e-6

    def diff(a, b):
        if a is None or b is None:
            return a != b
        return abs(a - b) > tolerance

    return (
        diff(existing.open, new.get("open")) or
        diff(existing.high, new.get("high")) or
        diff(existing.low, new.get("low")) or
        diff(existing.close, new.get("close")) or
        diff(existing.adj_close, new.get("adj_close")) or
        diff(existing.volume, new.get("volume"))
    )


def store_market_data_with_versioning(df):
    """
    Stores dataframe into market_data table with:
    - Versioning
    - History tracking
    - Change detection
    - Logging
    """

    logger.info("Starting market data storage pipeline")

    db: Session = SessionLocal()

    inserted = 0
    updated = 0
    skipped = 0
    archived = 0

    try:
        records = df.to_dict(orient="records")
        logger.info(f"Processing {len(records)} records")

        for row in records:
            symbol_id = row["symbol_id"]
            timestamp = row["timestamp"]

            logger.debug(f"Processing row: {symbol_id} @ {timestamp}")

            existing = db.query(MarketData).filter_by(
                symbol_id=symbol_id,
                timestamp=timestamp
            ).first()

            # CASE 1: New record
            if not existing:
                logger.info(f"[INSERT] {symbol_id} @ {timestamp}")

                new_record = MarketData(
                symbol_id=row.get("symbol_id"),
                timestamp=row.get("timestamp"),

                open=row.get("open"),
                high=row.get("high"),
                low=row.get("low"),
                close=row.get("close"),
                adj_close=row.get("adj_close"),
                volume=row.get("volume"),

                log_return=row.get("log_return"),
                simple_return=row.get("simple_return"),

                volatility=row.get("volatility"),
                volatility_7d=row.get("volatility_7d"),
                volatility_30d=row.get("volatility_30d"),

                rolling_mean_7d=row.get("rolling_mean_7d"),
                rolling_std_7d=row.get("rolling_std_7d"),

                missing_flag=row.get("missing_flag"),
                price_valid=row.get("price_valid"),
                return_valid=row.get("return_valid"),
                quality_flag=row.get("quality_flag"),

                data_source=row.get("data_source"),
                ingestion_time=row.get("ingestion_time"),

                version=1
            )
                db.add(new_record)
                inserted += 1
                continue

            # CASE 2: No meaningful change
            if not has_changed(existing, row):
                logger.debug(f"[SKIP] No change for {symbol_id} @ {timestamp}")
                skipped += 1
                continue

            # CASE 3: Data changed
            logger.warning(f"[UPDATE] Change detected for {symbol_id} @ {timestamp}")

            # Archive old version
            history = MarketDataHistory(
                symbol_id=existing.symbol_id,
                timestamp=existing.timestamp,
                open=existing.open,
                high=existing.high,
                low=existing.low,
                close=existing.close,
                adj_close=existing.adj_close,
                volume=existing.volume,
                log_return=existing.log_return,
                volatility=existing.volatility,
                data_source=existing.data_source,
                version=existing.version,
                archived_at=datetime.utcnow()
            )

            db.add(history)
            archived += 1

            logger.info(
                f"[ARCHIVE] {symbol_id} @ {timestamp} | version={existing.version}"
            )

            # Update main table
            existing.open = row.get("open")
            existing.high = row.get("high")
            existing.low = row.get("low")
            existing.close = row.get("close")
            existing.adj_close = row.get("adj_close")
            existing.volume = row.get("volume")

            existing.log_return = row.get("log_return")
            existing.volatility = row.get("volatility")

            existing.data_source = row.get("data_source")

            existing.version += 1
            existing.last_updated = datetime.utcnow()

            updated += 1

            logger.info(
                f"[UPDATED] {symbol_id} @ {timestamp} → version={existing.version}"
            )

        db.commit()

        logger.info("Market data storage completed")
        logger.info(
            f"Summary → inserted={inserted}, updated={updated}, "
            f"archived={archived}, skipped={skipped}"
        )

    except Exception as e:
        logger.exception(f"[ERROR] Failed to store market data: {str(e)}")
        db.rollback()
        raise e

    finally:
        db.close()
        logger.info("Database session closed")