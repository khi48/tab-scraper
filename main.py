"""Robust TAB data scraper with memory monitoring and error recovery.

Features:
- Memory monitoring and cleanup
- Graceful degradation when resources are low
- Better error handling and recovery
- Health checks
- Automatic cleanup of database connections

Migrated 2026-05-15 from legacy json.tab.co.nz endpoints to the public
Affiliates v1 API (https://api.tab.co.nz/affiliates/v1/).

Author: k.hitchcock (enhanced by Claude)
"""
import gc
import time
import psutil
import logging
import time as timer
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, List, Any
from tab_data_extractor import TabDataExtractor
from mongodb_handler import MongoDBHandler

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
DATE_FORMAT = '%Y-%m-%d'


def now_utc() -> datetime:
    """Naive UTC datetime — keeps comparisons consistent regardless of container TZ."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def today_utc_str() -> str:
    return now_utc().strftime(DATE_FORMAT)

# Memory thresholds (in MB)
MEMORY_WARNING_THRESHOLD = 300  # 300MB - start warning
MEMORY_CRITICAL_THRESHOLD = 400  # 400MB - skip non-essential operations
MEMORY_EMERGENCY_THRESHOLD = 450  # 450MB - force cleanup and skip cycle

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt=DATETIME_FORMAT,
)

logger = logging.getLogger(__name__)


class MemoryMonitor:
    """Monitor and report memory usage."""

    def __init__(self):
        self.process = psutil.Process()
        self.peak_memory = 0
        self.cleanup_count = 0

    def get_memory_mb(self) -> float:
        memory_mb = self.process.memory_info().rss / 1024 / 1024
        if memory_mb > self.peak_memory:
            self.peak_memory = memory_mb
        return memory_mb

    def get_memory_percent(self) -> float:
        return self.process.memory_percent()

    def check_memory_status(self) -> tuple[str, float]:
        memory_mb = self.get_memory_mb()
        if memory_mb >= MEMORY_EMERGENCY_THRESHOLD:
            return 'emergency', memory_mb
        if memory_mb >= MEMORY_CRITICAL_THRESHOLD:
            return 'critical', memory_mb
        if memory_mb >= MEMORY_WARNING_THRESHOLD:
            return 'warning', memory_mb
        return 'ok', memory_mb

    def force_cleanup(self):
        logger.warning(f"Forcing memory cleanup (count: {self.cleanup_count})")
        gc.collect()
        self.cleanup_count += 1
        memory_before = self.get_memory_mb()
        time.sleep(0.5)
        memory_after = self.get_memory_mb()
        freed = memory_before - memory_after
        logger.info(f"Memory cleanup: {memory_before:.1f}MB -> {memory_after:.1f}MB (freed: {freed:.1f}MB)")
        return freed

    def log_memory_stats(self):
        memory_mb = self.get_memory_mb()
        percent = self.get_memory_percent()
        logger.info(f"Memory: {memory_mb:.1f}MB ({percent:.1f}% of system) | Peak: {self.peak_memory:.1f}MB | Cleanups: {self.cleanup_count}")


def convert_date_to_collection_format(date_string):
    return '_' + date_string.replace('-', '')


def iso_utc_to_str(iso_str: str) -> str:
    """Strip ISO 8601 Z suffix → naive UTC '%Y-%m-%d %H:%M:%S'.

    Affiliates v1 publishes race start_time in UTC; storing UTC throughout
    avoids tz drift since the container runs with TZ=UTC.
    """
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).replace(tzinfo=None).strftime(DATETIME_FORMAT)


def update_odds_data_local(data_extractor: TabDataExtractor, formatted_data: Dict[str, Any]) -> tuple:
    """For each race within ±5min of start time, fetch event endpoint and append odds snapshot."""
    now = now_utc()
    timestamp = now.strftime(DATETIME_FORMAT)
    need_update_schedule = False

    for _id, race in formatted_data.items():
        norm_time = race.get("norm_time")
        if not norm_time:
            continue
        try:
            race_time = datetime.strptime(norm_time, DATETIME_FORMAT)
        except (TypeError, ValueError):
            continue

        if now < race_time - timedelta(minutes=5) or now > race_time + timedelta(minutes=5):
            continue

        logger.info(f"Updating race: {_id}")
        event = data_extractor.get_event_data(_id)
        if event is None:
            continue

        entries = race.setdefault("entries", {})
        for runner in event.get("runners") or []:
            num_int = runner.get("runner_number")
            if num_int is None:
                continue
            num = str(num_int)

            entry = entries.get(num)
            if entry is None:
                entry = {
                    "runner_number": num_int,
                    "name": runner.get("name"),
                    "is_scratched": runner.get("is_scratched", False),
                    "barrier": runner.get("barrier"),
                    "jockey": runner.get("jockey"),
                    "trainer_name": runner.get("trainer_name"),
                    "weight": runner.get("weight"),
                    "results_plc": False,
                    "odds": {},
                }
                entries[num] = entry

            if entry.get("is_scratched") or entry.get("scratched"):
                continue

            entry["odds"][timestamp] = {
                "fixed_win": runner.get("odds", {}).get("fixed_win"),
                "fixed_place": runner.get("odds", {}).get("fixed_place"),
            }

    return formatted_data, need_update_schedule


def update_results_data_local(data_extractor: TabDataExtractor, formatted_data: Dict[str, Any]) -> Dict[str, Any]:
    """For each race past start time without results, fetch event and persist data.results[]."""
    now = now_utc()

    for _id, race in formatted_data.items():
        if race.get("got_results"):
            continue

        norm_time = race.get("norm_time")
        if not norm_time:
            continue
        try:
            race_time = datetime.strptime(norm_time, DATETIME_FORMAT)
        except (TypeError, ValueError):
            continue

        if now < race_time:
            continue

        event = data_extractor.get_event_data(_id)
        if event is None:
            continue

        results = event.get("results") or []
        if not results:
            continue

        entries = race.setdefault("entries", {})
        for placed in results:
            num_int = placed.get("runner_number")
            if num_int is None:
                continue
            num = str(num_int)

            entry = entries.get(num)
            if entry is None:
                entry = {
                    "runner_number": num_int,
                    "name": placed.get("name"),
                    "is_scratched": False,
                    "results_plc": False,
                    "odds": {},
                }
                entries[num] = entry

            entry["results_rank"] = placed.get("position")
            entry["results_margin"] = placed.get("margin_length")
            entry["results_plc"] = True

        race["got_results"] = True

    return formatted_data


def reformat_collection_format(documents: List[Dict[str, Any]]):
    try:
        return {str(doc["_id"]): doc for doc in documents}
    except Exception as e:
        logger.error(f"Error reformatting documents: {e}")
        return None


def extract_and_update_results(mongodb: MongoDBHandler, data_extractor: TabDataExtractor, collection_name: str):
    mongodb.set_collection(collection_name)
    existing_data = mongodb.get_all_documents()
    formatted_data = reformat_collection_format(existing_data)
    if not formatted_data:
        return

    updated_data = update_results_data_local(data_extractor, formatted_data)

    for _id, data in updated_data.items():
        mongodb.replace_document(_id, data)


def extract_and_update_odds(mongodb: MongoDBHandler, data_extractor: TabDataExtractor, collection_name: str) -> bool:
    mongodb.set_collection(collection_name)
    existing_data = mongodb.get_all_documents()
    formatted_data = reformat_collection_format(existing_data)
    if not formatted_data:
        return False

    updated_data, need_update_schedule = update_odds_data_local(data_extractor, formatted_data)

    for _id, data in updated_data.items():
        mongodb.replace_document(_id, data)

    return need_update_schedule


def extract_schedule_data(schedule_data: Dict[str, Any]) -> Dict[str, Any]:
    formatted_data: Dict[str, Any] = {}
    meetings = (schedule_data or {}).get("meetings") or []

    for idx, meeting in enumerate(meetings, start=1):
        for race in meeting.get("races") or []:
            race_id = race.get("id")
            if not race_id:
                continue

            start_iso = race.get("start_time")
            try:
                norm_time = iso_utc_to_str(start_iso) if start_iso else None
            except (TypeError, ValueError):
                norm_time = None

            formatted_data[race_id] = {
                "_id": race_id,
                "meeting_name": meeting.get("name"),
                "meeting_number": idx,
                "meeting_code": meeting.get("meeting"),
                "race_name": race.get("name"),
                "norm_time": norm_time,
                "race_number": race.get("race_number"),
                "race_length": race.get("distance"),
                "race_track": race.get("track_condition"),
                "race_weather": race.get("weather"),
                "got_results": False,
                "time_schedule_pulled": now_utc().strftime(DATETIME_FORMAT),
                "entries": {},
            }

    return formatted_data


def update_schedule_missing_races(mongodb: MongoDBHandler, data_extractor: TabDataExtractor, collection_name: str):
    schedule_data = data_extractor.get_schedule_data()
    formatted_data = extract_schedule_data(schedule_data)

    mongodb.set_collection(collection_name)
    existing = mongodb.get_all_documents()
    existing_map = reformat_collection_format(existing) or {}

    for _id, data in formatted_data.items():
        if _id not in existing_map:
            mongodb.post_data(data)


def pull_schedule_and_create_collection(mongodb: MongoDBHandler, data_extractor: TabDataExtractor, collection_name: str):
    mongodb.create_collection(collection_name)

    schedule_data = data_extractor.get_schedule_data()
    formatted_data = extract_schedule_data(schedule_data)

    mongodb.set_collection(collection_name)
    for _, data in formatted_data.items():
        mongodb.post_data(data)


def pull_tab_data_robust(memory_monitor: MemoryMonitor):
    """Robust TAB data pulling with memory monitoring."""
    start_time = timer.time()
    mongodb = None

    try:
        status, memory_mb = memory_monitor.check_memory_status()

        if status == 'emergency':
            logger.error(f"EMERGENCY: Memory at {memory_mb:.1f}MB - SKIPPING CYCLE")
            memory_monitor.force_cleanup()
            return

        if status == 'critical':
            logger.warning(f"CRITICAL: Memory at {memory_mb:.1f}MB - limited operations only")
            memory_monitor.force_cleanup()

        if status == 'warning':
            logger.warning(f"WARNING: Memory at {memory_mb:.1f}MB")

        data_extractor = TabDataExtractor()
        mongodb = MongoDBHandler(database_name="tab")

        if not mongodb.connect():
            logger.error("Failed to connect to MongoDB")
            return

        collection_name = convert_date_to_collection_format(today_utc_str())
        logger.info(f"Current date: {collection_name}")

        if not mongodb.check_collection_in_db(collection_name):
            logger.info("Today's collection missing — pulling schedule")
            pull_schedule_and_create_collection(mongodb, data_extractor, collection_name)

        logger.info(f"Current collection: {collection_name}")

        if status != 'critical':
            logger.info("Updating odds")
            update_schedule = extract_and_update_odds(mongodb, data_extractor, collection_name)
            if update_schedule:
                logger.info("Updating schedule with missing races")
                update_schedule_missing_races(mongodb, data_extractor, collection_name)
        else:
            logger.warning("Skipping odds update due to memory pressure")

        now = now_utc()
        if now.minute == 0 and 0 <= now.second <= 10:
            if status != 'critical':
                logger.info("Updating results")
                extract_and_update_results(mongodb, data_extractor, collection_name)
            else:
                logger.warning("Skipping results update due to memory pressure")

        logger.info("Done for now")

    except Exception as e:
        logger.error(f"Error in pull_tab_data_robust: {e}", exc_info=True)

    finally:
        if mongodb:
            try:
                mongodb.close_connection()
                logger.info("MongoDB connection closed")
            except Exception as e:
                logger.error(f"Error closing MongoDB: {e}")

        gc.collect()

        execution_time = timer.time() - start_time
        final_status, final_memory = memory_monitor.check_memory_status()
        logger.info(f"Execution time: {execution_time:.2f}s | Final memory: {final_memory:.1f}MB ({final_status})")


def main():
    logger.info("=" * 60)
    logger.info("Starting robust TAB scraper")

    memory_monitor = MemoryMonitor()
    memory_monitor.log_memory_stats()

    try:
        pull_tab_data_robust(memory_monitor)
    except Exception as e:
        logger.error(f"Fatal error in main: {e}", exc_info=True)
    finally:
        memory_monitor.log_memory_stats()
        logger.info("=" * 60)


if __name__ == '__main__':
    main()
