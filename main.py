"""
Robust TAB data scraper with memory monitoring and error recovery.

Features:
- Memory monitoring and cleanup
- Graceful degradation when resources are low
- Better error handling and recovery
- Health checks
- Automatic cleanup of database connections

Author: k.hitchcock (enhanced by Claude)
Date: 2024-12-09
"""
import sys
import gc
import json
import time
import psutil
import logging
import time as timer
from datetime import datetime, timedelta, date, time as time_class
from typing import Dict, Optional, List, Any
from tab_data_extractor import TabDataExtractor
from mongodb_handler import MongoDBHandler

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
DATE_FORMAT = '%Y-%m-%d'

# Memory thresholds (in MB)
MEMORY_WARNING_THRESHOLD = 300  # 300MB - start warning
MEMORY_CRITICAL_THRESHOLD = 400  # 400MB - skip non-essential operations
MEMORY_EMERGENCY_THRESHOLD = 450  # 450MB - force cleanup and skip cycle

# Configure logging
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
        """Get current memory usage in MB."""
        mem_info = self.process.memory_info()
        memory_mb = mem_info.rss / 1024 / 1024
        if memory_mb > self.peak_memory:
            self.peak_memory = memory_mb
        return memory_mb

    def get_memory_percent(self) -> float:
        """Get memory usage as percentage of system RAM."""
        return self.process.memory_percent()

    def check_memory_status(self) -> tuple[str, float]:
        """
        Check current memory status.

        Returns:
            tuple: (status_level, memory_mb)
            status_level: 'ok', 'warning', 'critical', or 'emergency'
        """
        memory_mb = self.get_memory_mb()

        if memory_mb >= MEMORY_EMERGENCY_THRESHOLD:
            return 'emergency', memory_mb
        elif memory_mb >= MEMORY_CRITICAL_THRESHOLD:
            return 'critical', memory_mb
        elif memory_mb >= MEMORY_WARNING_THRESHOLD:
            return 'warning', memory_mb
        else:
            return 'ok', memory_mb

    def force_cleanup(self):
        """Force garbage collection and memory cleanup."""
        logger.warning(f"🧹 Forcing memory cleanup (count: {self.cleanup_count})")
        gc.collect()
        self.cleanup_count += 1

        # Log memory before/after
        memory_before = self.get_memory_mb()
        time.sleep(0.5)  # Give GC time to work
        memory_after = self.get_memory_mb()
        freed = memory_before - memory_after

        logger.info(f"💾 Memory cleanup: {memory_before:.1f}MB → {memory_after:.1f}MB (freed: {freed:.1f}MB)")
        return freed

    def log_memory_stats(self):
        """Log current memory statistics."""
        memory_mb = self.get_memory_mb()
        percent = self.get_memory_percent()
        logger.info(f"📊 Memory: {memory_mb:.1f}MB ({percent:.1f}% of system) | Peak: {self.peak_memory:.1f}MB | Cleanups: {self.cleanup_count}")


def check_within_time_bounds(start_time, end_time):
    """Check if current time is within bounds."""
    current_time = datetime.now().time()
    if start_time <= current_time <= end_time:
        return True
    return False


def convert_date_to_collection_format(date_string):
    """Convert date string to MongoDB collection name format."""
    return '_' + date_string.replace('-', '')


def update_odds_data_local(odds_data: Dict[str, Optional[Dict]], formatted_data: Dict[str, Optional[Dict]]) -> tuple[Dict[str, Optional[Dict]], bool]:
    """Update local odds data from TAB API."""
    need_update_schedule = False
    now = datetime.now()
    timestamp = now.strftime(DATETIME_FORMAT)

    for meeting in odds_data["meetings"]:
        for race in meeting["races"]:
            _id = race["id"]

            if _id and _id not in formatted_data.keys():
                logger.info(f"Can't find id: {_id} in mongodb data, did schedule miss it?")
                need_update_schedule = True
                continue

            race_time = formatted_data[_id]["norm_time"]
            race_time = datetime.strptime(race_time, DATETIME_FORMAT)
            current_time = datetime.now()

            # Ignore if the data isn't near race time
            if current_time < (race_time - timedelta(minutes=5)) or current_time > (race_time + timedelta(minutes=5)):
                continue

            logger.info(f"Updating race: {_id}")

            for entry in race["entries"]:
                entry_num = str(entry["number"])
                if not formatted_data[_id]["entries"][entry_num]["scratched"]:
                    formatted_data[_id]["entries"][entry_num]["odds"][timestamp] = entry

    return formatted_data, need_update_schedule


def update_results_data_local(results_data: Dict[str, Optional[Dict]], formatted_data: Dict[str, Optional[Dict]]) -> Dict[str, Optional[Dict]]:
    """Update local results data from TAB API."""
    for meeting in results_data["meetings"]:
        for race in meeting["races"]:
            _id = race["id"]

            if not _id:
                continue

            if formatted_data[_id]["got_results"]:
                continue

            if race["placings"] == []:
                continue

            for placed in race["placings"]:
                entry_num = str(placed["number"])
                entry_results = {
                    "results_distance": placed["distance"],
                    "results_favouritism": placed["favouritism"],
                    "results_rank": placed["rank"],
                    "results_plc": True
                }
                formatted_data[_id]["entries"][entry_num].update(entry_results)

            for also_ran in race["also_ran"]:
                entry_num = str(also_ran["number"])
                entry_results = {
                    "results_distance": also_ran["distance"],
                    "results_rank": also_ran["finish_position"]
                }
                formatted_data[_id]["entries"][entry_num].update(entry_results)

            formatted_data[_id]["got_results"] = True

    return formatted_data


def reformat_collection_format(documents: List[Dict[str, Any]]):
    """Reformat documents with _id as key."""
    try:
        id_keyed_documents = {}
        for doc in documents:
            doc_id = str(doc['_id'])
            id_keyed_documents[doc_id] = doc
        return id_keyed_documents
    except Exception as e:
        logger.error(f"Error reformatting documents: {e}")
        return None


def extract_and_update_results(mongodb: MongoDBHandler, data_extractor: TabDataExtractor, collection_name: str):
    """Extract and update results data."""
    results_data = data_extractor.get_results_data()

    mongodb.set_collection(collection_name)
    existing_data = mongodb.get_all_documents()
    formatted_data = reformat_collection_format(existing_data)

    updated_data = update_results_data_local(results_data, formatted_data)

    for id, data in updated_data.items():
        mongodb.replace_document(id, data)


def extract_and_update_odds(mongodb: MongoDBHandler, data_extractor: TabDataExtractor, collection_name: str) -> bool:
    """Extract and update odds data."""
    odds_data = data_extractor.get_odds_data()

    mongodb.set_collection(collection_name)
    existing_data = mongodb.get_all_documents()
    formatted_data = reformat_collection_format(existing_data)

    updated_data, need_update_schedule = update_odds_data_local(odds_data, formatted_data)

    for id, data in updated_data.items():
        mongodb.replace_document(id, data)

    return need_update_schedule


def extract_schedule_data(schedule_data: Dict[str, Optional[Dict]]) -> Dict[str, Optional[Dict]]:
    """Extract and format schedule data."""
    formatted_data = {}
    race_count = 0

    for meeting in schedule_data["meetings"]:
        for race in meeting["races"]:
            formatted_data[race["id"]] = {
                "_id": race["id"],
                "meeting_name": meeting["name"],
                "meeting_number": meeting["number"],
                "meeting_code": meeting["code"],
                "race_name": race["name"],
                "norm_time": race["norm_time"],
                "race_number": race["number"],
                "race_length": race["length"],
                "race_track": race["track"],
                "race_weather": race["weather"],
                "got_results": False,
                "time_schedule_pulled": datetime.now().strftime(DATETIME_FORMAT),
                "entries": {}
            }
            for entry in race["entries"]:
                formatted_data[race["id"]]["entries"][str(entry["number"])] = entry
                formatted_data[race["id"]]["entries"][str(entry["number"])]["results_plc"] = False
                formatted_data[race["id"]]["entries"][str(entry["number"])]["odds"] = {}
            race_count += 1

    return formatted_data


def update_schedule_missing_races(mongodb: MongoDBHandler, data_extractor: TabDataExtractor, collection_name: str):
    """Update schedule with any missing races."""
    schedule_date = data_extractor.get_schedule_data()
    formatted_data = extract_schedule_data(schedule_date)

    mongodb.set_collection(collection_name)
    d = mongodb.get_all_documents()
    d = reformat_collection_format(d)

    for _id, data in formatted_data.items():
        if _id not in d.keys():
            mongodb.post_data(data)


def pull_schedule_and_create_collection(mongodb: MongoDBHandler, data_extractor: TabDataExtractor, collection_name: str):
    """Pull schedule and create new collection."""
    mongodb.create_collection(collection_name)

    schedule_date = data_extractor.get_schedule_data()
    formatted_data = extract_schedule_data(schedule_date)

    mongodb.set_collection(collection_name)
    for _, data in formatted_data.items():
        mongodb.post_data(data)


def pull_tab_data_robust(memory_monitor: MemoryMonitor):
    """
    Robust TAB data pulling with memory monitoring.

    Args:
        memory_monitor: MemoryMonitor instance for tracking memory
    """
    start_time = timer.time()
    mongodb = None

    try:
        # Check memory before starting
        status, memory_mb = memory_monitor.check_memory_status()

        if status == 'emergency':
            logger.error(f"🚨 EMERGENCY: Memory at {memory_mb:.1f}MB - SKIPPING CYCLE")
            memory_monitor.force_cleanup()
            return

        if status == 'critical':
            logger.warning(f"⚠️  CRITICAL: Memory at {memory_mb:.1f}MB - limited operations only")
            memory_monitor.force_cleanup()

        if status == 'warning':
            logger.warning(f"⚠️  WARNING: Memory at {memory_mb:.1f}MB")

        # Initialize components
        data_extractor = TabDataExtractor()
        mongodb = MongoDBHandler(database_name="tab")

        if not mongodb.connect():
            logger.error("Failed to connect to MongoDB")
            return

        collection_name = convert_date_to_collection_format(date.today().strftime(DATE_FORMAT))
        logger.info(f"Current date: {collection_name}")

        # Check for schedule changes (only in early morning hours)
        if check_within_time_bounds(time_class(0, 0), time_class(5, 0)):
            logger.info("Within schedule changing time bounds")
            date_in_db = mongodb.check_collection_in_db(collection_name)
            if not date_in_db:
                schedule_date = convert_date_to_collection_format(data_extractor.get_schedule_data()["date"])
                if schedule_date == collection_name:
                    logger.info("Updating schedule")
                    pull_schedule_and_create_collection(mongodb, data_extractor, schedule_date)
                else:
                    collection_name = schedule_date

        logger.info(f"Current collection: {collection_name}")

        # Update odds (skip if in critical memory state)
        if status != 'critical':
            logger.info("Updating odds")
            update_schedule = extract_and_update_odds(mongodb, data_extractor, collection_name)

            if update_schedule:
                logger.info("Updating schedule with missing races")
                update_schedule_missing_races(mongodb, data_extractor, collection_name)
        else:
            logger.warning("Skipping odds update due to memory pressure")

        # Pull results (only on the hour, skip if memory critical)
        now = datetime.now()
        if now.minute == 0 and 0 <= now.second <= 10:
            if status != 'critical':
                logger.info("Updating results")
                extract_and_update_results(mongodb, data_extractor, collection_name)
            else:
                logger.warning("Skipping results update due to memory pressure")

        logger.info("Done for now")

    except Exception as e:
        logger.error(f"❌ Error in pull_tab_data_robust: {e}", exc_info=True)

    finally:
        # Always cleanup
        if mongodb:
            try:
                mongodb.close_connection()
                logger.info("MongoDB connection closed")
            except Exception as e:
                logger.error(f"Error closing MongoDB: {e}")

        # Force cleanup after each run
        gc.collect()

        end_time = timer.time()
        execution_time = end_time - start_time

        # Log final memory stats
        final_status, final_memory = memory_monitor.check_memory_status()
        logger.info(f"⏱️  Execution time: {execution_time:.2f}s | Final memory: {final_memory:.1f}MB ({final_status})")


def main():
    """Main entry point with memory monitoring."""
    logger.info("=" * 60)
    logger.info("🚀 Starting robust TAB scraper")

    memory_monitor = MemoryMonitor()
    memory_monitor.log_memory_stats()

    try:
        pull_tab_data_robust(memory_monitor)
    except Exception as e:
        logger.error(f"❌ Fatal error in main: {e}", exc_info=True)
    finally:
        memory_monitor.log_memory_stats()
        logger.info("=" * 60)


if __name__ == '__main__':
    main()
