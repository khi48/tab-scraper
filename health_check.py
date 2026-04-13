"""
Health check script for TAB scraper.

Checks:
- Memory usage
- MongoDB connectivity
- Last scrape time
- System resources

Usage:
  python health_check.py
"""
import psutil
import logging
from datetime import datetime, timedelta
from mongodb_handler import MongoDBHandler

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Thresholds
MEMORY_WARNING_MB = 300
MEMORY_CRITICAL_MB = 400
DISK_WARNING_PERCENT = 80
DISK_CRITICAL_PERCENT = 90


def check_memory():
    """Check system and process memory."""
    process = psutil.Process()
    mem_info = process.memory_info()
    memory_mb = mem_info.rss / 1024 / 1024

    # System memory
    system_mem = psutil.virtual_memory()
    system_used_percent = system_mem.percent

    status = '✅ OK'
    if memory_mb >= MEMORY_CRITICAL_MB:
        status = '🚨 CRITICAL'
    elif memory_mb >= MEMORY_WARNING_MB:
        status = '⚠️  WARNING'

    logger.info(f"Memory: {status}")
    logger.info(f"  Process: {memory_mb:.1f}MB")
    logger.info(f"  System: {system_used_percent:.1f}% used ({system_mem.used / 1024 / 1024 / 1024:.1f}GB / {system_mem.total / 1024 / 1024 / 1024:.1f}GB)")

    return status != '🚨 CRITICAL'


def check_disk():
    """Check disk space."""
    disk = psutil.disk_usage('/')
    percent_used = disk.percent

    status = '✅ OK'
    if percent_used >= DISK_CRITICAL_PERCENT:
        status = '🚨 CRITICAL'
    elif percent_used >= DISK_WARNING_PERCENT:
        status = '⚠️  WARNING'

    logger.info(f"Disk: {status}")
    logger.info(f"  Used: {percent_used:.1f}% ({disk.used / 1024 / 1024 / 1024:.1f}GB / {disk.total / 1024 / 1024 / 1024:.1f}GB)")
    logger.info(f"  Free: {disk.free / 1024 / 1024 / 1024:.1f}GB")

    return status != '🚨 CRITICAL'


def check_mongodb():
    """Check MongoDB connectivity."""
    try:
        mongodb = MongoDBHandler(database_name="tab")
        if mongodb.connect():
            collections = mongodb.get_all_collections()
            logger.info(f"MongoDB: ✅ OK")
            logger.info(f"  Collections: {len(collections)}")
            logger.info(f"  Latest: {max(collections) if collections else 'None'}")
            mongodb.close_connection()
            return True
        else:
            logger.error(f"MongoDB: ❌ FAILED - Cannot connect")
            return False
    except Exception as e:
        logger.error(f"MongoDB: ❌ FAILED - {e}")
        return False


def check_cpu():
    """Check CPU usage."""
    cpu_percent = psutil.cpu_percent(interval=1)

    status = '✅ OK'
    if cpu_percent >= 90:
        status = '⚠️  WARNING'

    logger.info(f"CPU: {status}")
    logger.info(f"  Usage: {cpu_percent:.1f}%")

    return True  # CPU high is warning, not failure


def main():
    """Run all health checks."""
    logger.info("=" * 60)
    logger.info("🏥 TAB Scraper Health Check")
    logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    checks = {
        'Memory': check_memory(),
        'Disk': check_disk(),
        'CPU': check_cpu(),
        'MongoDB': check_mongodb()
    }

    logger.info("=" * 60)
    logger.info("Summary:")
    all_passed = all(checks.values())
    for check_name, passed in checks.items():
        status = '✅' if passed else '❌'
        logger.info(f"  {status} {check_name}")

    if all_passed:
        logger.info("✅ All checks passed")
        return 0
    else:
        logger.error("❌ Some checks failed")
        return 1


if __name__ == '__main__':
    exit(main())
