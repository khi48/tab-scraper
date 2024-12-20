"""
A contintously running script

Author: k.hitchcock
Date: 2024-12-11
"""
import sys
import json
import time
import logging
import time as timer
from datetime import datetime, timedelta, date, time
from typing import Dict, Optional, List, Any
from tab_data_extractor import TabDataExtractor
from mongodb_handler import MongoDBHandler

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
DATE_FORMAT = '%Y-%m-%d'

logging.basicConfig(
    level=logging.INFO,  # Set the logging level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt=DATETIME_FORMAT,
)

# Create a logger
logger = logging.getLogger(__name__)

def check_within_time_bounds(start_time, end_time):
    current_time = datetime.now().time()
    if start_time <= current_time <= end_time:
        return True
    return False

def convert_date_to_collection_format(date_string):
    return '_' + date_string.replace('-', '')


def update_odds_data_local(odds_data: Dict[str, Optional[Dict]], formatted_data: Dict[str, Optional[Dict]]) -> Dict[str, Optional[Dict]]:
    # logging.info("extracting and reformatting odds data")
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
            
            # ignore if the data isn't near race time
            # HACK: there is a slight assumption that the race would have started 5 minutes after norm time
            if current_time < (race_time - timedelta(minutes=5)) or current_time > (race_time + timedelta(minutes=5)):
                continue

            logger.info(f"Updating race: {_id}")

            for entry in race["entries"]:
                entry_num = str(entry["number"])
                if not formatted_data[_id]["entries"][entry_num]["scratched"]:
                    formatted_data[_id]["entries"][entry_num]["odds"][timestamp] = entry 

    return formatted_data, need_update_schedule


def update_results_data_local(results_data: Dict[str, Optional[Dict]], formatted_data: Dict[str, Optional[Dict]]) -> Dict[str, Optional[Dict]]:
    # logging.info("extracting and reformatting results data")
    for meeting in results_data["meetings"]:
        for race in meeting["races"]:
            _id = race["id"]

            # the results page will be pretty empty at the start of the day... 
            # tbh only need to pull the results page once in a day
            if not _id:
                continue

            if formatted_data[_id]["got_results"]: # TODO move this check higher
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
        """
        Reformats documents with _id as key
        
        Args:
            documents (list): List of documents to reformat
        """
        try:
            # Create a dictionary with _id as key
            id_keyed_documents = {}
            for doc in documents:

                # Convert ObjectId to string for JSON serialization
                doc_id = str(doc['_id'])
                
                # Add to dictionary
                id_keyed_documents[doc_id] = doc
            return id_keyed_documents
        
        except Exception as e:
            # logging.error(f"Error exporting to JSON: {e}")
            return None
        

def extract_and_update_results(mongodb: MongoDBHandler, data_extractor: TabDataExtractor, collection_name: str):
    results_data = data_extractor.get_results_data()

    mongodb.set_collection(collection_name)
    existing_data = mongodb.get_all_documents()
    formatted_data = reformat_collection_format(existing_data)

    updated_data = update_results_data_local(results_data, formatted_data)

    for id, data in updated_data.items(): 
        mongodb.replace_document(id, data)


def extract_and_update_odds(mongodb: MongoDBHandler, data_extractor: TabDataExtractor, collection_name: str) -> bool:

    odds_data = data_extractor.get_odds_data()

    mongodb.set_collection(collection_name)
    existing_data = mongodb.get_all_documents()
    formatted_data = reformat_collection_format(existing_data)

    updated_data, need_update_schedule = update_odds_data_local(odds_data, formatted_data)

    for id, data in updated_data.items(): 
        mongodb.replace_document(id, data)

    return need_update_schedule

def extract_schedule_data(schedule_data: Dict[str, Optional[Dict]]) -> Dict[str, Optional[Dict]]:
    # logging.info("extracting and reformatting schedule data")
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
                "entries": {
                }
            }
            for entry in race["entries"]:
                formatted_data[race["id"]]["entries"][str(entry["number"])] = entry
                formatted_data[race["id"]]["entries"][str(entry["number"])]["results_plc"] = False
                formatted_data[race["id"]]["entries"][str(entry["number"])]["odds"] = {}
            race_count += 1
            
    return formatted_data

def update_schedule_missing_races(mongodb: MongoDBHandler, data_extractor: TabDataExtractor, collection_name: str):
    schedule_date = data_extractor.get_schedule_data()
    formatted_data = extract_schedule_data(schedule_date)

    mongodb.set_collection(collection_name)
    d = mongodb.get_all_documents()
    d = reformat_collection_format(d)

    for _id, data in formatted_data.items(): 
        if _id not in d.keys():
            mongodb.post_data(data)

def pull_schedule_and_create_collection(mongodb: MongoDBHandler, data_extractor: TabDataExtractor, collection_name: str):
    mongodb.create_collection(collection_name)

    schedule_date = data_extractor.get_schedule_data()
    formatted_data = extract_schedule_data(schedule_date)

    mongodb.set_collection(collection_name)
    for _, data in formatted_data.items(): 
        mongodb.post_data(data)

def find_latest_collection(mongodb: MongoDBHandler) -> str:
    collections = mongodb.get_all_collections()
    latest_collection = max(collections)
    return latest_collection

def find_first_and_final_race_of_day(mongodb: MongoDBHandler, latest_collection):
    mongodb.set_collection(latest_collection)
    d = mongodb.get_all_documents()

    time_list = []
    for race in d:
        time_list.append(race["norm_time"])

    return max(time_list)


# making big assumption around things changing at 1:30 
# things will change potentially around the final race time, but have evidence that 
# doesn't always happen (transition from 9th to 10th dec)
def pull_tab_data():
    start_time = timer.time()
    data_extractor = TabDataExtractor()
    mongodb = MongoDBHandler(database_name="tab")
    mongodb.connect()
    collection_name = convert_date_to_collection_format(date.today().strftime(DATE_FORMAT))
    logger.info(f"Current date: {collection_name}")

    if check_within_time_bounds(time(0, 00), time(5, 00)):
        logger.info("within schedule changing time bounds")
        date_in_db = mongodb.check_collection_in_db(collection_name)
        if not date_in_db:
            schedule_date = convert_date_to_collection_format(data_extractor.get_schedule_data()["date"])
            if schedule_date == collection_name:
                logger.info("updating schedule")
                pull_schedule_and_create_collection(mongodb, data_extractor, schedule_date)
            else:
                collection_name = schedule_date

    logger.info(f"Current collection: {collection_name}")

    logger.info("updating odds")
    update_schedule = extract_and_update_odds(mongodb, data_extractor, collection_name)

    if update_schedule:
        logger.info("Updating schedule with missing races")
        update_schedule_missing_races(mongodb, data_extractor, collection_name)

    # pull results
    # lets just pull every hour I guess
    now = datetime.now()
    if now.minute == 0 and 0 <= now.second <= 10:
        #TODO add check to see if results have been pulled before
        # document = collection.find_one()
        logger.info("updating results")
        extract_and_update_results(mongodb, data_extractor, collection_name)

    logging.info(f"Done for now")
    end_time = timer.time()
    logger.info(f"Execution time: {end_time - start_time} seconds")

def main():
    pull_tab_data()

if __name__ == '__main__':
    main()