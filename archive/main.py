'''
Extracts data from TAB site and puts into mongoDB

Every day has a new collection with naming: _yyyymmmdd

Each race is then broken down into it's own document with each _id being the race_id


General notes:
- Seems to be very little data in mornings. Results is pretty much empty and there is only a little on odds

MongoDB JSON Schema:

schema = {
    "_id": "string",
    "meeting_name": "string",
    "race_name": "string",
    "race_number": "interger",
    "race_norm_time": "string",
    "race_length": "string",
    "entries": {
        entry_number: {
            # all entry info
            odds: {
                time : {
                }
            }
        }
    }
}
'''
import sys
import json
import time
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Any
from tab_data_extractor import TabDataExtractor
from mongodb_handler import MongoDBHandler

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

logging.basicConfig(
    level=logging.INFO,  # Set the logging level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt=DATETIME_FORMAT,
)

# Create a logger
logger = logging.getLogger(__name__)


def extract_odds_data(odds_data: Dict[str, Optional[Dict]], formatted_data: Dict[str, Optional[Dict]]) -> Dict[str, Optional[Dict]]:
    # logging.info("extracting and reformatting odds data")
    now = datetime.now()
    timestamp = now.strftime(DATETIME_FORMAT)
    for meeting in odds_data["meetings"]:
        for race in meeting["races"]:
            _id = race["id"]

            race_time = formatted_data[_id]["norm_time"]
            race_time = datetime.strptime(race_time, DATETIME_FORMAT)
            current_time = datetime.now()

            if current_time < (race_time - timedelta(hours=2)):
                #logger.info(f"Race: {_id} too far in future, no point collecting now")
                continue

            if formatted_data[_id]["got_results"]: # TODO move this check higher
                #logger.info(f"Already got results for race: {_id}")
                continue

            for entry in race["entries"]:
                entry_num = str(entry["number"])
                if not formatted_data[_id]["entries"][entry_num]["scratched"]:
                    formatted_data[_id]["entries"][entry_num]["odds"][timestamp] = entry 

    return formatted_data

def extract_results_data(results_data: Dict[str, Optional[Dict]], formatted_data: Dict[str, Optional[Dict]]) -> Dict[str, Optional[Dict]]:
    # logging.info("extracting and reformatting results data")
    for meeting in results_data["meetings"]:
        for race in meeting["races"]:
            _id = race["id"]

            # the results page will be pretty empty at the start of the day... 
            # tbh only need to pull the results page once in a day
            if not _id:
                # #logger.info(f"No results for race {_id}, skipping")
                continue

            if formatted_data[_id]["got_results"]: # TODO move this check higher
                #logger.info(f"Already got results for race: {_id}")
                continue
            
            if race["placings"] == []:
                #logger.info(f"No placings for race {_id}")
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
            #logger.info(f"Updated results for race: {_id}")
            formatted_data[_id]["got_results"] = True

    return formatted_data

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


def first_pull_of_day(mongodb: MongoDBHandler, data_extractor: TabDataExtractor, days_collection_name: str):

    logging.info("Running first pull of the day")

    # extract all data
    # logging.info("Pulling information")
    all_data = data_extractor.get_all_data()
    
    # reformat data
    formatted_data = {}
    # start_pulling = time.time()
    formatted_data = extract_schedule_data(all_data["schedule"])
    formatted_data = extract_odds_data(all_data["odds"], formatted_data)
    formatted_data = extract_results_data(all_data["results"], formatted_data)
    # finish_pulling = time.time()
    #logger.info(f"Extract all data: {start_pulling - finish_pulling} seconds")


    # with open('output.json', 'w') as f:
    #     json.dump(formatted_data, f, indent=4)

    # create collection
    logging.info("Creating collection db")
    mongodb.create_collection(days_collection_name)

    # push data
    logging.info(f"Posting to {days_collection_name} collection")
    mongodb.set_collection(days_collection_name)
    for _, data in formatted_data.items(): 
        mongodb.post_data(data)
    
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
        

# HACK: this whole function is hacky. Rather than pulling all the data from db and  updating that,
# could just pull data from TAB and push it to individual parts... oh well
def regular_pull(mongodb: MongoDBHandler, data_extractor: TabDataExtractor, days_collection_name: str):
    regular_pull_start = time.time()
    mongodb.set_collection(days_collection_name)
    set_collection_time = time.time()
    logger.info(f"Set Collection time: {set_collection_time-regular_pull_start} seconds")

    start_pulling = time.time()
    logging.info(f"Pulling all documents from {days_collection_name}")
    existing_data = mongodb.get_all_documents()
    existing_data = reformat_collection_format(existing_data)
    finish_pulling = time.time()
    logger.info(f"Time to pull and reformat data from mongodb:  {finish_pulling-start_pulling} seconds")


    start_pulling = time.time()
    odds_data = data_extractor.get_odds_data()
    results_data = data_extractor.get_results_data()
    finish_pulling = time.time()
    logger.info(f"Time pull from TAB:  {finish_pulling-start_pulling} seconds")
    # TODO: Don't be lazy and check if we already have results on some of them, don't need to go back through and look
    
    start_pulling = time.time()
    updated_data = extract_odds_data(odds_data, existing_data)
    updated_data = extract_results_data(results_data, updated_data)
    finish_pulling = time.time()
    logger.info(f"Time to reformat data: {start_pulling - finish_pulling} seconds")

    # HACK: Literally just reposting overtop of existing data rather than updating subsection
    logging.info(f"Updating documents in {days_collection_name} collection")
    start_update = time.time()
    for id, data in updated_data.items(): 
        mongodb.replace_document(id, data)
    finish_update = time.time()
    logger.info(f"Update mongoDB: {finish_update-start_update} seconds")

    regular_pull_finish = time.time()
    logger.info(f"Regular Pull time: {regular_pull_finish-regular_pull_start} seconds")


def convert_date(date_string):
    return '_' + date_string.replace('-', '')

def pull_tab_data():
    start_time = time.time()
    data_extractor = TabDataExtractor()
    mongodb = MongoDBHandler(database_name="tab")
    mongodb.connect()

    # days_collection_name = datetime.now().strftime('_%Y%m%d')
    schedule_date = data_extractor.get_schedule_data()["date"]
    days_collection_name = convert_date(schedule_date)
    middle_time = time.time()
    logger.info(f"Time to setup and pull one schedul: {middle_time - start_time} seconds")

    first_pull_completed = mongodb.check_collection_in_db(days_collection_name)
    logging.info(f"Day exists in db: {first_pull_completed}")

    if not first_pull_completed:
        first_pull_of_day(mongodb, data_extractor, days_collection_name)
    else:
        regular_pull(mongodb, data_extractor, days_collection_name)

    logging.info(f"Done for now")
    end_time = time.time()
    logger.info(f"Execution time: {end_time - start_time} seconds")

    
def main():
    pull_tab_data()

if __name__ == '__main__':
    main()


'''

New structure:

- pull schedule
- check date
- check in db
    - if not in db:
        - create collection 
        - post schedule format
    
- post odds update as single post? Is that more efficient
- post results update near the end of the day?
Need to know when TAB switches day -> 1:30? Can take the hit at that time. At 1:35, do the check and see if day changed



'''