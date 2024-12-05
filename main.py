'''
Extracts data from TAB site and puts into mongoDB

Every day has a new collection with naming: _yyyymmmdd

Each race is then broken down into it's own document with each _id being the race_id


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
import json
import logging
from datetime import datetime
from typing import Dict, Optional
from tab_data_extractor import TabDataExtractor
from mongodb_handler import MongoDBHandler

logging.basicConfig(
    level=logging.INFO,  # Set the logging level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

# Create a logger
logger = logging.getLogger(__name__)

def schedule_extract():
    pass

def odds_extract():
    pass

def extract_odds_data(odds_data: Dict[str, Optional[Dict]]) -> Dict[str, Optional[Dict]]:
    pass

def extract_results_data(results_data: Dict[str, Optional[Dict]]) -> Dict[str, Optional[Dict]]:
    pass

def extract_schedule_data(schedule_data: Dict[str, Optional[Dict]]) -> Dict[str, Optional[Dict]]:
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
                "entries": {
                }
            }
            for entry in race["entries"]:
                formatted_data[race["id"]]["entries"][entry["number"]] = entry
            race_count += 1
            
    return formatted_data


def first_pull_of_day(mongodb: MongoDBHandler, data_extractor: TabDataExtractor, days_collection_name: str):
    # create collection
    # mongodb.create_collection(days_collection_name)

    # extract all data
    all_data = data_extractor.get_all_data()
    
    # reformat data
    formatted_data = {}
    formatted_data = extract_schedule_data(all_data["schedule"])
    formatted_data = extract_odds_data(all_data["odds"], formatted_data)
    formatted_data = extract_results_data(all_data["results"], formatted_data)
    

def regular_pull():
    pass


def main():
    data_extractor = TabDataExtractor()
    mongodb = MongoDBHandler(database_name="tab")
    mongodb.connect()

    days_collection_name = datetime.now().strftime('_%Y%m%d')

    first_pull_completed = mongodb.check_collection_in_db(days_collection_name)
    print(f"Day exists in db: {first_pull_completed}")

    if not first_pull_completed:
        first_pull_of_day(mongodb, data_extractor, days_collection_name)
    else:
        regular_pull()

if __name__ == '__main__':
    main()



# just dont use arrays in json please
# mongodb.update_document(3456, {"layer1": {"layer2": {"layer3": {"v1":1, "v2":2}}}})
# mongodb.append_to_existing_document(3456, {"layer1.layer2.layer4": {"v3":4}})
