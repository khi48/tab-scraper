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
from datetime import datetime
from tab_data_extractor import TabDataExtractor
from mongodb_handler import MongoDBHandler

def schedule_extract():
    pass

def odds_extract():
    pass

def first_pull_of_day(mongodb: MongoDBHandler, data_extractor: TabDataExtractor, days_collection_name: str):
    
    # extract all data
    data = data_extractor.get_all_data()
    
    # reformat data

    # create collection
    mongodb.create_collection(days_collection_name)

    # post data to collection

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
