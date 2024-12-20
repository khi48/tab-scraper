"""
Triggers a bit according to preset conditions

Author: k.hitchcock
Date: 2024-12-17
"""
import os
import sys
import inspect
from datetime import datetime, date, timedelta

#HACK: TODO: make this pip installable
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir) 

from mongodb_handler import MongoDBHandler
from tab_data_extractor import TabDataExtractor

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
DATE_FORMAT = '%Y-%m-%d'

def convert_date_to_collection_format(date_string):
    return '_' + date_string.replace('-', '')

def trigger_bet():
    pass

def analyse_odds():
    pass

def pull_live_odds():
    pass

def get_active_date(mongodb: MongoDBHandler):
    today = date.today()
    collection_name = convert_date_to_collection_format(today.strftime(DATE_FORMAT))
    while not mongodb.check_collection_in_db(collection_name):
        today = today - timedelta(days=1)
        collection_name = convert_date_to_collection_format(today.strftime(DATE_FORMAT))
    return collection_name

def pull_schedule():
    d = []
    mongodb = MongoDBHandler(database_name="tab")
    mongodb.connect()
    collection_name = get_active_date(mongodb)
    mongodb.set_collection(collection_name)
    all_races = mongodb.get_all_documents()
    return all_races

def check_races(races, seconds_till_race):
    for race in races:
        race_time = race["norm_time"]
        race_time = datetime.strptime(race_time, DATETIME_FORMAT)
        current_time = datetime.now()

        # check within 10s of race
        if not(current_time > (race_time - timedelta(seconds=seconds_till_race)) and current_time < race_time):
            continue

        # get odds

def main():
    races = pull_schedule()
    tab_extractor = TabDataExtractor()
    odds_data = tab_extractor.get_odds_data()

    race, race_soon = check_races(races, 10)
    if not race_soon:
        return
    
    odds = pull_live_odds()
    odds_good = analyse_odds(odds)
    if odds_good:
        trigger_bet(race)

    return

if __name__ == '__main__':
    main()