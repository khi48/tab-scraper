"""
Some Stuff

Author: k.hitchcock
Date: 2024-12-07

TODO:
- DONE - fix day time.... a single day can run through till 5am I suspect
- pull every bit of odds, schedule and results at midnight just to confirm all data afterwards


"""

import os
import sys
import inspect
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


#HACK: TODO: make this pip installable
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir) 

from mongodb_handler import MongoDBHandler

logging.basicConfig(
    level=logging.INFO,  # Set the logging level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

# Create a logger
logger = logging.getLogger(__name__)

@dataclass
class RaceStats:
    total_races = 0
    race_scratched = 0
    race_no_results = 0
    race_no_favourite = 0
    race_plc_odds_too_low = 0
    entry_no_odds = 0
    entry_scratched = 0
    entry_no_odds_before_race = 0
    entry_invalid_payouts = 0
    entry_favourite_no_result_rank = 0
    valid_results = 0
    win_results = 0
    winners = []
    losers = []
    plc_diff = []
    win_diff = []
    profit = 0

def average_list(n):
    return sum(n) / len(n)

def average_sketchy_list(n):
    total = 0
    count = 0
    for v in n:
        if v:
            total += v
            count += 1
    return total/count


def get_all_data(mongodb: MongoDBHandler) -> List[Dict[str, Any]]:
    return mongodb.get_all_documents()

def find_latest_entry(time_dict):
    # Convert keys to datetime objects
    latest_key = max(time_dict.keys())
    return time_dict[latest_key]

def create_stripplots(pd_df, title, x_axis):
    os.makedirs('output_plots', exist_ok=True)

    # Create sample data
    # Create a figure with two subplots
    plt.figure(figsize=(10, 6))
    sns.stripplot(
        x='Odds', 
        hue='Result', 
        data=pd_df, 
        palette={'Win': 'green', 'Lose': 'red'},
        jitter=0.2,
        alpha=0.7,
        # dodge=True
    )
    plt.title(title)
    plt.xlabel(x_axis)
    plt.tight_layout()
    plt.savefig(f"output_plots/{title}_stripplot.png")
    plt.close()
    

def find_entry_before_race(race_time_str, time_dict, seconds_delta):
    race_time = datetime.strptime(race_time_str, '%Y-%m-%d %H:%M:%S')
    race_time = race_time - timedelta(seconds=seconds_delta)
    
    # Filter entries before the race time
    before_entries = {
        k: v for k, v in time_dict.items() 
        if datetime.strptime(k, '%Y-%m-%d %H:%M:%S') < race_time
    }
    
    # Raise error if no entries before race time
    if not before_entries:
        # logger.error("No entries found before the race time")
        return None
    
    # Find the entry closest to, but before, the race time
    closest_entry = max(
        before_entries.keys(), 
        key=lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
    )
    
    return time_dict[closest_entry]

def make_me_a_scatterplot(data, x_label, y_label, hue, title):
    os.makedirs('output_plots', exist_ok=True)

    # Create sample data
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=data, x=x_label, y=y_label, hue=hue)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.tight_layout()
    plt.savefig(f"output_plots/{title}_scatterplot.png")
    plt.close()


def field_extractor(data: list, time: str, field: str):
    return_data = []
    for d in data:
        return_data.append(d[time][field])
    return return_data

def top_1_placing_analysis(data: List[Dict[str, Any]]) -> int:
    """
    Searching to find the the peoples favourites (win), and betting on them to place.

    Betting $1 on each to keep things simple
    """
    stats = RaceStats()
    stats.total_races = len(data)

    for race in data:
        # race can occur on the next day as races are all over the world, may not have results for this race
        if not race["got_results"]:
            stats.race_no_results += 1
            continue    

        race_time = race["norm_time"]
        favourite = {}
        preivous_entry_scratched_cnt = stats.entry_scratched
        entries = race["entries"]
        valid_entries = []
        for _, entry in entries.items():

            # entry scratched, ignore
            if entry["scratched"]:
                stats.entry_scratched += 1
                continue
            
            # No odds for entry
            if not entry["odds"]:
                stats.entry_no_odds += 1
                continue

            # entry_latest_odds = find_latest_entry(entry['odds'])
            entry_latest_odds = find_entry_before_race(race_time, entry['odds'], seconds_delta=5)
            # entry_latest_odds = find_latest_entry(entry["odds"])

            # no odds before race, can't use that info for whole race, break out of looking at entries
            if not entry_latest_odds:
                stats.entry_no_odds_before_race += 1
                break
        
            if entry_latest_odds["scr"]:
                stats.entry_scratched += 1
                continue

            # invalid odds - potentially due to lack of people betting
            if entry_latest_odds["win"] == 0 or entry_latest_odds["plc"] == 0:
                stats.entry_invalid_payouts += 1
                continue

            # setting first favourite
            if not favourite:
                favourite = entry
                continue
            
            latest_odds_win = entry_latest_odds["win"]
            favourite_odds_win = find_entry_before_race(race_time, favourite["odds"], seconds_delta=5)["win"]
            # favourite_odds_win = find_latest_entry(favourite["odds"])["win"]


            if latest_odds_win < favourite_odds_win:
                favourite = entry

        # determine if entire race was scratched
        if preivous_entry_scratched_cnt - stats.entry_scratched == len(entries):
            stats.race_scratched += 1

        if not favourite:
            stats.race_no_favourite += 1
            # Strange error going on. Sometimes there are no valid odds before a race.. Only seems to occur after race norm_time. 
            # Maybe this happens because too few people bet on the races?
            # either way, don't seem to have enough information to make a bet before race. Ignore this race

            # looks as though if they have fixed odds then they don't show the tote odds.... Have a screenshot of this
            continue

        favourite_odds_before = find_entry_before_race(race_time, favourite["odds"], seconds_delta=5)
        favourite_plc_before = favourite_odds_before["plc"]
        favourite_win_before = favourite_odds_before["win"]
        favourite_win_plc_ratio_before = favourite_win_before/favourite_plc_before

        # deeming this not worth betting on
        # if favourite_plc_before < 1.6 or favourite_plc_before > 1.9 or favourite_win_before > 3:# favourite_win_plc_ratio_before > 2.2 or favourite_plc_before < 2.5 or favourite_plc_before > 3.2:
        #     stats.race_plc_odds_too_low += 1
        #     continue
        
        favourite_odds_after = find_latest_entry(favourite["odds"])
        # favourite_odds_after = find_entry_before_race(race_time, favourite["odds"])

        favourite_plc = favourite_odds_after["plc"]
        favourite_win = favourite_odds_after["win"]

        stats.plc_diff.append(favourite_plc-favourite_plc_before)
        stats.win_diff.append(favourite_win-favourite_win_before) 


        if "results_rank" not in favourite or (not favourite["results_rank"] == 1 and not favourite["results_rank"] == 2):
            # sometimes "also_ran" field is empty in results, so not all entries finishing get a final position.
            # these ones certainly did not place, so should count against odds
            stats.entry_favourite_no_result_rank += 1
            stats.profit -= 1
            stats.losers.append({
                "before":favourite_odds_before,
                "after": favourite_odds_after
            })
        else:
            # favourite["results_rank"] < 3 and favourite["results_rank"] > 0: 
            stats.profit += favourite_plc-1
            stats.winners.append({
                "before":favourite_odds_before,
                "after": favourite_odds_after
            })
            stats.win_results +=1

        

        stats.valid_results += 1
        
    winners_before_plc = field_extractor(stats.winners, "before", "plc")
    winners_after_plc = field_extractor(stats.winners, "after", "plc")
    losers_before_plc = field_extractor(stats.losers, "before", "plc")
    losers_after_plc = field_extractor(stats.losers, "after", "plc")
    winners_before_win = field_extractor(stats.winners, "before", "win")
    winners_after_win = field_extractor(stats.winners, "after", "win")
    losers_before_win = field_extractor(stats.losers, "before", "win")
    losers_after_win = field_extractor(stats.losers, "after", "win")


    winners_before_ffplc = field_extractor(stats.winners, "before", "ffplc")


    win_data = pd.DataFrame({
        'Odds': winners_before_win + losers_before_win,
        'Result': ['Win']*len(winners_before_win) + ['Lose']*len(losers_before_win)
    })
    create_stripplots(win_data, "WIN before", "odds")

    win_data = pd.DataFrame({
        'Odds': winners_after_win + losers_after_win,
        'Result': ['Win']*len(winners_after_win) + ['Lose']*len(losers_after_win)
    })
    create_stripplots(win_data, "WIN after", "odds")
    
    plc_data = pd.DataFrame({
        'Odds': winners_before_plc + losers_before_plc,
        'Result': ['Win']*len(winners_before_plc) + ['Lose']*len(losers_before_plc)
    })
    create_stripplots(plc_data, "PLC before", "odds")

    plc_data = pd.DataFrame({
        'Odds': winners_after_plc + losers_after_plc,
        'Result': ['Win']*len(winners_after_plc) + ['Lose']*len(losers_after_plc)
    })
    create_stripplots(plc_data, "PLC after", "odds")

    # plc_data = pd.DataFrame({
    #     'Odds': stats.winners_plc_win_ratio + stats.losers_plc_win_ratio,
    #     'Result': ['Win']*len(stats.winners_plc_before) + ['Lose']*len(stats.losers_plc_before)
    # })
    # create_stripplots(plc_data, "WIN-PLC Ratio", "odds")

    plc_diff_data = pd.DataFrame({
        'Odds': stats.plc_diff,
        'Result': ['Win']*len(stats.plc_diff)
    })
    create_stripplots(plc_diff_data, "PLC_DIFF", "diff")

    win_diff_data = pd.DataFrame({
        'Odds': stats.win_diff,
        'Result': ['Win']*len(stats.win_diff)
    })
    create_stripplots(win_diff_data, "WIN_DIFF", "diff")

    odds_before_and_after = pd.DataFrame({
            'before': winners_before_plc + losers_before_plc,
            'after': winners_after_plc + losers_after_plc,
            'result': ['Win']*len(winners_after_plc) + ['Lose']*len(losers_after_plc)
        })
    make_me_a_scatterplot(odds_before_and_after, "before", "after", "result", "before vs after plc")

    logger.info(f"Total win results: {stats.win_results}")
    logger.info(f"Total valid results: {stats.valid_results}")
    logger.info(f"Average win PLC: {average_list(winners_after_plc)}")
    logger.info(f"Average win FFPLC: {average_sketchy_list(winners_before_ffplc)}")
    logger.info(f"Win Ratio: {stats.win_results/stats.valid_results}")
    logger.info(f"stats: {vars(stats)}")

    return stats.profit

def find_latest_race_of_day(d):
    time_list = []
    for race in d:  
        time_list.append(race['norm_time'])

    max_time = max(time_list)
    logger.info(f"Latest time: {max(time_list)}")

    for race in d:
        if max_time == race['norm_time']:
            logger.info(f"race is: {race['_id']}")

def find_first_race_of_day(d):
    time_list = []
    for race in d:  
        time_list.append(race['norm_time'])

    min_time = min(time_list)
    logger.info(f"First time: {min_time}")

    for race in d:
        if min_time == race['norm_time']:
            logger.info(f"race is: {race['_id']}")

def find_next_race(d):
    time_list = []
    for race in d:  
        time_list.append(race['norm_time'])

    now_time = datetime.now()#.strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"now time: {now_time}")
    
    # Filter entries after the race time
    after_entries = {
        k for k in time_list
        if datetime.strptime(k, '%Y-%m-%d %H:%M:%S') > now_time
    }
    
    closest_entry = min(
        after_entries, 
        key=lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
    )

    logger.info(f"Next Race time: {closest_entry}")
    for race in d:
        if closest_entry == race['norm_time']:
            logger.info(f"race is: {race['_id']}")

def find_first_and_final_race_of_day(d):
    time_list = []
    for race in d:
        time_list.append(race["norm_time"])

    last_race = max(time_list)
    first_race = min(time_list)
    top_5_sorted = sorted(time_list, reverse=True)[:5]

    logger.info(f"Last race: {last_race}")
    logger.info(f"top_5_sorted: {top_5_sorted}")
    logger.info(f"First race: {first_race}")

def main():
    collection_name = "_20241212"
    # console_handler = logging.StreamHandler(sys.stdout)
    # console_handler.setLevel(logging.INFO)
    # logger.addHandler(console_handler)

    logger.info(f"Pulling from collection: {collection_name}")
    mongodb = MongoDBHandler(database_name="tab")
    mongodb.connect()
    mongodb.set_collection(collection_name=collection_name)
    d = get_all_data(mongodb)

    # find_next_race(d)
    # find_first_race_of_day(d)
    # find_latest_race_of_day(d)
    # find_first_and_final_race_of_day(d)

    logger.info(f"Analysing betting pattern")
    tally = top_1_placing_analysis(d)
    logger.info(f"Final tally: {tally}")


if __name__ == '__main__':
    main()