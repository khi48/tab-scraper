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
    race_filtered = 0
    entry_no_odds = 0
    entry_scratched = 0
    entry_no_fixed_odds = 0
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
    failed_count = 0
    for v in n:
        if v:
            total += v
            count += 1
        else:
            failed_count += 1
    logger.info(f"Number of missing ffplcs: {failed_count}")
    if count == 0:
        return 0
    return total/count


def get_all_data(mongodb: MongoDBHandler) -> List[Dict[str, Any]]:
    return mongodb.get_all_documents()

def find_latest_entry(time_dict):
    # Convert keys to datetime objects
    latest_key = max(time_dict.keys())
    return time_dict[latest_key]

def create_stripplots(pd_df, title, x_axis, hue, subfolder="before"):
    output_dir = f'output_plots/{subfolder}'
    os.makedirs(output_dir, exist_ok=True)

    # Create sample data
    # Create a figure with two subplots
    plt.figure(figsize=(10, 6))
    sns.stripplot(
        x=x_axis, 
        hue=hue, 
        data=pd_df, 
        # palette={'Win': 'green', 'Lose': 'red'},
        jitter=0.2,
        alpha=0.7,
        # dodge=True
    )
    plt.title(title)
    plt.xlabel(x_axis)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/{title}_stripplot.png")
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

def make_me_a_scatterplot(data, x_label, y_label, hue, title, subfolder="before"):
    output_dir = f'output_plots/{subfolder}'
    os.makedirs(output_dir, exist_ok=True)

    # Create sample data
    plt.figure(figsize=(10, 6))
    sns.scatterplot(
        data=data, 
        x=x_label, 
        y=y_label, 
        hue=hue,
        alpha=0.6,  # Transparency to see overlapping points
        edgecolors='black',  # Black edge to distinguish points
        )
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/{title}_scatterplot.png")
    plt.close()


def field_extractor(data: list, time: str, field: str):
    return_data = []
    for d in data:
        return_data.append(d[time][field])
    return return_data

def top_1_placing_analysis(data: List[Dict[str, Any]], time_delta=5) -> int:
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
            entry_latest_odds = find_entry_before_race(race_time, entry['odds'], seconds_delta=time_delta)
            # entry_latest_odds = find_latest_entry(entry["odds"])

            # no odds before race, can't use that info for whole race, break out of looking at entries
            if not entry_latest_odds:
                stats.entry_no_odds_before_race += 1
                break
        
            if entry_latest_odds["scr"]:
                stats.entry_scratched += 1
                continue

            # invalid odds - potentially due to lack of people betting
            if not entry_latest_odds["win"] or not entry_latest_odds["plc"]:
                stats.entry_invalid_payouts += 1
                continue

            if not entry_latest_odds["ffwin"] or not entry_latest_odds["ffplc"]:
                stats.entry_no_fixed_odds += 1
                continue

            valid_entries.append(entry)

        sorted_entries = sorted(valid_entries, key=lambda x: find_entry_before_race(race_time, x['odds'], seconds_delta=time_delta)["ffwin"])
        
        if len(sorted_entries) < 3: #TODO: figure out if you want <3 or == 0
            stats.race_scratched += 1
            # logger.info(f"Not enough entries: {len(sorted_entries)}")
            continue
        
        favourite = sorted_entries[0]
        second_favourite = sorted_entries[1]
        third_favourite = sorted_entries[2]

        favourite_odds_before = find_entry_before_race(race_time, favourite["odds"], seconds_delta=time_delta)
        favourite_plc_before = favourite_odds_before["plc"]
        favourite_win_before = favourite_odds_before["win"]
        favourite_ffplc_before = favourite_odds_before["ffplc"]
        favourite_ffwin_before = favourite_odds_before["ffwin"]
        favourite_odds_before["win_plc_ratio"] = favourite_win_before/favourite_plc_before

        second_favourite_win_before = find_entry_before_race(race_time, second_favourite["odds"], seconds_delta=time_delta)["win"]
        third_favourite_win_before = find_entry_before_race(race_time, third_favourite["odds"], seconds_delta=time_delta)["win"]
        
        second_favourite_ffwin_before = find_entry_before_race(race_time, second_favourite["odds"], seconds_delta=time_delta)["ffwin"]
        third_favourite_ffwin_before = find_entry_before_race(race_time, third_favourite["odds"], seconds_delta=time_delta)["ffwin"]
        second_favourite_ffplc_before = find_entry_before_race(race_time, second_favourite["odds"], seconds_delta=time_delta)["ffplc"]
        third_favourite_ffplc_before = find_entry_before_race(race_time, third_favourite["odds"], seconds_delta=time_delta)["ffplc"]

        favourite_odds_before["second_favourite_diff"] = favourite_win_before-second_favourite_win_before
        favourite_odds_before["third_favourite_diff"] = favourite_win_before-third_favourite_win_before
        favourite_odds_before["second_favourite_ffdiff"] = favourite_ffwin_before-second_favourite_ffwin_before
        favourite_odds_before["third_favourite_ffdiff"] = favourite_ffwin_before-third_favourite_ffwin_before

        favourite_odds_before["second_favourite_ratio"] = favourite_win_before/second_favourite_win_before
        favourite_odds_before["third_favourite_ratio"] = favourite_win_before/third_favourite_win_before
        favourite_odds_before["second_favourite_ffwin_ratio"] = favourite_ffwin_before/second_favourite_ffwin_before
        favourite_odds_before["third_favourite_ffwin_ratio"] = favourite_ffwin_before/third_favourite_ffwin_before

        favourite_odds_before["second_favourite_ffplc_ratio"] = favourite_ffplc_before/second_favourite_ffplc_before
        favourite_odds_before["third_favourite_ffplc_ratio"] = favourite_ffplc_before/third_favourite_ffplc_before

        favourite_odds_before["tote_fixed_win_ratio"] = favourite_win_before/favourite_ffwin_before
        favourite_odds_before["tote_fixed_plc_ratio"] = favourite_plc_before/favourite_ffplc_before
        
        #  or favourite_plc_before < 1.1 improves a little
        # favourite_ffwin_before >= 2.0
        #  or favourite_win_before > 4.1 improves a litte
        if favourite_ffwin_before >= 1.7 or favourite_odds_before["tote_fixed_win_ratio"] < 1.5 or favourite_odds_before["tote_fixed_win_ratio"] > 6 or favourite_plc_before < 1.05:
            stats.race_filtered += 1
            continue 

        # if favourite_ffwin_before > 1.8 or favourite_ffplc_before > 1.04 or favourite_plc_before > 2:
        #     stats.race_filtered += 1
        #     continue 

        # works over 12-14th with 5*60-10 delay
        # if favourite_ffwin_before >= 2.1 or favourite_ffplc_before >= 1.2 or favourite_odds_before["tote_fixed_win_ratio"] <= 1.1 or favourite_plc_before <=1.1:
        #     stats.race_filtered += 1
        #     continue 

        # annoying outliers
        # if second_favourite_win_before > 60 or favourite_plc_before > 5:
        #     stats.race_filtered += 1
        #     continue

        # if favourite_odds_before["second_favourite_ffdiff"] > -3 or favourite_odds_before["third_favourite_ffplc_ratio"] > 0.7 or favourite_ffplc_before >= 1.2 or favourite_odds_before["tote_fixed_plc_ratio"] > 2 or favourite_odds_before["tote_fixed_win_ratio"] > 2:
        #     stats.race_filtered += 1
        #     continue

        # Filter works for 5*60-10 time delta
        # if favourite_plc_before < 1.4 or favourite_plc_before > 1.8 or favourite_win_before > 2.2:# favourite_win_plc_ratio_before > 2.2 or favourite_plc_before < 2.5 or favourite_plc_before > 3.2:
        #     stats.race_filtered += 1
        #     continue

        # if favourite_plc_before > 1.8:
        #     stats.race_filtered += 1
        #     continue

        # if favourite_ffwin_before > 1.8:
        #     stats.race_filtered += 1
        #     continue 

        # Working filter for 5 time delta
        # 20241212 -> bets on 7.26% of races with 0 profit
        # 20241213 -> bets on 6.9% of races with -0.29 profit
        # if favourite_ffwin_before >= 1.5 or favourite_ffplc_before <= 1 or favourite_odds_before["tote_fixed_win_ratio"] >= 2 or favourite_odds_before["tote_fixed_plc_ratio"] >= 3:
        #     stats.race_filtered += 1
        #     continue 


        # Working Filter for  5 time delta
        # _20241212 -> bets on 13.7% of races with 1.0 profit
        # _20241213 -> bets on 9.4% or races with ~0 profit
        # if favourite_odds_before["tote_fixed_win_ratio"] <= 1 or favourite_odds_before["tote_fixed_plc_ratio"] > 3 or favourite_ffwin_before >= 1.7 or favourite_odds_before["second_favourite_ffwin_ratio"] > 0.7:
        #     stats.race_filtered += 1
        #     continue


        # Working Filter for  5 time delta
        # _20241212 -> bets on 1.9% of races with 0.3 profit
        # _20241213 -> bets on 1.8% of races with 1.8 profit
        # if abs(favourite_odds_before["second_favourite_diff"]) < 8:
        #         stats.race_filtered += 1
        #         continue

        # Working Filter for 5 time delta
        # _20241212 -> bets on 2.5% of races with 0.2 profit
        # _20241213 -> bets on 2.0% of races with -0.4 profit
        # if abs(favourite_odds_before["second_favourite_diff"]) < 4.2 or favourite_plc_before < 1.2 or favourite_plc_before > 1.6:
        #     stats.race_filtered += 1
        #     continue

        # Working Filter for 5 time delta
        # _20241212 -> bets on 4.4% of races with 1.1 profit
        # _20241213 -> bets on 6.2% of races with -3.1 profit
        # if favourite_odds_before["second_favourite_ratio"] > 0.2 or favourite_plc_before > 2.4:
        #     stats.race_filtered += 1
        #     continue

        favourite_odds_after = find_latest_entry(favourite["odds"])

        if favourite_odds_after["scr"] or not favourite_odds_after["win"] or not favourite_odds_after["plc"]:
            stats.entry_invalid_payouts += 1
            continue

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

    winners_before_win_plc_ratio = field_extractor(stats.winners, "before", "win_plc_ratio")
    losers_before_win_plc_ratio = field_extractor(stats.losers, "before", "win_plc_ratio")



    winners_before_ffplc = field_extractor(stats.winners, "before", "ffplc")
    winners_after_ffplc = field_extractor(stats.winners, "after", "ffplc")
    losers_before_ffplc = field_extractor(stats.losers, "before", "ffplc")
    losers_after_ffplc = field_extractor(stats.losers, "after", "ffplc")
    winners_before_ffwin = field_extractor(stats.winners, "before", "ffwin")
    winners_after_ffwin = field_extractor(stats.winners, "after", "ffwin")
    losers_before_ffwin = field_extractor(stats.losers, "before", "ffwin")
    losers_after_ffwin = field_extractor(stats.losers, "after", "ffwin")


    winners_before_second_win_diff = field_extractor(stats.winners, "before", "second_favourite_diff")
    losers_before_second_win_diff = field_extractor(stats.losers, "before", "second_favourite_diff")
    winners_before_third_win_diff = field_extractor(stats.winners, "before", "third_favourite_diff")
    losers_before_third_win_diff = field_extractor(stats.losers, "before", "third_favourite_diff")

    winners_before_second_win_ratio = field_extractor(stats.winners, "before", "second_favourite_ratio")
    losers_before_second_win_ratio = field_extractor(stats.losers, "before", "second_favourite_ratio")
    winners_before_third_win_ratio = field_extractor(stats.winners, "before", "third_favourite_ratio")
    losers_before_third_win_ratio = field_extractor(stats.losers, "before", "third_favourite_ratio")


    winners_before_second_ffwin_diff = field_extractor(stats.winners, "before", "second_favourite_ffdiff")
    losers_before_second_ffwin_diff = field_extractor(stats.losers, "before", "second_favourite_ffdiff")
    winners_before_third_ffwin_diff = field_extractor(stats.winners, "before", "third_favourite_ffdiff")
    losers_before_third_ffwin_diff = field_extractor(stats.losers, "before", "third_favourite_ffdiff")

    winners_before_second_ffwin_ratio = field_extractor(stats.winners, "before", "second_favourite_ffwin_ratio")
    losers_before_second_ffwin_ratio = field_extractor(stats.losers, "before", "second_favourite_ffwin_ratio")
    winners_before_third_ffwin_ratio = field_extractor(stats.winners, "before", "third_favourite_ffwin_ratio")
    losers_before_third_ffwin_ratio = field_extractor(stats.losers, "before", "third_favourite_ffwin_ratio")

    winners_before_second_ffplc_ratio = field_extractor(stats.winners, "before", "second_favourite_ffplc_ratio")
    losers_before_second_ffplc_ratio = field_extractor(stats.losers, "before", "second_favourite_ffplc_ratio")


    winners_before_ffplc = field_extractor(stats.winners, "before", "ffplc")


    winners_before_win_ratio = field_extractor(stats.winners, "before", "tote_fixed_win_ratio")
    losers_before_win_ratio = field_extractor(stats.losers, "before", "tote_fixed_win_ratio")
    winners_before_plc_ratio = field_extractor(stats.winners, "before", "tote_fixed_plc_ratio")
    losers_before_plc_ratio = field_extractor(stats.losers, "before", "tote_fixed_plc_ratio")


    win_data = pd.DataFrame({
        'Odds': winners_before_win + losers_before_win,
        'Result': ['Win']*len(winners_before_win) + ['Lose']*len(losers_before_win)
    })
    create_stripplots(win_data, "WIN before", "Odds", "Result", "before")

    win_data = pd.DataFrame({
        'Odds': winners_after_win + losers_after_win,
        'Result': ['Win']*len(winners_after_win) + ['Lose']*len(losers_after_win)
    })
    create_stripplots(win_data, "WIN after", "Odds", "Result", "after")
    
    plc_data = pd.DataFrame({
        'Odds': winners_before_plc + losers_before_plc,
        'Result': ['Win']*len(winners_before_plc) + ['Lose']*len(losers_before_plc)
    })
    create_stripplots(plc_data, "PLC before", "Odds", "Result", "before")

    plc_data = pd.DataFrame({
        'Odds': winners_after_plc + losers_after_plc,
        'Result': ['Win']*len(winners_after_plc) + ['Lose']*len(losers_after_plc)
    })
    create_stripplots(plc_data, "PLC after", "Odds", "Result", "after")


    win_plc_ratio = pd.DataFrame({
        'Ratio': winners_before_win_plc_ratio + losers_before_win_plc_ratio,
        'Result': ['Win']*len(winners_before_win_plc_ratio) + ['Lose']*len(losers_before_win_plc_ratio)
    })
    create_stripplots(win_plc_ratio, "WIN-PLC Ratio", "Ratio", "Result", "before")



    # 2nd and 3rd WIN comparision

    second_win_diff_data = pd.DataFrame({
        'Win_Diff': winners_before_second_win_diff + losers_before_second_win_diff,
        'Result': ['Win']*len(winners_before_second_win_diff) + ['Lose']*len(losers_before_second_win_diff)
    })
    create_stripplots(second_win_diff_data, "Second Win Diff", "Win_Diff", "Result", "before")

    third_win_diff_data = pd.DataFrame({
        'Win_Diff': winners_before_third_win_diff + losers_before_third_win_diff,
        'Result': ['Win']*len(winners_before_third_win_diff) + ['Lose']*len(losers_before_third_win_diff)
    })
    create_stripplots(third_win_diff_data, "Third Win Diff", "Win_Diff", "Result", "before")

    
    second_win_ratio_data = pd.DataFrame({
        'Win_ratio': winners_before_second_win_ratio + losers_before_second_win_ratio,
        'Result': ['Win']*len(winners_before_second_win_ratio) + ['Lose']*len(losers_before_second_win_ratio)
    })
    create_stripplots(second_win_ratio_data, "Second Win ratio", "Win_ratio", "Result", "before")

    third_win_ratio_data = pd.DataFrame({
        'Win_ratio': winners_before_third_win_ratio + losers_before_third_win_ratio,
        'Result': ['Win']*len(winners_before_third_win_ratio) + ['Lose']*len(losers_before_third_win_ratio)
    })
    create_stripplots(third_win_ratio_data, "Third Win ratio", "Win_ratio", "Result", "before")
   

    # ---------------------------------------- FIXED ODDS ----------------------------------------

    ffwin_data = pd.DataFrame({
        'Odds': winners_before_ffwin + losers_before_ffwin,
        'Result': ['Win']*len(winners_before_ffwin) + ['Lose']*len(losers_before_ffwin)
    })
    create_stripplots(ffwin_data, "FFWIN before", "Odds", "Result", "before/fixed")

    ffwin_data = pd.DataFrame({
        'Odds': winners_after_ffwin + losers_after_ffwin,
        'Result': ['Win']*len(winners_after_ffwin) + ['Lose']*len(losers_after_ffwin)
    })
    create_stripplots(ffwin_data, "FFWIN after", "Odds", "Result", "after/fixed")
    
    ffplc_data = pd.DataFrame({
        'Odds': winners_before_ffplc + losers_before_ffplc,
        'Result': ['Win']*len(winners_before_ffplc) + ['Lose']*len(losers_before_ffplc)
    })
    create_stripplots(ffplc_data, "FFPLC before", "Odds", "Result", "before/fixed")

    ffplc_data = pd.DataFrame({
        'Odds': winners_after_ffplc + losers_after_ffplc,
        'Result': ['Win']*len(winners_after_ffplc) + ['Lose']*len(losers_after_ffplc)
    })
    create_stripplots(ffplc_data, "FFPLC after", "Odds", "Result", "after/fixed")

    # 2nd and 3rd FFWIN comparision

    second_ffwin_diff_data = pd.DataFrame({
        'FFWin_Diff': winners_before_second_ffwin_diff + losers_before_second_ffwin_diff,
        'Result': ['Win']*len(winners_before_second_ffwin_diff) + ['Lose']*len(losers_before_second_ffwin_diff)
    })
    create_stripplots(second_ffwin_diff_data, "Second Win FFDiff", "FFWin_Diff", "Result", "before/fixed")

    third_ffwin_diff_data = pd.DataFrame({
        'FFWin_Diff': winners_before_third_ffwin_diff + losers_before_third_ffwin_diff,
        'Result': ['Win']*len(winners_before_third_ffwin_diff) + ['Lose']*len(losers_before_third_ffwin_diff)
    })
    create_stripplots(third_ffwin_diff_data, "Third FFWin Diff", "FFWin_Diff", "Result", "before/fixed")

    
    second_ffwin_ratio_data = pd.DataFrame({
        'FFWin_ratio': winners_before_second_ffwin_ratio + losers_before_second_ffwin_ratio,
        'Result': ['Win']*len(winners_before_second_ffwin_ratio) + ['Lose']*len(losers_before_second_ffwin_ratio)
    })
    create_stripplots(second_ffwin_ratio_data, "Second FFWin ratio", "FFWin_ratio", "Result", "before/fixed")

    third_ffwin_ratio_data = pd.DataFrame({
        'FFWin_ratio': winners_before_third_ffwin_ratio + losers_before_third_ffwin_ratio,
        'Result': ['Win']*len(winners_before_third_ffwin_ratio) + ['Lose']*len(losers_before_third_ffwin_ratio)
    })
    create_stripplots(third_ffwin_ratio_data, "Third FFWin ratio", "FFWin_ratio", "Result", "before/fixed")


    second_ffplc_ratio_data = pd.DataFrame({
        'FFplc_ratio': winners_before_second_ffplc_ratio + losers_before_second_ffplc_ratio,
        'Result': ['Win']*len(winners_before_second_ffplc_ratio) + ['Lose']*len(losers_before_second_ffplc_ratio)
    })
    create_stripplots(second_ffplc_ratio_data, "Second FFplc ratio", "FFplc_ratio", "Result", "before/fixed")

    tote_fixed_win_ratio_data = pd.DataFrame({
        'tote_fixed_win_ratio': winners_before_win_ratio + losers_before_win_ratio,
        'Result': ['Win']*len(winners_before_win_ratio) + ['Lose']*len(losers_before_win_ratio)
    })
    create_stripplots(tote_fixed_win_ratio_data, "Tote vs Fixed Win Ratio", "tote_fixed_win_ratio", "Result", "before/fixed")

    tote_fixed_plc_ratio_data = pd.DataFrame({
        'tote_fixed_plc_ratio': winners_before_plc_ratio + losers_before_plc_ratio,
        'Result': ['Win']*len(winners_before_plc_ratio) + ['Lose']*len(losers_before_plc_ratio)
    })
    create_stripplots(tote_fixed_plc_ratio_data, "Tote vs Fixed plc Ratio", "tote_fixed_plc_ratio", "Result", "before/fixed")


    fixed_vs_tote_plc = pd.DataFrame({
            'fixed plc': winners_before_ffplc + losers_before_ffplc,
            'tote plc': winners_before_plc + losers_before_plc,
            'result': ['Win']*len(winners_before_plc) + ['Lose']*len(losers_before_plc)
        })
    make_me_a_scatterplot(fixed_vs_tote_plc, "fixed plc", "tote plc", "result", "fixed vs tote plc before", "before/fixed")


    fixed_vs_tote_win = pd.DataFrame({
            'fixed win': winners_before_ffwin + losers_before_ffwin,
            'tote win': winners_before_win + losers_before_win,
            'result': ['Win']*len(winners_before_win) + ['Lose']*len(losers_before_win)
        })
    make_me_a_scatterplot(fixed_vs_tote_win, "fixed win", "tote win", "result", "fixed vs tote win before", "before/fixed")


    # ------------------------------------------------ OVERVIEW ------------------------------------------------
    plc_diff_data = pd.DataFrame({
        'Diff': stats.plc_diff,
    })
    create_stripplots(plc_diff_data, "PLC_DIFF", "Diff", None, "overview")

    win_diff_data = pd.DataFrame({
        'Diff': stats.win_diff,
    })
    create_stripplots(win_diff_data, "WIN_DIFF", "Diff", None, "overview")

    odds_before_and_after = pd.DataFrame({
            'before': winners_before_plc + losers_before_plc,
            'after': winners_after_plc + losers_after_plc,
            'result': ['Win']*len(winners_after_plc) + ['Lose']*len(losers_after_plc)
        })
    make_me_a_scatterplot(odds_before_and_after, "before", "after", "result", "before vs after plc", "overview")

    logger.info(f"Total filtered results: {stats.race_filtered}")
    logger.info(f"Total valid results: {stats.valid_results}") 
    logger.info(f"Total win results: {stats.win_results}")
    logger.info(f"% Bet on: {stats.valid_results/(stats.valid_results+stats.race_filtered)*100}%")
    logger.info(f"Average win PLC: {average_list(winners_after_plc)}")
    logger.info(f"Average win FFPLC: {average_sketchy_list(winners_after_ffplc)}")
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
    # console_handler = logging.StreamHandler(sys.stdout)
    # console_handler.setLevel(logging.INFO)
    # logger.addHandler(console_handler)
    collection_list = []
    for i in range(14,32):
        collection_list.append(f"_202412{i}")
    
    for i in range(1,10):
        collection_list.append(f"_2025010{i}")

    d = []
    mongodb = MongoDBHandler(database_name="tab")
    mongodb.connect()

    for collection_name in collection_list:

        logger.info(f"Pulling from collection: {collection_name}")

        mongodb.set_collection(collection_name=collection_name)
        collection_d = get_all_data(mongodb)
        d.extend(collection_d)

    # find_next_race(d)
    # find_first_race_of_day(d)
    # find_latest_race_of_day(d)
    # find_first_and_final_race_of_day(d)

    logger.info(f"Analysing betting pattern")
    tally = top_1_placing_analysis(d, 5)
    logger.info(f"Final tally: {tally}")


if __name__ == '__main__':
    main()