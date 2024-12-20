import os
import sys
import time
import json
import inspect
import logging
import schedule
from datetime import datetime, timedelta


#HACK: TODO: make this pip installable
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir) 

from tab_data_extractor import TabDataExtractor

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

class OneTimeScheduler:
    def __init__(self, log_level=logging.INFO):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        
        # Create console handler if none exists
        if not self.logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(log_level)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

        self.schedule_file_name = "timing_schedule.json"
        self.debug_file_name = "test_output.json"
        self.run_offset_time = 10 # seconds
        # self.schedule_file = Path(self.schedule_file_name)
        self.running = True
        self.current_jobs = {}
        self.last_file_check = datetime.now()
        self.file_check_interval = 600  # Check for schedule updates every 600 seconds
        self.tab_data_extractor = TabDataExtractor()
        
    # def load_schedule(self):
    #     """Load schedule from JSON file."""
    #     try:
    #         with open(self.schedule_file, 'r') as f:
    #             return json.load(f)
    #     except FileNotFoundError:
    #         return {}
            
    def should_check_schedule(self):
        """Determine if it's time to check for schedule updates."""
        now = datetime.now()
        if (now - self.last_file_check).seconds >= self.file_check_interval:
            self.last_file_check = now
            return True
        return False
    
    def is_future_time(self, time_str):
        """Check if the given time is in the future for the specified date."""
        task_datetime = datetime.strptime(time_str, DATETIME_FORMAT)
        return task_datetime > datetime.now()
    
    def pull_and_reformat_tab_scheule(self):
        timing_schedule = []
        schedule = self.tab_data_extractor.get_schedule_data()
        for meeting in schedule["meetings"]:
            for race in meeting["races"]:

                # offset execution time by run_offset_time
                race_time = datetime.strptime(race["norm_time"], DATETIME_FORMAT)
                race_time = race_time - timedelta(seconds=self.run_offset_time)
                race_time = race_time.strftime(DATETIME_FORMAT)

                timing_schedule.append(
                        {
                            "norm_time": race_time, 
                            "id": race["id"]
                        }  
                    )
        return timing_schedule
    
    def pull_race_odds(self, race_id):
        odds = self.tab_data_extractor.get_odds_data()
        for meeting in odds["meetings"]:
            for race in meeting["races"]:
                if race["id"] == race_id:
                    return race
        return None
    
    def update_schedule(self):
        """Update the schedule with new trigger times."""
        # Clear existing jobs that haven't run yet
        schedule.clear()
        self.current_jobs.clear()
        
        # Load new schedule
        races = self.pull_and_reformat_tab_scheule()

        with open(self.schedule_file_name, 'w') as f:
            json.dump(races, f, indent=4)
        
        # Schedule each task for specified dates
        for race_info in races:
            # Only schedule if the time hasn't passed yet
            trigger_date_time = race_info["norm_time"]
            race_id = race_info["id"]
            if self.is_future_time(trigger_date_time):
                trigger_time = trigger_date_time.split(" ")[-1] # HACK just extracting the time
                self.logger.info(f"trigger time: {trigger_time}")

                job = schedule.every().day.at(trigger_time).do(
                    self.run_task, trigger_time, race_id
                )
                job_key = f"{race_id}_{trigger_time}"
                self.current_jobs[job_key] = job
                    
    def run_task(self, trigger_time, race_id):
        """Execute the task and remove it from schedule after completion."""
        self.logger.info(f"Running task at {trigger_time}")

        # Add your task logic here
        self.logger.info(f"race_id: {race_id}")
        odds = self.pull_race_odds(race_id)
        with open(self.debug_file_name, 'w') as f:
            json.dump(odds, f, indent=4)

        # Remove the job after it runs or if the date has passed
        job_key = f"{race_id}_{trigger_time}"
        if job_key in self.current_jobs:
            schedule.cancel_job(self.current_jobs[job_key])
            del self.current_jobs[job_key]
            
    def run(self):
        """Main loop to run the scheduler."""
        self.update_schedule()
        
        while self.running:
            if self.should_check_schedule():
                self.update_schedule()

            if not self.current_jobs:
                self.logger.info("No tasks to schedule. Exiting.")
                return
            
            schedule.run_pending()
            time.sleep(1)
            
    def stop(self):
        """Stop the scheduler."""
        self.running = False

if __name__ == "__main__":    
    # Initialize and run scheduler
    scheduler = OneTimeScheduler()
    try:
        scheduler.run()
    except KeyboardInterrupt:
        scheduler.stop()