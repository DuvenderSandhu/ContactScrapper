# from crontab import CronTab
from api_management import get_supabase_client
from markdown import fetch_and_store_markdowns
from scraper import scrape_urls_manually,scrape_urls
supabase = get_supabase_client()

# cron = CronTab(user=True)

def createCron(urls, cronCommand, fields, selection_type="manual",css_selectors={}):
    # Create the data dictionary from the function arguments
    data = {
        "urls": urls,
        "cronCommand": cronCommand,
        "selection_type":selection_type ,
        "fields": fields,
        "css_selector": css_selectors
    }
    
    # Get Supabase client
    supabase = get_supabase_client()
    

    # Insert data into the "cron" table
    response = supabase.table("cron").insert(data).execute()
    print(response)
    # Handle the response
    if response:
        print("Cron entry created successfully:", response.data)
    else:
        print("Error:", response.error)

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import time
import threading


# Function to run the task (you can replace this with your actual task logic)
def run_task(command,cron):
    print("cron",cron)
    print(f"Executing task: {command}")
    # Replace this with the actual logic you need to execute for the cron job
    unique_names= fetch_and_store_markdowns(cron['urls'],cron['depth_value'],cron['max_url'],cron['next_button_selector'])
    print("unique_names",unique_names)
    if cron['selection_type']=='ai':
        print("unique_names",unique_names)
        in_tokens_s, out_tokens_s, cost_s, parsed_data = scrape_urls(unique_names,cron['fields'],cron['selection_type'])
        print(parsed_data)
        data= {
            "data":parsed_data
        }
        supabase.table("cron").update(data).eq("id",cron['id']).execute()
        print("Saved",unique_name)

    else:
        all_data= scrape_urls_manually(unique_names,cron['fields'],cron['selection_type'])
        print("all Data",all_data)

        data= {
            "data":all_data
        }
        supabase.table("cron").update(data).eq("id",cron['id']).execute()
        print("Updated ")

    # fetch_and_store_markdowns(cron['urls'],fields,css_selector
    print("Final Executed ")


def fetch_and_schedule_crons(scheduler):
    # Get Supabase client
    supabase = get_supabase_client()
    # Fetch all active cron jobs from the database
    response = supabase.table("cron").select("id", "cronCommand","depth_value","urls","fields","css_selector","selection_type","selection_type","next_button_selector","max_url").execute()
    print("Response",response)
    cron_jobs = response.data
    print("Response Data",response.data)
    # Create a list of existing job IDs from the scheduler
    existing_jobs = {job.id for job in scheduler.get_jobs()}

    # Loop through the cron jobs and add new ones, remove old ones
    for cron_job in cron_jobs:
        cron_command = cron_job['cronCommand']  # The cron expression (e.g., "* * * * *")
        task_name = cron_job['id']

        # If the job isn't already scheduled, add it
        if task_name not in existing_jobs:
            print(f"Scheduling new Cron Job: {task_name} with cron expression: {cron_command}")
            command = f"Executing Cron Job: {task_name}"  # Define the command or task to be executed
            scheduler.add_job(run_task, CronTrigger.from_crontab(cron_command), args=[command,cron_job], id=task_name)

    # Check for and remove jobs that are no longer in the database
    for job in scheduler.get_jobs():
        if job.id not in [cron_job['id'] for cron_job in cron_jobs]:
            print(f"Removing Cron Job: {job.id}")
            job.remove()  # Remove the job if it's not in the current cron jobs


def run_crons():
    # Create a scheduler instance
    scheduler = BackgroundScheduler()

    # Initial fetch and scheduling of cron jobs
    fetch_and_schedule_crons(scheduler)

    # Schedule the fetch_and_schedule_crons function to run every hour
    scheduler.add_job(fetch_and_schedule_crons, CronTrigger.from_crontab('* * * * *'), args=[scheduler])

    # Start the scheduler
    scheduler.start()

    # Keep the script running to allow the scheduler to continuously check and execute tasks
    try:
        while True:
            time.sleep(1)  # Sleep to keep the scheduler running
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()  # Clean up and shut down the scheduler when done


def get_cron_data():
    supabase = get_supabase_client()
    # Fetch all active cron jobs from the database
    response = supabase.table("cron").select("id", "cronCommand","depth_value","urls","fields","css_selector","selection_type","selection_type","next_button_selector","max_url","data").execute()
    print("Response",response)
    cron_jobs = response.data
    if(len(cron_jobs)!=0):
        return cron_jobs
    else:
        return []
# Start the cron jobs by calling the function

# Start the cron jobs by calling the function
if __name__ == "__main__":
    # Run crons in a separate thread so that the main process isn't blocked
    cron_thread = threading.Thread(target=run_crons)
    cron_thread.start()
