import schedule
import time
import logging
import sys

def schedule_scraper_job(scraper, subreddits, post_type, time_filter, post_limit, pagination_limit, interval_minutes=1):
    """
    Schedules the scraper job to run at regular intervals.
    
    :param scraper: RedditScraper instance
    :param subreddits: List of subreddit names to scrape
    :param post_type: Type of posts to fetch (e.g., 'hot', 'top', etc.)
    :param time_filter: Time filter for fetching top posts (e.g., 'day', 'week', etc.)
    :param post_limit: Maximum number of posts to fetch
    :param pagination_limit: Number of pages to scrape before stopping
    :param interval_minutes: Interval in minutes to run the scraper job (default: 1 minute)
    """
    logging.info(f"Scheduling scraper job every {interval_minutes} minutes.")
    
    # Schedule the job to run at the specified interval
    schedule.every(interval_minutes).minutes.do(
        scraper.run,
        subreddits=subreddits,
        post_type=post_type,
        time_filter=time_filter,
        post_limit=post_limit,  # Pass post_limit to run
        pagination_limit=pagination_limit  # Pass pagination_limit to run
    )
    try:
        logging.info("Scheduler started. Press Ctrl+C to stop.")
        # Keep the scheduler running indefinitely
        while True:
            schedule.run_pending()
            time.sleep(1)  # Sleep for 1 second to avoid excessive CPU usage
    except KeyboardInterrupt:
        logging.info("Scheduler stopped by user.")
        sys.exit(0)  # Exit gracefully when the user presses Ctrl+C
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        sys.exit(1)  # Exit with error status