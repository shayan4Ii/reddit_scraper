import argparse
import logging
import sys 
from modules.scraper import RedditScraper
from modules.config import load_config
from modules.logging_config import logging 
from modules.scheduler import schedule_scraper_job

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reddit Post Scraper with Pagination")
    parser.add_argument(
        "--post_type",
        choices=["hot", "new", "top", "rising"],
        default="hot",
        help="Type of posts to fetch (default: 'hot')"
    )
    parser.add_argument(
        "--time_filter",
        choices=["hour", "day", "week", "month", "year", "all"],
        default="day",
        help="Time filter for top posts (default: 'day')"
    )
    parser.add_argument(
        "--pagination_limit",
        type=int,
        default=10,
        help="Number of pages to scrape (default: 10)"
    )
    parser.add_argument(
        "--post_limit",
        type=int,
        default=10,
        help="Number of posts to fetch (default: 10)"
    )
    parser.add_argument(
        "--subreddits",
        required=True,
        help="Comma-separated list of subreddits to scrape"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

    # Load configuration
    config = load_config()

    # Initialize the scraper with API rate limit settings from the config
    scraper = RedditScraper(
        client_id=config["CLIENT_ID"],
        client_secret=config["CLIENT_SECRET"],
        user_agent=config["USER_AGENT"],
        db_url=config["DB_URL"],  # Use 'db_url' instead of 'db_path'
        api_rate_limit_delay=config["API_RATE_LIMIT_DELAY"],  # Pass from config
        max_api_retries=config["MAX_API_RETRIES"]           # Pass from config
    )

    # Schedule scraper job
    schedule_scraper_job(
        scraper,
        subreddits=args.subreddits.split(","),
        post_type=args.post_type,
        time_filter=args.time_filter,
        post_limit=args.post_limit,
        pagination_limit=args.pagination_limit
    )

    logging.info("Scraping complete.")
    sys.exit(0)
