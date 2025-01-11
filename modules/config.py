from decouple import config

def load_config():
    return {
        "CLIENT_ID": config("CLIENT_ID"),
        "CLIENT_SECRET": config("CLIENT_SECRET"),
        "USER_AGENT": config("USER_AGENT"),
        "DB_URL": "sqlite:///reddit_post.db",  # Use DB_URL instead of DB_PATH
        "API_RATE_LIMIT_DELAY": 2,
        "MAX_API_RETRIES": 3,
        "POST_FETCH_LIMIT": 100,  # Number of posts to fetch in a single API call
        "PAGINATION_LIMIT": 10,  # Number of pages to scrape

    }


