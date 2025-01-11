import logging
import time
import argparse
from datetime import datetime
import praw
from modules.database import Post  # Import the Post model from database.py

from modules.database import setup_database, save_posts_to_db  # Use setup_database for SQLAlchemy
from praw.exceptions import RedditAPIException
from requests.exceptions import RequestException
from modules.config import load_config  # Import the configuration

class RedditScraper:
    def __init__(self, client_id, client_secret, user_agent, db_url, api_rate_limit_delay, max_api_retries):
        logging.debug("Initializing RedditScraper")
        try:
            self.reddit = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                user_agent=user_agent
            )
            # Set up the SQLAlchemy session factory
            self.session_factory = setup_database(db_url)
            self.api_rate_limit_delay = api_rate_limit_delay
            self.max_api_retries = max_api_retries
            logging.info("RedditScraper initialized successfully with SQLAlchemy DB")
        except Exception as e:
            logging.critical("Failed to initialize RedditScraper", exc_info=True)
            raise

    def fetch_posts(self, subreddit_name, post_type="top", time_filter="day", search_query=None, sort_by_comments=False, post_limit=100, pagination_limit=None):
        logging.info(f"Fetching posts from subreddit: {subreddit_name}, post_type: {post_type}")
        posts = []
        seen_urls = set()
        after = None
        total_posts_fetched = 0
        current_page = 0

        total_posts_to_fetch = pagination_limit * 100 if pagination_limit else 100

        while current_page < (pagination_limit if pagination_limit else 1):
            try:
                logging.info(f"Fetching page {current_page + 1}, starting after: {after}")
                subreddit = self.reddit.subreddit(subreddit_name)
                
                if post_type == "hot":
                    posts_listing = list(subreddit.hot(limit=100, params={'after': after} if after else None))
                elif post_type == "new":
                    posts_listing = list(subreddit.new(limit=100, params={'after': after} if after else None))
                elif post_type == "top":
                    posts_listing = list(subreddit.top(time_filter=time_filter, limit=100, params={'after': after} if after else None))
                elif post_type == "rising":
                    posts_listing = list(subreddit.rising(limit=100, params={'after': after} if after else None))
                elif post_type == "relevance":
                    if not search_query:
                        raise ValueError("Search query is required for relevance-based posts.")
                    posts_listing = list(subreddit.search(query=search_query, sort="relevance", limit=100, params={'after': after} if after else None))
                else:
                    raise ValueError("Invalid post_type")

                if not posts_listing:
                    logging.info("No more posts available.")
                    break

                logging.info(f"Retrieved {len(posts_listing)} posts in current batch")

                batch = []
                for post in posts_listing:
                    post_data = self.extract_post_data(post, subreddit_name)
                    if post_data["post_url"] not in seen_urls:
                        batch.append(post_data)
                        seen_urls.add(post_data["post_url"])

                posts.extend(batch)
                total_posts_fetched += len(batch)
                if posts_listing:
                    after = posts_listing[-1].fullname
                    logging.info(f"Next page will start after post: {after}")
                else:
                    break

                current_page += 1
                logging.info(f"Completed page {current_page}")
                logging.info(f"Total posts fetched so far: {total_posts_fetched}")
                time.sleep(self.api_rate_limit_delay)  # Use the configurable delay

            except Exception as e:
                logging.error(f"Error during fetch: {str(e)}", exc_info=True)
                break

        logging.info(f"Finished fetching {len(posts)} total posts from {subreddit_name}")
        return posts

    def extract_post_data(self, post, subreddit_name):
        logging.debug(f"Extracting data for post: {post.title}")
        try:
            post_data = {
                "id": post.id,
                "title": post.title,
                "description": post.selftext,
                "media_url": post.url,
                "post_url": f"https://www.reddit.com{post.permalink}",
                "num_comments": post.num_comments,
                "score": post.score,
                "username": post.author.name if post.author else "N/A",
                "created_utc": datetime.utcfromtimestamp(post.created_utc).isoformat(),
                "subreddit": subreddit_name,
            }
            return post_data
        except Exception as e:
            logging.error(f"Error extracting post data: {e}", exc_info=True)
            raise

    def run(self, subreddits, post_type, time_filter, pagination_limit, post_limit=None):
        for subreddit_name in subreddits:
            logging.info(f"Processing subreddit: {subreddit_name}")
            try:
                posts = self.fetch_posts(
                    subreddit_name=subreddit_name,
                    post_type=post_type,
                    time_filter=time_filter,
                    pagination_limit=pagination_limit,
                    post_limit=post_limit
                )
                self.save_posts_with_duplicates(posts)
                time.sleep(self.api_rate_limit_delay)  # Use the configurable delay
            except Exception as e:
                logging.error(f"Error processing subreddit '{subreddit_name}': {e}", exc_info=True)
        logging.info("Completed scraping all subreddits. Exiting the program.")
        exit(0)

    def save_posts_with_duplicates(self, posts):
        # Use the session factory from SQLAlchemy
        Session = self.session_factory()
        try:
            for post in posts:
                existing_post = Session.query(Post).filter_by(post_url=post["post_url"]).first()
                if existing_post:
                    logging.info(f"Duplicate post detected: {post['post_url']} - Dropping duplicate.")
                else:
                    post_obj = Post(**post)  # Using the Post ORM model
                    Session.add(post_obj)
            Session.commit()
            logging.info(f"Batch of posts saved to database")
        except Exception as e:
            logging.error(f"Error saving posts to database: {e}", exc_info=True)
            Session.rollback()
        finally:
            Session.close()
