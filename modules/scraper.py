import logging
import time
from datetime import datetime
import praw
from modules.database import Post, Comment
from modules.database import setup_database, save_posts_to_db
from praw.exceptions import RedditAPIException
from requests.exceptions import RequestException
from modules.config import load_config

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
                        # Fetch comments for the post
                        comments = self.fetch_comments(post)
                        post_data["comments"] = comments  # Add comments to post data
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

    def fetch_comments(self, post, max_comments=100):
        # logging.info(f"Fetching comments for post: {post.id}")
        comments_data = []
        try:
            post.comments.replace_more(limit=None)
            comments = post.comments.list()[:max_comments]
            for comment in comments:
                if isinstance(comment, praw.models.Comment):
                    comments_data.append({
                        "comment_id": comment.id,
                        "post_id": post.id,
                        "username": comment.author.name if comment.author else "N/A",
                        "body": comment.body,
                        "score": comment.score,
                        "created_utc": datetime.utcfromtimestamp(comment.created_utc).isoformat(),
                    })
        except Exception as e:
            logging.error(f"Error fetching comments for post {post.id}: {e}", exc_info=True)
        return comments_data

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
        """
        Save posts and their comments to the database in real-time.
        """
        Session = self.session_factory()  # Get the session class from sessionmaker

        try:
            for post_data in posts:
                # Check if the post already exists
                existing_post = Session.query(Post).filter_by(post_url=post_data["post_url"]).first()
                if existing_post:
                    logging.info(f"Duplicate post detected: {post_data['post_url']} - Skipping.")
                    continue

                # Create a Post object
                post = Post(
                    id=post_data["id"],
                    title=post_data["title"],
                    description=post_data["description"],
                    media_url=post_data["media_url"],
                    post_url=post_data["post_url"],
                    num_comments=post_data["num_comments"],
                    score=post_data["score"],
                    username=post_data["username"],
                    created_utc=post_data["created_utc"],
                    subreddit=post_data["subreddit"]
                )
                Session.add(post)
                logging.debug(f"Post added to session: {post_data['id']}")

                # Add comments, if any
                for comment_data in post_data.get("comments", []):
                    existing_comment = Session.query(Comment).filter_by(comment_id=comment_data["comment_id"]).first()
                    if existing_comment:
                        logging.info(f"Duplicate comment detected: {comment_data['comment_id']} - Skipping.")
                        continue

                    # Create a Comment object and link it to the post
                    comment = Comment(
                        comment_id=comment_data["comment_id"],
                        post_id=post_data["id"],
                        username=comment_data["username"],
                        body=comment_data["body"],
                        score=comment_data["score"],
                        created_utc=comment_data["created_utc"]
                    )
                    Session.add(comment)
                    logging.debug(f"Comment added to session: {comment_data['comment_id']}")

                # Commit after adding the post and comments
                Session.commit()
                # logging.info(f"Post {post_data['id']} and its comments saved successfully.")
            
        except Exception as e:
            logging.error(f"Error saving posts to database: {e}", exc_info=True)
            Session.rollback()
        finally:
            Session.close()
