from sqlalchemy import create_engine, Column, String, Integer, Text, ForeignKey, Index, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import logging

# Define the database connection and ORM base
Base = declarative_base()

# Define the Post model
class Post(Base):
    __tablename__ = 'posts'

    id = Column(String, primary_key=True)
    title = Column(Text, nullable=False)
    description = Column(Text, nullable=True)
    media_url = Column(Text, nullable=True)
    post_url = Column(Text, nullable=False, unique=True)
    num_comments = Column(Integer, nullable=False)
    score = Column(Integer, nullable=False)
    username = Column(String, nullable=False)
    created_utc = Column(String, nullable=False)
    subreddit = Column(String, nullable=False)
    is_multiple_subreddits = Column(Boolean, default=False)  # New column to track multiple subreddits

    # Relationship with comments
    comments = relationship("Comment", back_populates="post", cascade="all, delete, delete-orphan")

    # Indexes for faster queries
    __table_args__ = (
        Index('idx_subreddit', 'subreddit'),
        Index('idx_created_utc', 'created_utc'),
    )


# Define the Comment model
class Comment(Base):
    __tablename__ = 'comments'

    id = Column(Integer, primary_key=True, autoincrement=True)
    comment_id = Column(String, unique=True, nullable=False)
    post_id = Column(String, ForeignKey('posts.id'), nullable=False)  # Foreign key linking to the Post table
    username = Column(String, nullable=True)
    body = Column(Text, nullable=False)
    score = Column(Integer, nullable=False)
    created_utc = Column(String, nullable=False)

    # Relationship with posts
    post = relationship("Post", back_populates="comments")


def setup_database(db_url):
    """
    Sets up the database connection and creates the table if it doesn't exist.
    :param db_url: The database connection string (e.g., sqlite:///posts.db)
    :return: A SQLAlchemy Session factory
    """
    try:
        logging.info(f"Connecting to database at {db_url}")
        engine = create_engine(db_url)
        Base.metadata.create_all(engine)  # Create tables if they don't exist
        logging.info("Database tables created successfully")
        return sessionmaker(bind=engine)
    except Exception as e:
        logging.critical("Error setting up the database", exc_info=True)
        raise


def save_posts_to_db(session_factory, posts):
    """
    Save the scraped posts and their comments to the database using SQLAlchemy.
    :param session_factory: The SQLAlchemy Session factory
    :param posts: List of posts with their comments to save
    """
    logging.info("Saving posts and comments to database")
    Session = session_factory()

    try:
        with Session() as session:
            for post_data in posts:
                # Check if the post already exists
                existing_post = session.query(Post).filter_by(post_url=post_data['post_url']).first()
                if existing_post:
                    logging.warning(f"Post already exists in database: {post_data['post_url']} - Skipping.")
                    continue

                # Create a Post object
                post = Post(
                    id=post_data['id'],
                    title=post_data['title'],
                    description=post_data['description'],
                    media_url=post_data['media_url'],
                    post_url=post_data['post_url'],
                    num_comments=post_data['num_comments'],
                    score=post_data['score'],
                    username=post_data['username'],
                    created_utc=post_data['created_utc'],
                    subreddit=post_data['subreddit'],
                    is_multiple_subreddits=post_data['is_multiple_subreddits']  # Save the new field
                )
                session.add(post)
                logging.debug(f"Post added to session: {post_data['id']}")

                # Add comments
                for comment_data in post_data.get("comments", []):
                    existing_comment = session.query(Comment).filter_by(comment_id=comment_data['comment_id']).first()
                    if existing_comment:
                        logging.info(f"Comment already exists in database: {comment_data['comment_id']} - Skipping.")
                        continue

                    comment = Comment(
                        comment_id=comment_data['comment_id'],
                        post_id=post_data['id'],
                        username=comment_data['username'],
                        body=comment_data['body'],
                        score=comment_data['score'],
                        created_utc=comment_data['created_utc']
                    )
                    session.add(comment)
                    logging.debug(f"Comment added to session: {comment_data['comment_id']}")

            session.commit()
            logging.info("Posts and comments saved successfully to database")
    except Exception as e:
        logging.critical("Error saving posts and comments to database", exc_info=True)
        raise


def extract_post_data(self, post, subreddit_name):
    logging.debug(f"Extracting data for post: {post.title}")
    try:
        # Determine if the post is from multiple subreddits
        is_multiple_subreddits = len(post.subreddit.display_name.split(',')) > 1 if hasattr(post.subreddit, 'display_name') else False

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
            "is_multiple_subreddits": is_multiple_subreddits,  # Set the new field
        }
        return post_data
    except Exception as e:
        logging.error(f"Error extracting post data: {e}", exc_info=True)
        raise
