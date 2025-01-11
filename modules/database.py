from sqlalchemy import create_engine, Column, String, Integer, Text, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging

# Define the database connection and ORM base
Base = declarative_base()

# Define the Posts model
class Post(Base):
    __tablename__ = 'posts'

    id = Column(String, primary_key=True)
    title = Column(Text, nullable=False)
    description = Column(Text, nullable=True)
    media_url = Column(Text, nullable=True)
    post_url = Column(Text, nullable=False)
    num_comments = Column(Integer, nullable=False)
    score = Column(Integer, nullable=False)
    username = Column(String, nullable=False)
    created_utc = Column(String, nullable=False)
    subreddit = Column(String, nullable=False)

    # Indexes for faster queries
    __table_args__ = (
        Index('idx_subreddit', 'subreddit'),
        Index('idx_created_utc', 'created_utc'),
    )


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
    Save the scraped posts to the database using SQLAlchemy.
    :param session_factory: The SQLAlchemy Session factory
    :param posts: List of posts to save
    """
    logging.info("Saving posts to database")
    Session = session_factory()

    try:
        with Session() as session:
            for post_data in posts:
                # Check if post exists
                existing_post = session.query(Post).filter_by(id=post_data['id']).first()

                if existing_post:
                    logging.warning(f"Post already exists in database: {post_data['id']} - Skipping.")
                    continue

                # Create a Post object and add it to the session
                post = Post(**post_data)
                session.add(post)
                logging.debug(f"Post added to session: {post_data['id']}")

            session.commit()
            logging.info("Posts saved successfully to database")
    except Exception as e:
        logging.critical("Error saving posts to database", exc_info=True)
        raise
