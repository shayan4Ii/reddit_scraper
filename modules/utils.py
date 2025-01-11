import time

def exponential_backoff(attempt, base_delay=1):
    """Implement exponential backoff for retry logic."""
    delay = base_delay * (2 ** attempt)
    time.sleep(delay)

