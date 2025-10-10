import datetime
import re


#this file contains utility functions used across multiple modules
def parse_datetime(datetime_str):
    """Parse datetime string safely, handling extended precision"""
    try:
        # First try direct parsing
        return datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
    except ValueError:
        # If that fails, normalize the format
        # Handle microseconds with more than 6 digits
        matches = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d+)(\+\d{2}:\d{2}|Z)', datetime_str)
        if matches:
            base_time = matches.group(1)
            microseconds = matches.group(2)[:6]  # Truncate to 6 digits
            timezone_str = matches.group(3).replace('Z', '+00:00')
            normalized_str = f"{base_time}.{microseconds}{timezone_str}"
            return datetime.fromisoformat(normalized_str)
        else:
            # If regex doesn't match, try a different approach
            return datetime.strptime(datetime_str.replace('Z', '+0000'), "%Y-%m-%dT%H:%M:%S.%f%z")


def calculate_posting_frequency(posts, user_created_at=None):
    """Calculate posting frequency (posts per day)"""
    if not posts:
        return 0.0
    
    # Get date range for posts
    post_dates = [parse_datetime(post['created_at']) for post in posts if post.get('created_at')]
    if not post_dates:
        return 0.0
    
    earliest_post = min(post_dates)
    latest_post = max(post_dates)
    
    # Use account creation date if available and earlier
    if user_created_at:
        try:
            created_date = parse_datetime(user_created_at)
            earliest_post = min(earliest_post, created_date)
        except:
            pass
    
    # Calculate days between first and last post
    days_active = (latest_post - earliest_post).days + 1
    
    return len(posts) / days_active if days_active > 0 else 0.0
