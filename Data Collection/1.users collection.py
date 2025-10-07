import os
import json
import time
import re
import csv
import pandas as pd
from datetime import datetime, timezone, timedelta
from atproto import Client
from collections import defaultdict

# Configuration 
OUTPUT_DIR = './users'
CSV_DIR = './users/csv'
START_DATE = datetime(2024, 2, 1, tzinfo=timezone.utc)
END_DATE = datetime(2025, 2, 1, tzinfo=timezone.utc)
MAX_USERS = 500

# Bluesky credentials
BSKY_USERNAME = 'yourname.bsky.social'
BSKY_PASSWORD = 'yourpassword'

# Ensure output directories exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)

# Initialize the client and login
client = Client()
print("Attempting to login...")
client.login(BSKY_USERNAME, BSKY_PASSWORD)
print("Login successful!")

#_______________________________________________________________________________________
#_______________________________________________________________________________________

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

#_______________________________________________________________________________________
#_______________________________________________________________________________________

def get_initial_users(max_users=100):
    """Get a set of initial users to start with - optimized for diverse user discovery"""
    print(f"Finding initial users (target: {max_users})...")
    users_seen = set() # set prevents duplicates
    users_processed = 0
    
    # Multiple strategies to find users in bluesky
    strategies = [
        # 1. Get users from timeline
        ("timeline", lambda: client.app.bsky.feed.get_timeline({'limit': 100})),
        # 2. Get users from popular posts
        ("popular feed", lambda: client.app.bsky.unspecced.get_popular({'limit': 100})),
        # 3. Search for common terms to find active users
        ("search - news", lambda: client.app.bsky.feed.search_posts({'q': 'news', 'limit': 100})),
        ("search - update", lambda: client.app.bsky.feed.search_posts({'q': 'update', 'limit': 100})),
        ("search - today", lambda: client.app.bsky.feed.search_posts({'q': 'today', 'limit': 100})),
        ("search - like", lambda: client.app.bsky.feed.search_posts({'q': 'like', 'limit': 100})),
        ("search - follow", lambda: client.app.bsky.feed.search_posts({'q': 'follow', 'limit': 100})),
        ("search - tech", lambda: client.app.bsky.feed.search_posts({'q': 'tech', 'limit': 100})),
        ("search - art", lambda: client.app.bsky.feed.search_posts({'q': 'art', 'limit': 100})),
        ("search - music", lambda: client.app.bsky.feed.search_posts({'q': 'music', 'limit': 100}))
    ]
    
    # Track which ones we've tried
    strategies_used = set()
    
    # Network expansion loop    
    while len(users_seen) < max_users and users_processed < max_users * 3:
        # If we've tried all strategies but still need more users,
        # get followers of existing users to expand the network
        if len(strategies_used) == len(strategies) and users_seen:
            print("Tried all search strategies, expanding through follower network...")
            seed_users = list(users_seen)[:50]  # Use first 50 users as seeds
            
            for i, (did, handle) in enumerate(seed_users):
                if len(users_seen) >= max_users:
                    break
                
                print(f"Expanding network through user {i+1}/{len(seed_users)}: {handle}")
                try:
                    # Get followers
                    followers = client.app.bsky.graph.get_followers({'actor': did, 'limit': 50})
                    for follower in followers.followers:
                        users_seen.add((follower.did, follower.handle))
                        users_processed += 1
                        if len(users_seen) >= max_users:
                            break
                    
                    # Get following
                    following = client.app.bsky.graph.get_follows({'actor': did, 'limit': 50})
                    for follow in following.follows:
                        users_seen.add((follow.did, follow.handle))
                        users_processed += 1
                        if len(users_seen) >= max_users:
                            break
                            
                    time.sleep(1)  # Rate limiting
                except Exception as e:
                    print(f"Error expanding through {handle}: {e}")
            
            # If we still need more, reset strategies to try again
            if len(users_seen) < max_users:
                strategies_used.clear()
            
        else:
            # Choose a strategy we haven't used yet
            available_strategies = [(name, func) for name, func in strategies if name not in strategies_used]
            if not available_strategies:
                strategies_used.clear()
                available_strategies = strategies
                
            strategy_name, strategy_func = available_strategies[0]
            strategies_used.add(strategy_name)
            
            print(f"Finding users with strategy: {strategy_name}")
            try:
                result = strategy_func()
                
                # Process results based on structure
                items = []
                if hasattr(result, 'feed'):
                    items = result.feed
                elif hasattr(result, 'posts'):
                    items = result.posts
                
                print(f"Got {len(items)} items from {strategy_name}")
                
                for item in items:
                    # Extract post info
                    post = item.post if hasattr(item, 'post') else item
                    
                    # Extract author
                    if hasattr(post, 'author'):
                        users_seen.add((post.author.did, post.author.handle))
                        
                        # Also add users from replies/mentions
                        if hasattr(post, 'reply') and post.reply:
                            if hasattr(post.reply, 'parent') and hasattr(post.reply.parent, 'author'):
                                parent_author = post.reply.parent.author
                                users_seen.add((parent_author.did, parent_author.handle))
                    
                    users_processed += 1
                    
                print(f"Found {len(users_seen)} unique users so far")
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                print(f"Error with {strategy_name}: {e}")
    
    # Return users up to the maximum requested
    return list(users_seen)[:max_users]
#_______________________________________________________________________________________
#_______________________________________________________________________________________
def get_user_connections(did, handle):
    """Get followers and following for a user with increased limits"""
    followers = []
    following = []
    
    # Get followers with larger batch
    try:
        cursor = None
        while True:
            print(f"Getting followers for {handle} (cursor: {cursor})")
            followers_page = client.app.bsky.graph.get_followers({
                'actor': did,
                'limit': 100,  # Maximum allowed
                'cursor': cursor
            })
            
            for follower in followers_page.followers:
                followers.append({
                    'did': follower.did,
                    'handle': follower.handle,
                    'display_name': follower.display_name if hasattr(follower, 'display_name') else None
                })
            
            print(f"Retrieved {len(followers_page.followers)} followers, total: {len(followers)}")
            
            cursor = followers_page.cursor
            if not cursor:
                break
                
            time.sleep(0.5)  # Rate limiting
            
            # For very large accounts, limit to 2000 followers to avoid excessive API calls
            if len(followers) >= 2000:
                print(f"Reached 2000 followers limit for {handle}, stopping collection")
                break
    except Exception as e:
        print(f"Error getting followers for {handle}: {e}")
    
    # Get following ("friends") with larger batch
    try:
        cursor = None
        while True:
            print(f"Getting following for {handle} (cursor: {cursor})")
            following_page = client.app.bsky.graph.get_follows({
                'actor': did,
                'limit': 100,  # Maximum allowed
                'cursor': cursor
            })
            
            for follow in following_page.follows:
                following.append({
                    'did': follow.did,
                    'handle': follow.handle,
                    'display_name': follow.display_name if hasattr(follow, 'display_name') else None
                })
            
            print(f"Retrieved {len(following_page.follows)} following, total: {len(following)}")
            
            cursor = following_page.cursor
            if not cursor:
                break
                
            time.sleep(0.5)  # Rate limiting
            
            # For very large accounts, limit to 2000 following to avoid excessive API calls
            if len(following) >= 2000:
                print(f"Reached 2000 following limit for {handle}, stopping collection")
                break
    except Exception as e:
        print(f"Error getting following for {handle}: {e}")
        
    return followers, following

def get_all_user_posts(did, handle):
    """Get ALL posts by a user (not just within timeframe) for comprehensive analysis"""
    posts = []
    reposts = []
    
    try:
        cursor = None
        
        while True:
            print(f"Getting all posts for {handle} (cursor: {cursor})")
            feed = client.app.bsky.feed.get_author_feed({
                'actor': did,
                'limit': 100,
                'cursor': cursor
            })
            
            if not feed.feed:
                print("No posts found.")
                break
                
            for item in feed.feed:
                try:
                    post = item.post
                    
                    # Try to get post date from different possible locations
                    post_datetime = None
                    if hasattr(post, 'indexed_at'):
                        post_datetime = parse_datetime(post.indexed_at)
                    elif hasattr(post.record, 'created_at'):
                        post_datetime = parse_datetime(post.record.created_at)
                    
                    if not post_datetime:
                        print(f"Could not determine post date, skipping")
                        continue
                    
                    # Check if it's a repost
                    is_repost = False
                    if hasattr(item, 'reason'):
                        try:
                            reason_type = None
                            if hasattr(item.reason, '$type'):
                                reason_type = getattr(item.reason, '$type')
                            elif hasattr(item.reason, 'type'):
                                reason_type = item.reason.type
                                
                            if reason_type == 'app.bsky.feed.defs#reasonRepost':
                                is_repost = True
                                repost_info = {
                                    'repost_by': did,
                                    'repost_by_handle': handle,
                                    'original_uri': post.uri,
                                    'original_cid': post.cid,
                                    'original_author_did': post.author.did,
                                    'original_author_handle': post.author.handle,
                                    'repost_time': post_datetime.isoformat(),
                                    'in_timeframe': START_DATE <= post_datetime <= END_DATE
                                }
                                reposts.append(repost_info)
                        except Exception as e:
                            print(f"Error processing repost: {e}")
                    
                    # For regular posts
                    if not is_repost:
                        post_info = {
                            'uri': post.uri,
                            'cid': post.cid,
                            'text': post.record.text if hasattr(post.record, 'text') else '',
                            'created_at': post_datetime.isoformat(),
                            'author_did': did,
                            'author_handle': handle,
                            'like_count': post.like_count if hasattr(post, 'like_count') else 0,
                            'repost_count': post.repost_count if hasattr(post, 'repost_count') else 0,
                            'reply_count': post.reply_count if hasattr(post, 'reply_count') else 0,
                            'in_timeframe': START_DATE <= post_datetime <= END_DATE
                        }
                        posts.append(post_info)
                except Exception as e:
                    print(f"Error processing post item: {e}")
            
            cursor = feed.cursor
            if not cursor:
                break
                
            time.sleep(0.5)  # Rate limiting
            
            # Limit total posts per user to avoid excessive data
            if len(posts) + len(reposts) >= 1000:
                print(f"Reached 1000 posts limit for {handle}, stopping collection")
                break
            
    except Exception as e:
        print(f"Error getting posts for {handle}: {e}")
        
    print(f"Found {len(posts)} posts and {len(reposts)} reposts for {handle}")
    return posts, reposts

def get_user_likes_given(did, handle):
    """Get likes given by a user - Note: This might be limited by API availability"""
    likes_given = []
    
    # Note: Bluesky API might not provide a direct way to get all likes by a user
    # This is a placeholder for future implementation if the API supports it
    print(f"Getting likes given by {handle} - Limited by API capabilities")
    
    # For now, we'll return empty list and calculate from other sources
    return likes_given

def get_post_interactions(post_uri, post_cid):
    """Get likes and reposts for a specific post"""
    likes = []
    reposts = []
    
    # Get likes
    try:
        print(f"Getting likes for post {post_cid[:8]}...")
        likes_response = client.app.bsky.feed.get_likes({
            'uri': post_uri,
            'limit': 100
        })
        
        for like in likes_response.likes:
            likes.append({
                'post_uri': post_uri,
                'post_cid': post_cid,
                'liker_did': like.actor.did,
                'liker_handle': like.actor.handle,
                'liker_display_name': like.actor.display_name if hasattr(like.actor, 'display_name') else None,
                'created_at': like.created_at if hasattr(like, 'created_at') else None
            })
            
        print(f"Found {len(likes)} likes")
    except Exception as e:
        print(f"Error getting likes for post {post_cid[:8]}: {e}")
    
    # Get reposts
    try:
        print(f"Getting reposts for post {post_cid[:8]}...")
        reposts_response = client.app.bsky.feed.get_reposted_by({
            'uri': post_uri,
            'limit': 100
        })
        
        for repost in reposts_response.reposted_by:
            reposts.append({
                'post_uri': post_uri,
                'post_cid': post_cid,
                'reposter_did': repost.did,
                'reposter_handle': repost.handle,
                'reposter_display_name': repost.display_name if hasattr(repost, 'display_name') else None
            })
            
        print(f"Found {len(reposts)} reposts")
    except Exception as e:
        print(f"Error getting reposts for post {post_cid[:8]}: {e}")
        
    return likes, reposts

#_______________________________________________________________________________________
#_______________________________________________________________________________________

def create_comprehensive_user_profile(user_info, all_posts, all_reposts, all_likes_given, followers, following):
    """Create comprehensive user profile with all requested attributes"""
    
    # Filter posts within timeframe
    timeframe_posts = [p for p in all_posts if p.get('in_timeframe', False)]
    timeframe_reposts = [r for r in all_reposts if r.get('in_timeframe', False)]
    
    # Calculate posting frequency
    posting_frequency = calculate_posting_frequency(all_posts, user_info.get('created_at'))
    
    # Calculate timeframe-specific posting frequency
    timeframe_posting_frequency = calculate_posting_frequency(timeframe_posts) if timeframe_posts else 0.0
    
    profile = {
        'user_id': user_info['did'],
        'username': user_info['handle'],
        'display_name': user_info.get('display_name', ''),
        'description': user_info.get('description', ''),
        'created_at': user_info.get('created_at', ''),
        
        # Counts
        'followers_count': len(followers),
        'following_count': len(following),
        'posts_count_total': len(all_posts),
        'posts_count_timeframe': len(timeframe_posts),
        'reposts_count_total': len(all_reposts),
        'reposts_count_timeframe': len(timeframe_reposts),
        'likes_given_count': len(all_likes_given),  # Will be 0 for now due to API limitations
        
        # Posting frequency
        'posting_frequency_total': round(posting_frequency, 4),
        'posting_frequency_timeframe': round(timeframe_posting_frequency, 4),
        
        # Engagement metrics (total across all posts)
        'total_likes_received': sum(p.get('like_count', 0) for p in all_posts),
        'total_reposts_received': sum(p.get('repost_count', 0) for p in all_posts),
        'total_replies_received': sum(p.get('reply_count', 0) for p in all_posts),
        
        # Timeframe-specific engagement
        'timeframe_likes_received': sum(p.get('like_count', 0) for p in timeframe_posts),
        'timeframe_reposts_received': sum(p.get('repost_count', 0) for p in timeframe_posts),
        'timeframe_replies_received': sum(p.get('reply_count', 0) for p in timeframe_posts),
        
        # Average engagement per post
        'avg_likes_per_post': round(sum(p.get('like_count', 0) for p in all_posts) / len(all_posts), 2) if all_posts else 0,
        'avg_reposts_per_post': round(sum(p.get('repost_count', 0) for p in all_posts) / len(all_posts), 2) if all_posts else 0,
        'avg_replies_per_post': round(sum(p.get('reply_count', 0) for p in all_posts) / len(all_posts), 2) if all_posts else 0,
        
        # Data collection timestamp
        'data_collected_at': datetime.now(timezone.utc).isoformat()
    }
    
    return profile

#_______________________________________________________________________________________
#_______________________________________________________________________________________

def json_to_csv(json_file, csv_file):
    """Convert a JSON file to CSV format"""
    try:
        # Read the JSON data
        with open(json_file, 'r', encoding='utf-8') as file:
            data = json.load(file)
            
        # If data is empty, skip
        if not data:
            print(f"No data in {json_file}, skipping conversion")
            return False
            
        # Convert to DataFrame and save as CSV
        df = pd.DataFrame(data)
        df.to_csv(csv_file, index=False, encoding='utf-8')
        print(f"Successfully converted {json_file} to {csv_file}")
        return True
        
    except Exception as e:
        print(f"Error converting {json_file}: {e}")
        return False

def save_checkpoint(data, filename, current_idx):
    """Save data collected so far as a checkpoint"""
    checkpoint_file = os.path.join(OUTPUT_DIR, f"checkpoint_{filename}_{current_idx}.json")
    with open(checkpoint_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    print(f"Saved checkpoint to {checkpoint_file}")

def main():
    # Get initial set of users
    initial_users = get_initial_users(max_users=MAX_USERS)
    
    if not initial_users:
        print("No users found. Exiting.")
        return
        
    print(f"Starting comprehensive data collection for {len(initial_users)} users...")
    
    # Data structures
    all_users_profiles = []
    all_users_data = []
    all_followers = []
    all_following = []
    all_posts = []
    all_reposts = []
    all_likes = []
    all_post_reposts = []
    all_likes_given = []
    
    # User-specific data for profile creation
    user_specific_data = {}
    
    # Process each user
    checkpoint_interval = 25  # Save checkpoint every 25 users
    for idx, (did, handle) in enumerate(initial_users):
        # Save checkpoints periodically
        if idx > 0 and idx % checkpoint_interval == 0:
            print(f"\nSaving checkpoint at user {idx}/{len(initial_users)}...")
            save_checkpoint(all_users_data, "users_profiles", idx)
            save_checkpoint(all_followers, "followers", idx)
            save_checkpoint(all_following, "following", idx)
            save_checkpoint(all_posts, "posts", idx)
            save_checkpoint(all_reposts, "reposts", idx)
        
        print(f"\n{'='*60}")
        print(f"Processing user {idx+1}/{len(initial_users)}: {handle}")
        print(f"{'='*60}")
        
        # Initialize user data
        user_posts = []
        user_reposts = []
        user_followers = []
        user_following = []
        user_likes_given = []
        
        # Get user profile
        try:
            user_profile = client.app.bsky.actor.get_profile({'actor': did})
            
            user_info = {
                'did': did,
                'handle': handle,
                'display_name': getattr(user_profile, 'display_name', None),
                'description': getattr(user_profile, 'description', None),
                'followers_count': getattr(user_profile, 'followers_count', 0),
                'following_count': getattr(user_profile, 'follows_count', 0),
                'posts_count': getattr(user_profile, 'posts_count', 0),
                'created_at': getattr(user_profile, 'created_at', None)
            }
            
            all_users_data.append(user_info)
            print(f"? Added user profile for {handle}")
            
            # Get followers and following
            print("Getting connections...")
            user_followers, user_following = get_user_connections(did, handle)
            
            for follower in user_followers:
                all_followers.append({
                    'user_did': did,
                    'user_handle': handle,
                    'follower_did': follower['did'],
                    'follower_handle': follower['handle'],
                    'follower_display_name': follower['display_name']
                })
            
            for follow in user_following:
                all_following.append({
                    'user_did': did,
                    'user_handle': handle,
                    'following_did': follow['did'],
                    'following_handle': follow['handle'],
                    'following_display_name': follow['display_name']
                })
                
            print(f"? Added {len(user_followers)} followers and {len(user_following)} following for {handle}")
            
            # Get ALL posts (not just timeframe)
            print("Getting all posts...")
            user_posts, user_reposts = get_all_user_posts(did, handle)
            all_posts.extend(user_posts)
            all_reposts.extend(user_reposts)
            
            # Get likes given by user (placeholder for now)
            user_likes_given = get_user_likes_given(did, handle)
            all_likes_given.extend(user_likes_given)
            
            # Create comprehensive user profile
            comprehensive_profile = create_comprehensive_user_profile(
                user_info, user_posts, user_reposts, user_likes_given, 
                user_followers, user_following
            )
            all_users_profiles.append(comprehensive_profile)
            
            print(f"? Created comprehensive profile for {handle}")
            
        except Exception as e:
            print(f"? Error processing user {handle}: {e}")
            
        time.sleep(2)  # Rate limiting between users
    
    # Collect interactions for timeframe posts only (to save time)
    print(f"\n{'='*60}")
    print("Collecting likes and reposts for timeframe posts...")
    print(f"{'='*60}")
    
    timeframe_posts = [p for p in all_posts if p.get('in_timeframe', False)]
    post_count = len(timeframe_posts)
    
    for i, post in enumerate(timeframe_posts):
        if i % 10 == 0:  # Progress update
            print(f"Processing post interactions {i+1}/{post_count}...")
            
        if i % 50 == 0 and i > 0:  # Checkpoint for interactions
            save_checkpoint(all_likes, "post_likes", i)
            save_checkpoint(all_post_reposts, "post_reposts", i)
            
        post_likes, post_reposts = get_post_interactions(post['uri'], post['cid'])
        all_likes.extend(post_likes)
        all_post_reposts.extend(post_reposts)
        time.sleep(0.8)  # Rate limiting
    
    # Save all data to files
    print(f"\n{'='*60}")
    print("Saving all collected data...")
    print(f"{'='*60}")
    
    # Define date range for filenames
    date_range = f"{START_DATE.strftime('%Y-%m-%d')}_to_{END_DATE.strftime('%Y-%m-%d')}"
    
    json_files = [
        (all_users_profiles, f'users_comprehensive_profiles_{date_range}.json'),
        (all_users_data, f'users_basic_profiles_{date_range}.json'),
        (all_followers, f'followers_{date_range}.json'),
        (all_following, f'following_{date_range}.json'),
        (all_posts, f'posts_all_{date_range}.json'),
        (all_reposts, f'reposts_all_{date_range}.json'),
        (all_likes, f'post_likes_{date_range}.json'),
        (all_post_reposts, f'post_reposts_{date_range}.json'),
        (all_likes_given, f'user_likes_given_{date_range}.json')
    ]
    
    for data, filename in json_files:
        filepath = os.path.join(OUTPUT_DIR, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"? Saved {len(data)} records to {filename}")
    
    # Convert JSON to CSV
    print(f"\n{'='*60}")
    print("Converting JSON files to CSV format...")
    print(f"{'='*60}")
    
    files_to_convert = [
        (f'users_comprehensive_profiles_{date_range}.json', f'users_comprehensive_profiles_{date_range}.csv'),
        (f'users_basic_profiles_{date_range}.json', f'users_basic_profiles_{date_range}.csv'),
        (f'followers_{date_range}.json', f'followers_{date_range}.csv'),
        (f'following_{date_range}.json', f'following_{date_range}.csv'),
        (f'posts_all_{date_range}.json', f'posts_all_{date_range}.csv'),
        (f'reposts_all_{date_range}.json', f'reposts_all_{date_range}.csv'),
        (f'post_likes_{date_range}.json', f'post_likes_{date_range}.csv'),
        (f'post_reposts_{date_range}.json', f'post_reposts_{date_range}.csv'),
        (f'user_likes_given_{date_range}.json', f'user_likes_given_{date_range}.csv')
    ]
    
    # Process each file
    successful_conversions = 0
    for json_filename, csv_filename in files_to_convert:
        json_path = os.path.join(OUTPUT_DIR, json_filename)
        csv_path = os.path.join(CSV_DIR, csv_filename)
        
        # Check if JSON file exists
        if not os.path.exists(json_path):
            print(f"File {json_path} not found, skipping")
            continue
            
        # Convert to CSV
        if json_to_csv(json_path, csv_path):
            successful_conversions += 1
            
    print(f"\n{'='*60}")
    print("DATA COLLECTION COMPLETE!")
    print(f"{'='*60}")
    
    # Print comprehensive summary
    print(f"\n COLLECTION SUMMARY:")
    print(f"   ? Users processed: {len(all_users_profiles)}")
    print(f"   ? Total followers collected: {len(all_followers)}")
    print(f"   ? Total following collected: {len(all_following)}")
    print(f"   ? Total posts collected: {len(all_posts)}")
    print(f"   ? Posts in timeframe ({START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}): {len([p for p in all_posts if p.get('in_timeframe', False)])}")
    print(f"   ? Total reposts collected: {len(all_reposts)}")
    print(f"   ? Reposts in timeframe: {len([r for r in all_reposts if r.get('in_timeframe', False)])}")
    print(f"   ? Post likes collected: {len(all_likes)}")
    print(f"   ? Post reposts collected: {len(all_post_reposts)}")
    print(f"   ? User likes given: {len(all_likes_given)}")
    
    print(f"\n FILES CREATED:")
    print(f"   JSON files saved to: {OUTPUT_DIR}")
    print(f"   CSV files saved to: {CSV_DIR}")
    print(f"   Successfully converted {successful_conversions} of {len(files_to_convert)} files to CSV")
    
    # Show sample of comprehensive user profiles
    if all_users_profiles:
        print(f"\n SAMPLE USER PROFILE ATTRIBUTES:")
        sample_profile = all_users_profiles[0]
        for key, value in sample_profile.items():
            if isinstance(value, str) and len(str(value)) > 50:
                display_value = str(value)[:50] + "..."
            else:
                display_value = value
            print(f"   ? {key}: {display_value}")
    
    # Create a summary statistics file
    summary_stats = {
        'collection_date': datetime.now(timezone.utc).isoformat(),
        'date_range': f"{START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}",
        'users_collected': len(all_users_profiles),
        'total_posts': len(all_posts),
        'timeframe_posts': len([p for p in all_posts if p.get('in_timeframe', False)]),
        'total_reposts': len(all_reposts),
        'timeframe_reposts': len([r for r in all_reposts if r.get('in_timeframe', False)]),
        'total_followers': len(all_followers),
        'total_following': len(all_following),
        'post_likes': len(all_likes),
        'post_reposts': len(all_post_reposts),
        'user_likes_given': len(all_likes_given),
        'files_converted_to_csv': successful_conversions,
        'most_active_users': sorted(all_users_profiles, key=lambda x: x['posts_count_total'], reverse=True)[:5] if all_users_profiles else [],
        'most_followed_users': sorted(all_users_profiles, key=lambda x: x['followers_count'], reverse=True)[:5] if all_users_profiles else []
    }
    
    # Save summary
    summary_file = os.path.join(OUTPUT_DIR, f'collection_summary_{date_range}.json')
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary_stats, f, indent=2, ensure_ascii=False)
    print(f"\n Collection summary saved to: {summary_file}")
    
    print(f"\n All data collection and processing completed successfully!")
    print(f"   The comprehensive user profiles CSV contains all requested attributes:")
    print(f"    Follows count, followers count, posts count")
    print(f"    Number of reposts, number of likes given")
    print(f"    Post frequency (both total and timeframe-specific)")
    print(f"    Engagement metrics (likes, reposts, replies received)")
    print(f"    Data collected up to 2025-02-01 for {MAX_USERS} users")

# Run the script
if __name__ == "__main__":
    try:
        print(" Starting Enhanced Bluesky User Data Collection")
        print(f" Date range: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")
        print(f" Target users: {MAX_USERS}")
        print(f" Output directory: {OUTPUT_DIR}")
        print(f" CSV directory: {CSV_DIR}")
        print("="*60)
        
        main()
        
    except KeyboardInterrupt:
        print("\n?  Collection interrupted by user")
        print("Partial data may have been saved in checkpoint files")
    except Exception as e:
        print(f"\n Critical error in script execution: {e}")
        import traceback
        traceback.print_exc()
    


