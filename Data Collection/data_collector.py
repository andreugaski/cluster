import time
from auth import authenticate_client
from utils import parse_datetime, START_DATE, END_DATE

def get_user_info (client, did, handle):
    # MAKE IT A FUNCTION LATER
    #access user profile through API
    user_profile = client.app.bsky.actor.get_profile({'actor': did})
    
    # get basic user info thorugh API
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
    return user_info


def get_user_following(did, handle):
    """Get following for a user with increased limits, returning formatted data"""
    
    client = authenticate_client()
    following = []  # This will store the formatted data
    
    try:
        cursor = None
        while True:
            print(f"Getting following for {handle} (cursor: {cursor})")
            following_page = client.app.bsky.graph.get_follows({
                'actor': did,
                'limit': 100,  # Maximum allowed
                'cursor': cursor
            })
            
            # Format each follow immediately with user context
            for follow in following_page.follows:
                following.append({
                    'user_did': did,
                    'user_handle': handle,
                    'following_did': follow.did,
                    'following_handle': follow.handle,
                    'following_display_name': follow.display_name if hasattr(follow, 'display_name') else None
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
        
    return following


def get_user_followers(did, handle):
    """Get followers for a user with increased limits, returning formatted data"""
    
    client = authenticate_client()
    followers = []  # This will store the formatted data
    
    try:
        cursor = None
        while True:
            print(f"Getting followers for {handle} (cursor: {cursor})")
            followers_page = client.app.bsky.graph.get_followers({
                'actor': did,
                'limit': 100,
                'cursor': cursor
            })
            
            # Format each follower immediately with user context
            for follower in followers_page.followers:
                followers.append({
                    'user_did': did,
                    'user_handle': handle,
                    'follower_did': follower.did,
                    'follower_handle': follower.handle,
                    'follower_display_name': follower.display_name if hasattr(follower, 'display_name') else None
                })
            
            print(f"Retrieved {len(followers_page.followers)} followers, total: {len(followers)}")
            
            cursor = followers_page.cursor
            if not cursor:
                break
                
            time.sleep(0.5)
            
            if len(followers) >= 2000:
                print(f"Reached 2000 followers limit for {handle}, stopping collection")
                break
                
    except Exception as e:
        print(f"Error getting followers for {handle}: {e}")
    
    return followers

def get_all_user_posts(did, handle):
    """Get ALL posts by a user (not just within timeframe) for comprehensive analysis"""

    client = authenticate_client()  # Get the authenticated client

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
                    
                    #check if post_datetime was successfully parsed
                    if not post_datetime:
                        print(f"Could not determine post date, skipping")
                        continue
                    
                    # Check if it's a repost or a regular post
                    is_repost = False

                    # Check for repost indication (if reason means that IS A REPOST)
                    if hasattr(item, 'reason'):
                        try:
                            reason_type = None
                            # Different possible structures for reason
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

    client = authenticate_client()  # Get the authenticated client

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

