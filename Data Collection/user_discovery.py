import time
from auth import authenticate_client

def get_initial_users(max_users=100):
    """Get a set of initial users to start with - optimized for diverse user discovery"""

    client = authenticate_client()  # Get the authenticated client

    print(f"Finding initial users (target: {max_users})...")
    users_seen = set() # set prevents duplicates
    users_processed = 0
    
    
    
    #___________________________________________________________________________________________________
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
    # This data structure can go to a different file if needed
    #__________________________________________________________________________________________________
    
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
            # 
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