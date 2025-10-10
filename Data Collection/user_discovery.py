import time
from auth import authenticate_client


def get_search_strategies(client):
    """Return a list of search strategies to discover users."""
    return [
        ("timeline", lambda: client.app.bsky.feed.get_timeline({'limit': 100})),
        ("popular feed", lambda: client.app.bsky.unspecced.get_popular({'limit': 100})),
        ("search - news", lambda: client.app.bsky.feed.search_posts({'q': 'news', 'limit': 100})),
        ("search - update", lambda: client.app.bsky.feed.search_posts({'q': 'update', 'limit': 100})),
        ("search - today", lambda: client.app.bsky.feed.search_posts({'q': 'today', 'limit': 100})),
        ("search - like", lambda: client.app.bsky.feed.search_posts({'q': 'like', 'limit': 100})),
        ("search - follow", lambda: client.app.bsky.feed.search_posts({'q': 'follow', 'limit': 100})),
        ("search - tech", lambda: client.app.bsky.feed.search_posts({'q': 'tech', 'limit': 100})),
        ("search - art", lambda: client.app.bsky.feed.search_posts({'q': 'art', 'limit': 100})),
        ("search - music", lambda: client.app.bsky.feed.search_posts({'q': 'music', 'limit': 100}))
    ]


def expand_user_network(users_seen, client, max_users):
    """Expand through followers/following of known users."""
    users_processed = 0
    seed_users = list(users_seen)[:50]

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

    return users_seen


def discover_users_with_strategy(strategies_used, users_seen, users_processed, strategies):
    """Apply one unused strategy to discover users."""
    # Choose a strategy not used yet
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

    return users_seen, users_processed


def get_initial_users(max_users=100):
    """Get a set of initial users to start with - optimized for diverse user discovery."""

    client = authenticate_client()
    print(f"Finding initial users (target: {max_users})...")

    users_seen = set()
    users_processed = 0
    strategies = get_search_strategies(client)
    strategies_used = set()

    while len(users_seen) < max_users and users_processed < max_users * 3:
        if len(strategies_used) == len(strategies) and users_seen:
            print("Tried all search strategies, expanding through follower network...")
            users_seen = expand_user_network(users_seen, client, max_users)

            if len(users_seen) < max_users:
                strategies_used.clear()
        else:
            users_seen, users_processed = discover_users_with_strategy(
                strategies_used, users_seen, users_processed, strategies
            )

    return list(users_seen)[:max_users]
