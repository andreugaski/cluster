from utils import calculate_posting_frequency
import datetime
from datetime import timezone

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