from user_discovery import get_initial_users
from config import MAX_USERS, START_DATE, END_DATE, OUTPUT_DIR, CSV_DIR
from file_io import save_checkpoint, json_to_csv
from auth import authenticate_client
from data_collector import get_user_connections, get_all_user_posts, get_user_likes_given, get_post_interactions
from data_processor import create_comprehensive_user_profile
import time
import os
import json
from datetime import datetime, timezone

def main():

    # Authenticate and get client
    client = authenticate_client()

    # Get initial set of users
    initial_users = get_initial_users(max_users=MAX_USERS)
    
    # Check if any users were found
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
        
        # Initialize user data (whiped each iteration)
        user_posts, user_reposts, user_followers, user_following, user_likes_given = []

        # Get user profile
        try:

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
            
            # Append to all users data list the basic info dictionary
            all_users_data.append(user_info)
            print(f"? Added user profile for {handle}")
            
            # Get followers and following
            print("Getting connections...")
            user_followers, user_following = get_user_connections(did, handle)
            
            # Append to global lists with user context
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

            # Get all posts and reposts
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