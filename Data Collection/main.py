from user_discovery import get_initial_users
from config import MAX_USERS, START_DATE, END_DATE, OUTPUT_DIR, CSV_DIR
from file_io import save_checkpoint, saving_to_csv, saving_to_json, save_statistics
from auth import authenticate_client
from data_collector import get_user_info, get_user_following, get_user_followers, get_all_user_posts, get_user_likes_given, get_post_interactions
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
        
    print(f"Starting data collection for {len(initial_users)} users...")
    

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

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
            # - - - - - - - - - - - - GET BASIC USER INFO - - - - - - - - - - - - - - - - - - - - - - - 
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

            print("Getting basic user information...")

            user_info = get_user_info (client, did, handle)
            all_users_data.append(user_info)

            print(f"? Added user profile for {handle}")

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
            # - - - - - - - - - - - GET FOLLOWERS & FOLLOWING LIST - - - - - - - - - - - - - - - - - - - 
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

            print("Getting user connections...")

            user_followers = get_user_followers(did, handle)
            all_followers.extend(user_followers)  

            user_following = get_user_following(did, handle)
            all_following.extend(user_followers)  
                        
                
            print(f"? Added {len(user_followers)} followers and {len(user_following)} following for {handle}")
            
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
            # - - - - - - - - - - - - GET POSTS OF THE USER - - - - - - - - - - - - - - - - - - - - - -
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

            # Get ALL posts (not just timeframe)
            print("Getting all posts from the user...")

            user_posts, user_reposts = get_all_user_posts(did, handle)
            all_posts.extend(user_posts)
            all_reposts.extend(user_reposts)

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
            # - - - - - - - - - - - - GET LIKES OF THE USER - - - - - - - - - - - - - - - - - - - - - -
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            
            user_likes_given = get_user_likes_given(did, handle)
            all_likes_given.extend(user_likes_given)

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
            # - - - - - - - CREATE A USER PROFILE WITH THE COLLECTED DATA - - - - - - - - - - - - - - -
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            
            # Create comprehensive user profile
            comprehensive_profile = create_comprehensive_user_profile(
                user_info, user_posts, user_reposts, user_likes_given, 
                user_followers, user_following
            )

            #profile of all users with all attributes
            all_users_profiles.append(comprehensive_profile)
            
            print(f"? Created comprehensive profile for {handle}")
            
        except Exception as e:
            print(f"? Error processing user {handle}: {e}")
            
        time.sleep(2)  # Rate limiting between users
    
    # Collect interactions for timeframe posts only (to save time)
    print(f"\n{'='*60}")
    print("Collecting likes and reposts for timeframe posts...")
    print(f"{'='*60}")
    

    #  _ _ _ _ _ _ SEGREGATE BETTER THIS IS POST PART_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
    # _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
    # Posts block section
    timeframe_posts = [p for p in all_posts if p.get('in_timeframe', False)]
    post_count = len(timeframe_posts)
    
    # from posts of each user, get likes and repsots
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
    

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
    # - - - - - - - - - - - - FILE CREATION AND HANDLING - - - - - - - - - - - - - - - - - - - - 
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
    
    date_range = f"{START_DATE.strftime('%Y-%m-%d')}_to_{END_DATE.strftime('%Y-%m-%d')}"

    print("Saving all collected data in JSON...")
    
    saving_to_json(date_range, all_users_profiles, all_users_data,
                       all_followers, all_following,
                       all_posts, all_reposts, all_likes,
                       all_post_reposts, all_likes_given)

    print("Converting JSON files to CSV format...")
    
    successful_conversions, files_to_convert = saving_to_csv(date_range)
    
    print ("Saving statistics...")

    summary_file = save_statistics(
        successful_conversions, date_range, all_users_profiles,
        all_followers, all_following, all_posts, all_reposts, 
        all_likes, all_post_reposts, all_likes_given, files_to_convert
    )
    
    print(f" Comprehensive summary saved to: {summary_file}")

    print("DATA COLLECTION COMPLETE!")

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

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