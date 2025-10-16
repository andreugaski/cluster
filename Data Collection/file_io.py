import json
import pandas as pd
import os
from config import OUTPUT_DIR, CSV_DIR, START_DATE, END_DATE, MAX_USERS
from datetime import datetime, timezone

def saving_to_csv (date_range):

    # Conversion objects: (input JSON, output CSV)
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
    
    # Process each JSON file and convert to CSV
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

    return successful_conversions, files_to_convert

def save_statistics(successful_conversions, date_range, all_users_profiles,
                       all_followers, all_following,
                       all_posts, all_reposts, all_likes,
                       all_post_reposts, all_likes_given):
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
    


def saving_to_json(date_range, all_users_profiles, all_users_data,
                       all_followers, all_following,
                       all_posts, all_reposts, all_likes,
                       all_post_reposts, all_likes_given):
    # Store all data in JSON files
    json_files = [
        # _ _ _ _ USERS _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
        (all_users_profiles, f'users_comprehensive_profiles_{date_range}.json'),
        (all_users_data, f'users_basic_profiles_{date_range}.json'),
        (all_followers, f'followers_{date_range}.json'),
        (all_following, f'following_{date_range}.json'),
        (all_posts, f'posts_all_{date_range}.json'),
        (all_reposts, f'reposts_all_{date_range}.json'),
        # _ _ _ _ POSTS _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
        (all_likes, f'post_likes_{date_range}.json'),
        (all_post_reposts, f'post_reposts_{date_range}.json'),
        # _ _ _ _  NOT IMPLEMENTED YET _ _ _ _ _ _ _ _ 
        (all_likes_given, f'user_likes_given_{date_range}.json')
    ]

    # Dumping each data list to its respective JSON file
    for data, filename in json_files:
        filepath = os.path.join(OUTPUT_DIR, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"? Saved {len(data)} records to {filename}")

    return

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
