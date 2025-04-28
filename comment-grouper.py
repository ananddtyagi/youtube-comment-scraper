import pandas as pd
from datetime import datetime

def load_and_prepare_data(csv_path):
    """Load comments data and convert dates"""
    df = pd.read_csv(csv_path)
    df['published_at'] = pd.to_datetime(df['published_at'])
    return df

def find_ama_dates(df):
    """Find dates of AMA videos to use as period boundaries"""
    ama_videos = df[df['video_title'].str.contains('AMA', case=False, na=False)].copy()
    ama_videos = ama_videos.sort_values('published_at')
    return ama_videos['published_at'].unique()

def assign_periods(df, ama_dates):
    """Assign each comment to a period between AMAs"""
    df = df.copy()
    
    # Create period labels
    period_labels = []
    for i in range(len(ama_dates)-1):
        start_date = ama_dates[i]
        end_date = ama_dates[i+1]
        period_labels.append({
            'start': start_date,
            'end': end_date,
            'label': f"Period {i+1}: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        })
    
    # Assign period to each comment
    df['period'] = None
    for period in period_labels:
        mask = (df['published_at'] >= period['start']) & (df['published_at'] < period['end'])
        df.loc[mask, 'period'] = period['label']
    
    return df

def group_comments_by_period(df):
    """Group comments and metadata by period"""
    periods = {}
    
    for period in df['period'].unique():
        if pd.isna(period):
            continue
            
        period_data = df[df['period'] == period]
        
        periods[period] = {
            'comments': period_data['text'].tolist(),
            'video_count': period_data['video_id'].nunique(),
            'comment_count': len(period_data),
            'date_range': {
                'start': period_data['published_at'].min(),
                'end': period_data['published_at'].max()
            },
            'unique_authors': period_data['author'].nunique(),
            'videos': period_data[['video_title', 'video_id', 'video_url']].drop_duplicates().to_dict('records')
        }
    
    return periods

def save_grouped_data(periods, output_path):
    """Save grouped data to JSON"""
    import json
    from datetime import datetime
    
    # Convert datetime objects to strings
    serializable_periods = {}
    for period, data in periods.items():
        serializable_periods[period] = {
            **data,
            'date_range': {
                'start': data['date_range']['start'].strftime('%Y-%m-%d %H:%M:%S'),
                'end': data['date_range']['end'].strftime('%Y-%m-%d %H:%M:%S')
            }
        }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(serializable_periods, f, indent=2, ensure_ascii=False)

def main():
    input_file = "youtube_comments_20241109.csv"  # Your input CSV file
    output_file = "grouped_comments.json"
    
    # Load and process data
    df = load_and_prepare_data(input_file)
    ama_dates = find_ama_dates(df)
    
    if len(ama_dates) < 2:
        print("Error: Need at least 2 AMA videos to create periods")
        return
    
    # Group comments by AMA periods
    df_with_periods = assign_periods(df, ama_dates)
    periods = group_comments_by_period(df_with_periods)
    
    # Save results
    save_grouped_data(periods, output_file)
    print(f"Grouped {len(df)} comments into {len(periods)} periods")
    
    # Print summary
    for period, data in periods.items():
        print(f"\n{period}")
        print(f"Comments: {data['comment_count']}")
        print(f"Videos: {data['video_count']}")
        print(f"Unique authors: {data['unique_authors']}")

if __name__ == "__main__":
    main()