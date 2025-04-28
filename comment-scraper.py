from googleapiclient.discovery import build
from datetime import datetime
import pandas as pd
import time

def setup_youtube_api(api_key):
    """Initialize YouTube API client"""
    return build('youtube', 'v3', developerKey=api_key)

def is_short(youtube, video_id):
    """Check if a video is a Short based on duration and vertical aspect ratio"""
    try:
        response = youtube.videos().list(
            part='contentDetails,snippet',
            id=video_id
        ).execute()
        
        if not response['items']:
            return True  # Skip if video data can't be fetched
            
        video_data = response['items'][0]
        
        # Get duration in seconds
        duration = video_data['contentDetails']['duration']
        # Convert PT1M20S format to seconds
        duration_sec = sum(
            int(x[:-1]) * {'H': 3600, 'M': 60, 'S': 1}[x[-1]]
            for x in duration.replace('PT', '').replace('H', 'H ').replace('M', 'M ').replace('S', 'S ').split()
            if x
        )
        
        # Check if video has #shorts in title or description
        snippet = video_data['snippet']
        has_shorts_tag = (
            '#shorts' in snippet.get('title', '').lower() or
            '#shorts' in snippet.get('description', '').lower()
        )
        
        # Consider it a Short if duration is ≤ 60 seconds or has #shorts tag
        return duration_sec <= 60 or has_shorts_tag
        
    except Exception as e:
        print(f"Error checking if video {video_id} is a Short: {str(e)}")
        return True  # Skip on error

def get_channel_videos(youtube, channel_id, max_results=50):
    """Get regular videos (not Shorts) from a channel"""
    videos = []
    
    request = youtube.search().list(
        part='id,snippet',
        channelId=channel_id,
        maxResults=50,  # Max allowed per request
        order='date',
        type='video'
    )
    
    while request and len(videos) < max_results:
        response = request.execute()
        
        for item in response.get('items', []):
            if item['id']['kind'] == 'youtube#video':
                video_id = item['id']['videoId']
                
                # # Skip if it's a Short
                # if is_short(youtube, video_id):
                #     continue
                    
                videos.append({
                    'video_id': video_id,
                    'title': item['snippet']['title'],
                    'published_at': item['snippet']['publishedAt']
                })
                
                if len(videos) >= max_results:
                    break
        
        request = youtube.search().list_next(request, response)
        time.sleep(0.1)  # Respect API quotas
        
    return videos

def get_video_comments(youtube, video_id, max_comments=100):
    """Get comments for a specific video"""
    comments = []
    
    request = youtube.commentThreads().list(
        part='snippet',
        videoId=video_id,
        maxResults=min(max_comments, 100),
        textFormat='plainText'
    )
    
    while request and len(comments) < max_comments:
        try:
            response = request.execute()
            
            for item in response['items']:
                comment = item['snippet']['topLevelComment']['snippet']
                comments.append({
                    'author': comment['authorDisplayName'],
                    'text': comment['textDisplay'],
                    'likes': comment['likeCount'],
                    'published_at': comment['publishedAt']
                })
            
            request = youtube.commentThreads().list_next(request, response)
            
        except Exception as e:
            print(f"Error fetching comments for video {video_id}: {str(e)}")
            break
            
        time.sleep(0.1)  # Respect API quotas
    
    return comments

def scrape_channel_comments(api_key, channel_id, max_videos=10, max_comments_per_video=100):
    """Main function to scrape comments from a channel's regular videos (excluding Shorts)"""
    youtube = setup_youtube_api(api_key)
    all_comments = []
    
    # Get regular videos from channel
    print("Fetching regular videos (excluding Shorts)...")
    videos = get_channel_videos(youtube, channel_id, max_videos)
    print(f"Found {len(videos)} regular videos")
    
    # Get comments for each video
    for i, video in enumerate(videos, 1):
        print(f"Processing video {i}/{len(videos)}: {video['title']}")
        video_comments = get_video_comments(youtube, video['video_id'], max_comments_per_video)
        
        # Add video information to each comment
        for comment in video_comments:
            comment['video_id'] = video['video_id']
            comment['video_title'] = video['title']
            comment['video_url'] = f"https://www.youtube.com/watch?v={video['video_id']}"
        
        all_comments.extend(video_comments)
        print(f"Collected {len(video_comments)} comments")
    
    # Convert to DataFrame
    df = pd.DataFrame(all_comments)
    
    # Convert timestamp strings to datetime
    if not df.empty:
        df['published_at'] = pd.to_datetime(df['published_at'])
    
    return df

def save_comments(df, filename):
    """Save comments to CSV file"""
    df.to_csv(filename, index=False, encoding='utf-8')
    print(f"Saved {len(df)} comments to {filename}")

# Example usage
if __name__ == "__main__":
    API_KEY = YOUTUBE_API_KEY
    CHANNEL_ID = CHANNEL_ID
    
    df = scrape_channel_comments(
        api_key=API_KEY,
        channel_id=CHANNEL_ID,
        max_videos=200,
        max_comments_per_video=100
    )
    
    save_comments(df, f"youtube_comments_{datetime.now().strftime('%Y%m%d')}.csv")

    
    
    # var ytInitialData = {"responseContext":{"serviceTrackingParams":[{"service":"GFEEDBACK","params":[{"key":"route","value":"channel."},{"key":"is_owner","value":"false"},{"key":"is_alc_surface","value":"false"},{"key":"browse_id","value":"UC2utj8E4Z1p_RQAJzFLLtIw"},{"key":"browse_id_prefix","value":""},{"key":"logged_in","value":"1"},{"key":"e","value":"23804281,23966208,23986015,24004644,24077241,24108448,24166867,24181174,24241378,24439361,24453989,24495712,24499533,24542367,24548629,24566687,24699899,39325347,39325801,39325818,39325854,39326587,39326596,39326613,39326617,39326986,39327050,39327102,39327297,39327328,39327561,39327574,39327591,39327598,39327635,39327662,39327677,39327743,39327834,39327846,39327897,39327967,51009781,51010235,51017346,51020570,51025415,51030101,51037342,51037349,51050361,51053689,51057848,51057857,51063643,51064835,51072748,51091058,51095478,51098299,51111738,51115184,51117319,51124104,51125020,51129210,51133103,51134506,51141472,51145218,51152050,51156054,51157411,51157841,51158514,51160545,51165467,51169118,51174916,51176421,51176511,51178314,51178337,51178340,51178351,51178982,51182850,51183910,51184990,51194137,51195231,51204329,51213773,51217504,51220160,51221152,51222382,51222973,51223962,51226858,51227037,51227408,51227776,51228350,51230241,51230478,51231814,51236019,51237842,51239093,51241028,51242448,51243940,51248255,51248734,51251836,51255676,51255680,51255743,51256084,51257897,51257902,51257911,51257918,51258066,51259133,51263448,51265339,51265358,51265367,51266454,51268347,51272458,51273608,51274583,51275694,51275782,51276557,51276565,51281227,51282069,51282086,51282792,51285052,51285419,51285717,51287196,51287500,51288520,51289924,51289931,51289938,51289954,51289961,51289972,51290045,51294322,51294343,51295132,51295574,51296439,51298019,51298021,51299626,51299710,51299724,51299977,51300001,51300018,51300176,51300241,51300533,51300699,51302492,51302680,51302893,51303665,51303667,51303669,51303789,51304155,51305580,51305839,51306259,51307502,51308045,51308060,51308098,51309314,51310323,51310742,51311029,51311040,51311505,51311989,51312150,51312688,51313149,51313765,51313767,51314158,51314685,51314694,51314699,51314710,51314723,51315041,51315912,51315919,51315924,51315931,51315940,51315949,51315952,51315959,51315972,51315977,51316748,51316846,51317748,51317942,51318036,51318845,51319361,51321168,51322669,51323366,51324792,51325576,51326207,51326282,51326336,51326641,51326932,51327144,51327161,51327180,51327615,51327636,51328144,51328902,51328949,51329227,51329391,51329505,51330194,51331197,51331487,51331500,51331522,51331535,51331542,51331549,51331552,51331559,51332896,51332944,51333541,51333739,51333879,51335644,51336888,51337186,51337349,51337703,51338495,51338523,51339163,51339747,51341226,51341730,51342093,51342147,51343110,51343368,51344297,51344378,51344729"}]},{"service":"GOOGLE_HELP","params":[{"key":"browse_id","value":"UC2utj8E4Z1p_RQAJzFLLtIw"},{"key":"browse_id_prefix","value":""}]},{"service":"CSI","params":[{"key":"c","value":"WEB"},{"key":"cver","value":"2.20241107.11.00"},{"key":"yt_li","value":"1"},{"key":"GetChannelPage_rid","value":"0x90a2b0d9728530e8"}]},{"service":"GUIDED_HELP","params":[{"key":"logged_in","value":"1"}]},{"service":"ECATCHER","params":[{"key":"client.version","value":"2.20241107"},{"key":"client.name","value":"WEB"}]}],"maxAgeSeconds":300,"mainAppWebResponseContext":{"datasyncId":"102020250295758170150||","loggedOut":false,"trackingParam":"kx_fmPxhoPZRj7a3TkBMoHaTfvzoE823jJza_cmm4FJODAwRgkuswmIBwOcCE59TDtslLKPQ-SS"},"webResponseContextExtensionData":{"ytConfigData":{"visitorData":"CgtfV0htTG9UMGNDRSiojcC5BjIKCgJVUxIEGgAgJQ%3D%3D","sessionIndex":0,"rootVisualElementType":3611},"hasDecorated":true}},"contents":{"twoColumnBrowseResultsRenderer":{"tabs":[{"tabRenderer":{"endpoint":{"clickTrackingParams":"CDcQ8JMBGAgiEwif7d2MydCJAxU3eUwIHTN7A2g=","commandMetadata":{"webCommandMetadata":{"url":"/@Gulaq/featured","webPageType":"WEB_PAGE_TYPE_CHANNEL","rootVe":3611,"apiUrl":"/youtubei/v1/browse"}},"browseEndpoint":{"browseId":"UC2utj8E4Z1p_RQAJzFLLtIw","params":"EghmZWF0dXJlZPIGBAoCMgA%3D","canonicalBaseUrl":"/@Gulaq"}},"title":"Home","selected":true,"content":{"sectionListRenderer":{"contents":[{"itemSectionRenderer":{"contents":[{"shelfRenderer":{"title":{"runs":[{"text":"Past live streams","navigationEndpoint":{"clickTrackingParams":"CNEBENwcGAAiEwif7d2MydCJAxU3eUwIHTN7A2g=","commandMetadata":{"webCommandMetadata":{"url":"/@Gulaq/videos?view=2\u0026sort=dd\u0026live_view=503\u0026shelf_id=1","webPageType":"WEB_PAGE_TYPE_CHANNEL","rootVe":3611,"apiUrl":"/youtubei/v1/browse"}},"browseEndpoint":{"browseId":"UC2utj8E4Z1p_RQAJzFLLtIw","params":"EgZ2aWRlb3MYAyACOARwAfIGCQoHegCiAQIIAQ%3D%3D","canonicalBaseUrl":"/@Gulaq"}}}]},"endpoint":{"clickTrackingParams":"CNEBENwcGAAiEwif7d2MydCJAxU3eUwIHTN7A2g=","commandMetadata":{"webCommandMetadata":{"url":"/@Gulaq/videos?view=2\u0026sort=dd\u0026live_view=503\u0026shelf_id=1","webPageType":"WEB_PAGE_TYPE_CHANNEL","rootVe":3611,"apiUrl":"/youtubei/v1/browse"}},"browseEndpoint":{"browseId":"UC2utj8E4Z1p_RQAJzFLLtIw","params":"EgZ2aWRlb3MYAyACOARwAfIGCQoHegCiAQIIAQ%3D%3D","canonicalBaseUrl":"/@Gulaq"}},"content":{"horizontalListRenderer":{"items":[{"gridVideoRenderer":{"videoId":"16Mb2PnoJm8","thumbnail":{"thumbnails":[{"url":"https://i.ytimg.com/vi/16Mb2PnoJm8/hqdefault.jpg?sqp=-oaymwEbCKgBEF5IVfKriqkDDggBFQAAiEIYAXABwAEG\u0026rs=AOn4CLA1JCTSmDWEPLAa9fSFBN50TAW_mQ","width":168,"height":94},{"url":"https://i.ytimg.com/vi/16Mb2PnoJm8/hqdefault.jpg?sqp=-oaymwEbCMQBEG5IVfKriqkDDggBFQAAiEIYAXABwAEG\u0026rs=AOn4CLBlI8bVh3jQwiJyo-TSCXt6rZnVFQ","width":196,"height":110},{"url":"https://i.ytimg.com/vi/16Mb2PnoJm8/hqdefault.jpg?sqp=-oaymwEcCPYBEIoBSFXyq4qpAw4IARUAAIhCGAFwAcABBg==\u0026rs=AOn4CLCRmZj0mMnkPTkLkqKAKeRPoK-KIg","width":246,"height":138},{"url":"https://i.ytimg.com/vi/16Mb2PnoJm8/hqdefault.jpg?sqp=-oaymwEcCNACELwBSFXyq4qpAw4IARUAAIhCGAFwAcABBg==\u0026rs=AOn4CLCFAXMHf9pFmw2PI8jFPZvfPPoViQ","width":336,"height":188}]},"title":{"accessibility":{"accessibilityData":{"label":"Gulaq September AMA webinar by Gulaq 1,771 views Streamed 4 weeks ago 1 hour, 5 minutes"}},"simpleText":"Gulaq September AMA webinar"},"publishedTimeText":{"simpleText":"Streamed 4 weeks ago"},"viewCountText":{"simpleText":"1,771 views"},"navigationEndpoint":{"clickTrackingParams":"CKICEJQ1GAAiEwif7d2MydCJAxU3eUwIHTN7A2gyBmctaGlnaFoYVUMydXRqOEU0WjFwX1JRQUp6RkxMdEl3mgEFEPI4GG4=","commandMetadata":{"webCommandMetadata":{"url":"/watch?v=16Mb2PnoJm8","webPageType":"WEB_PAGE_TYPE_WATCH","rootVe":3832}},"watchEndpoint":{"videoId":"16Mb2PnoJm8","watchEndpointSupportedOnesieConfig":{"html5PlaybackOnesieConfig":{"commonConfig":{"url":"https://rr4---sn-nx57ynsz.googlevideo.com/initplayback?source=youtube\u0026oeis=1\u0026c=WEB\u0026oad=3200\u0026ovd=3200\u0026oaad=11000\u0026oavd=11000\u0026ocs=700\u0026oewis=1\u0026oputc=1\u0026ofpcc=1\u0026siu=1\u0026msp=1\u0026odepv=1\u0026id=d7a31bd8f9e8266f\u0026ip=172.92.149.238\u0026initcwndbps=1188750\u0026mt=1731200229\u0026oweuc=\u0026pxtags=Cg4KAnR4Egg1MTMzNzcwMw\u0026rxtags=Cg4KAnR4Egg1MTMzNzcwMQ%2CCg4KAnR4Egg1MTMzNzcwMg%2CCg4KAnR4Egg1MTMzNzcwMw%2CCg4KAnR4Egg1MTMzNzcwNA"}}}}},"trackingParams":"CKICEJQ1GAAiEwif7d2MydCJAxU3eUwIHTN7A2hA78ygz4_7xtHXAQ==","shortViewCountText":{"accessibility":{"accessibilityData":{"label":"1.7K views"}},"simpleText":"1.7K views"},"menu":{"menuRenderer":{"items":[{"menuServiceItemRenderer":{"text":{"runs":[{"text":"Add to queue"}]},"icon":{"iconType":"ADD_TO_QUEUE_TAIL"},"serviceEndpoint":{"clickTrackingParams":"CKgCEP6YBBgGIhMIn-3djMnQiQMVN3lMCB0zewNo","commandMetadata":{"webCommandMetadata":{"sendPost":true}},"signalServiceEndpoint":{"signal":"CLIENT_SIGNAL","actions":[{"clickTrackingParams":"CKgCEP6YBBgGIhMIn-3djMnQiQMVN3lMCB0zewNo","addToPlaylistCommand":{"openMiniplayer":true,"videoId":"16Mb2PnoJm8","listType":"PLAYLIST_EDIT_LIST_TYPE_QUEUE","onCreateListCommand":{"clickTrackingParams":"CKgCEP6YBBgGIhMIn-3djMnQiQMVN3lMCB0zewNo","commandMetadata":{"webCommandMetadata":{"sendPost":true,"apiUrl":"/youtubei/v1/playlist/create"}},"createPlaylistServiceEndpoint":{"videoIds":["16Mb2PnoJm8"],"params":"CAQ%3D"}},"videoIds":["16Mb2PnoJm8"]}}]}},"trackingParams":"CKgCEP6YBBgGIhMIn-3djMnQiQMVN3lMCB0zewNo"}},{"menuServiceItemRenderer":{"text":{"runs":[{"text":"Save to Watch later"}]},"icon":{"iconType":"WATCH_LATER"},"serviceEndpoint":{"clickTrackingParams":"CKICEJQ1GAAiEwif7d2MydCJAxU3eUwIHTN7A2g=","commandMetadata":{"webCommandMetadata":{"sendPost":true,"apiUrl":"/youtubei/v1/browse/edit_playlist"}},"playlistEditEndpoint":{"playlistId":"WL","actions":[{"addedVideoId":"16Mb2PnoJm8","action":"ACTION_ADD_VIDEO"}]}},"trackingParams":"CKICEJQ1GAAiEwif7d2MydCJAxU3eUwIHTN7A2g="}},{"menuServiceItemRenderer":{"text":{"runs":[{"text":"Save to playlist"}]},"icon":{"iconType":"PLAYLIST_ADD"},"serviceEndpoint":{"clickTrackingParams":"CKcCEJSsCRgIIhMIn-3djMnQiQMVN3lMCB0zewNo","commandMetadata":{"webCommandMetadata":{"sendPost":true,"apiUrl":"/youtubei/v1/playlist/get_add_to_playlist"}},"addToPlaylistServiceEndpoint":{"videoId":"16Mb2PnoJm8"}},"trackingParams":"CKcCEJSsCRgIIhMIn-3djMnQiQMVN3lMCB0zewNo"}},{"menuServiceItemDownloadRenderer":{"serviceEndpoint":{"clickTrackingParams":"CKYCENGqBRgJIhMIn-3djMnQiQMVN3lMCB0zewNo","offlineVideoEndpoint":{"videoId":"16Mb2PnoJm8","onAddCommand":{"clickTrackingParams":"CKYCENGqBRgJIhMIn-3djMnQiQMVN3lMCB0zewNo","getDownloadActionCommand":{"videoId":"16Mb2PnoJm8","params":"CAIQAA%3D%3D"}}}},"trackingParams":"CKYCENGqBRgJIhMIn-3djMnQiQMVN3lMCB0zewNo"}},{"menuServiceItemRenderer":{"text":{"runs":[{"text":"Share"}]},"icon":{"iconType":"SHARE"},"serviceEndpoint":{"clickTrackingParams":"CKICEJQ1GAAiEwif7d2MydCJAxU3eUwIHTN7A2g=","commandMetadata":{"webCommandMetadata":{"sendPost":true,"apiUrl":"/youtubei/v1/share/get_share_panel"}},"shareEntityServiceEndpoint":{"serializedShareEntity":"CgsxNk1iMlBub0ptOA%3D%3D","commands":[{"clickTrackingParams":"CKICEJQ1GAAiEwif…