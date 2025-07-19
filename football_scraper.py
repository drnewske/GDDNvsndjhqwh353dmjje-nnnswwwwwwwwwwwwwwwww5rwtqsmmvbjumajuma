import requests
import json
import re
import time
import random
import logging
import os
import uuid
import glob
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Tuple, Optional
from collections import defaultdict

# Configuration
STREAMED_API_BASE_URL = "https://streamed.su"
STREAMED_MATCHES_ENDPOINT = "/api/matches/all-today"
SPORTSONLINE_URL = "https://sportsonline.gl/"
DEFAULT_LOGO_URL = "https://cdn.jsdelivr.net/gh/drnewske/tyhdsjax-nfhbqsm/logos/default.png"
REQUEST_TIMEOUT = 10
LOG_FILE = "scraper.log"
OUTPUT_FILE = "live_events.json"

# Cleanup configuration
MATCH_CLEANUP_HOURS = 25  # Remove matches older than 25 hours
LOG_CLEANUP_HOURS = 48    # Remove log entries older than 48 hours

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def generate_fetch_code() -> str:
    """Generate a unique fetch code for this run"""
    return f"FETCH-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{str(uuid.uuid4())[:8].upper()}"

def cleanup_old_matches(matches: List[dict], fetch_code: str) -> List[dict]:
    """Remove matches older than MATCH_CLEANUP_HOURS"""
    logger.info(f"[{fetch_code}] Cleaning up old matches...")
    
    cutoff_time = datetime.now() - timedelta(hours=MATCH_CLEANUP_HOURS)
    valid_matches = []
    removed_count = 0
    
    for match in matches:
        try:
            match_date = match.get("date", "")
            match_time = match.get("time", "")
            
            if not match_date or not match_time or match_date == "Not Found" or match_time == "Not Found":
                valid_matches.append(match)
                continue
            
            day, month, year = map(int, match_date.split('-'))
            hour, minute = map(int, match_time.split(':'))
            match_datetime = datetime(year, month, day, hour, minute)
            
            if match_datetime < cutoff_time:
                removed_count += 1
                team1_name = match.get("team1", {}).get("name", "Unknown")
                team2_name = match.get("team2", {}).get("name", "Unknown")
                logger.info(f"[{fetch_code}] Removed old match: {team1_name} vs {team2_name} ({match_date} {match_time})")
            else:
                valid_matches.append(match)
                
        except (ValueError, KeyError, AttributeError) as e:
            logger.warning(f"[{fetch_code}] Could not parse match date/time, keeping match: {e}")
            valid_matches.append(match)
    
    if removed_count > 0:
        logger.info(f"[{fetch_code}] Cleanup complete: Removed {removed_count} old matches, {len(valid_matches)} matches remaining")
    else:
        logger.info(f"[{fetch_code}] No old matches to remove")
    
    return valid_matches

def cleanup_old_logs(fetch_code: str):
    """Clean up old log entries from the log file"""
    logger.info(f"[{fetch_code}] Cleaning up old log entries...")
    
    if not os.path.exists(LOG_FILE):
        return
    
    try:
        cutoff_time = datetime.now() - timedelta(hours=LOG_CLEANUP_HOURS)
        valid_lines = []
        removed_count = 0
        
        with open(LOG_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        for line in lines:
            try:
                timestamp_match = re.match(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+', line)
                if timestamp_match:
                    timestamp_str = timestamp_match.group(1)
                    log_datetime = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                    
                    if log_datetime >= cutoff_time:
                        valid_lines.append(line)
                    else:
                        removed_count += 1
                else:
                    valid_lines.append(line)
            except (ValueError, AttributeError):
                valid_lines.append(line)
        
        if removed_count > 0:
            with open(LOG_FILE, 'w', encoding='utf-8') as f:
                f.writelines(valid_lines)
            logger.info(f"[{fetch_code}] Log cleanup complete: Removed {removed_count} old log entries")
            
    except Exception as e:
        logger.error(f"[{fetch_code}] Error during log cleanup: {e}")

def cleanup_old_log_files(fetch_code: str):
    """Clean up old rotated log files if they exist"""
    try:
        log_pattern = f"{LOG_FILE}.*"
        log_files = glob.glob(log_pattern)
        cutoff_time = datetime.now() - timedelta(hours=LOG_CLEANUP_HOURS)
        removed_files = 0
        
        for log_file in log_files:
            if log_file == LOG_FILE:
                continue
            try:
                file_mtime = datetime.fromtimestamp(os.path.getmtime(log_file))
                if file_mtime < cutoff_time:
                    os.remove(log_file)
                    removed_files += 1
                    logger.info(f"[{fetch_code}] Removed old log file: {log_file}")
            except OSError as e:
                logger.warning(f"[{fetch_code}] Could not remove log file {log_file}: {e}")
                
    except Exception as e:
        logger.error(f"[{fetch_code}] Error during log file cleanup: {e}")

def fetch_data(url: str, headers: dict = None) -> Optional[dict]:
    """Fetches data from a given URL."""
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT, headers=headers)
        response.raise_for_status()
        return response.json() if 'json' in response.headers.get('content-type', '') else response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from {url}: {e}")
        return None

def is_valid_team_data(team1_name: str, team2_name: str) -> bool:
    """Check if both teams have valid names (not default values)"""
    invalid_values = {"Not Found", "Name Not Found", "", None}
    return (team1_name not in invalid_values and 
            team2_name not in invalid_values and
            team1_name.strip() != "" and 
            team2_name.strip() != "")

def get_match_date_from_timestamp(timestamp_ms: int) -> Tuple[str, str]:
    """Convert timestamp to formatted time and date, handling timezone properly"""
    try:
        dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        formatted_time = dt_utc.strftime("%H:%M")
        formatted_date = dt_utc.strftime("%d-%m-%Y")
        return formatted_time, formatted_date
    except (ValueError, OSError, OverflowError) as e:
        logger.error(f"Error processing timestamp {timestamp_ms}: {e}")
        return "Not Found", "Not Found"

def fetch_streamed_matches(fetch_code: str) -> List[dict]:
    """Fetch matches from streamed.su API with improved speed and precision."""
    logger.info(f"[{fetch_code}] Fetching matches from streamed.su...")
    matches_url = f"{STREAMED_API_BASE_URL}{STREAMED_MATCHES_ENDPOINT}"
    api_matches = fetch_data(matches_url)

    if not api_matches:
        logger.error(f"[{fetch_code}] Could not fetch streamed.su match data.")
        return []
    
    output_data = []
    today_date_str = datetime.now().strftime("%d-%m-%Y")
    logger.info(f"[{fetch_code}] Filtering for today's date: {today_date_str}")

    for match in api_matches:
        # ===== Filter 1: By Sport (Fast) =====
        if match.get("category") != "football":
            continue

        title = match.get("title", "Title Not Found")

        # ===== Filter 2: By Date (Fast) =====
        match_timestamp_ms = match.get("date")
        if not (match_timestamp_ms and isinstance(match_timestamp_ms, (int, float)) and match_timestamp_ms > 0):
            continue
        
        formatted_time, formatted_date = get_match_date_from_timestamp(match_timestamp_ms)
        
        if formatted_date != today_date_str:
            logger.info(f"[{fetch_code}] Skipping match not for today ({formatted_date}): {title}")
            continue
        
        # ===== Filter 3: By Team Data (Fast) =====
        team1 = {"name": "Not Found", "logo_url": DEFAULT_LOGO_URL}
        team2 = {"name": "Not Found", "logo_url": DEFAULT_LOGO_URL}
        teams_data = match.get("teams")
        if teams_data:
            if teams_data.get("home"):
                home_name = teams_data['home'].get('name', '').strip()
                if home_name:
                    team1['name'] = home_name
                    badge = teams_data['home'].get('badge')
                    if badge:
                        team1['logo_url'] = f"{STREAMED_API_BASE_URL}/api/images/badge/{badge}.webp"
            if teams_data.get("away"):
                away_name = teams_data['away'].get('name', '').strip()
                if away_name:
                    team2['name'] = away_name
                    badge = teams_data['away'].get('badge')
                    if badge:
                        team2['logo_url'] = f"{STREAMED_API_BASE_URL}/api/images/badge/{badge}.webp"
        
        if not is_valid_team_data(team1['name'], team2['name']):
            logger.warning(f"[{fetch_code}] Skipping match with invalid team data: {title}")
            continue

        # ===== Fetch Streams (Slow - ONLY runs if all previous checks pass) =====
        all_stream_links = []
        sources = match.get("sources", [])
        if not sources:
            logger.warning(f"[{fetch_code}] Skipping match with no listed sources: {title}")
            continue

        for source in sources:
            source_name = source.get("source")
            source_id = source.get("id")
            if not source_name or not source_id:
                continue

            stream_url = f"{STREAMED_API_BASE_URL}/api/stream/{source_name}/{source_id}"
            streams_data = fetch_data(stream_url)
            
            if streams_data and isinstance(streams_data, list):
                for stream in streams_data:
                    embed_url = stream.get("embedUrl")
                    if embed_url and "admin" not in embed_url and embed_url.startswith(('http://', 'https://')):
                        all_stream_links.append(embed_url)
            time.sleep(0.5)

        # ===== Final Check =====
        if not all_stream_links:
            logger.warning(f"[{fetch_code}] No valid stream links found after checking all sources for: {title}")
            continue

        # If we reach here, the match is valid, for today, and has links.
        formatted_match = {
            "source_name": "Drogon",
            "source_icon_url": "https://awoiaf.westeros.org/images/thumb/d/d4/Aegon_on_Balerion.jpg/450px-Aegon_on_Balerion.jpg",
            "match_title_from_api": title,
            "team1": team1,
            "team2": team2,
            "time": formatted_time,
            "date": formatted_date,
            "links": all_stream_links
        }
        output_data.append(formatted_match)

    logger.info(f"[{fetch_code}] Found {len(output_data)} valid matches for today from streamed.su")
    return output_data

def fetch_sportsonline_data() -> str:
    """Fetch the raw text data from sportsonline.gl"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
        response = requests.get(SPORTSONLINE_URL, headers=headers, timeout=30)
        response.raise_for_status()
        response.encoding = 'utf-8'
        return response.text
    except Exception as e:
        logger.error(f"Error fetching data from sportsonline.gl: {str(e)}")
        return ""

def get_current_day() -> str:
    """Get current day of the week in uppercase"""
    days = ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY']
    return days[datetime.now().weekday()]

def subtract_hour_from_time(time_str: str) -> str:
    """Subtract 1 hour from time string (UTC+1 to UTC conversion)"""
    try:
        hour, minute = map(int, time_str.split(':'))
        dt = datetime.now().replace(hour=hour, minute=minute, second=0, microsecond=0)
        dt_adjusted = dt - timedelta(hours=1)
        return dt_adjusted.strftime("%H:%M")
    except (ValueError, AttributeError) as e:
        logger.error(f"Error processing time {time_str}: {e}")
        return time_str

def time_to_minutes(time_str: str) -> int:
    """Convert time string to minutes since midnight for sorting"""
    try:
        hour, minute = map(int, time_str.split(':'))
        return hour * 60 + minute
    except (ValueError, AttributeError):
        return 0

def parse_sportsonline_data(raw_data: str, fetch_code: str) -> List[Tuple[str, str, str]]:
    """Parse the raw text data into structured format for current day only."""
    matches = []
    current_day = get_current_day()
    try:
        lines = raw_data.strip().split('\n')
        in_current_day_section = False
        for line in lines:
            line = line.strip()
            if not line:
                continue
            if line.upper() in ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY']:
                in_current_day_section = (line.upper() == current_day)
                continue
            if not in_current_day_section:
                continue
            if '|' in line:
                parts = line.split('|', 1)
                if len(parts) == 2:
                    left_part, stream_url = parts[0].strip(), parts[1].strip()
                    if not stream_url.startswith(('http://', 'https://')):
                        continue
                    time_match = re.match(r'^(\d{1,2}:\d{2})\s+(.+)$', left_part)
                    if time_match:
                        time, title = time_match.group(1), time_match.group(2).strip()
                        if ':' in title or not (' vs ' in title or ' x ' in title):
                            continue
                        teams = title.split(' vs ') if ' vs ' in title else title.split(' x ')
                        if len(teams) != 2 or not teams[0].strip() or not teams[1].strip():
                            continue
                        adjusted_time = subtract_hour_from_time(time)
                        title = title.replace(' x ', ' vs ')
                        matches.append((adjusted_time, title, stream_url))
        return matches
    except Exception as e:
        logger.error(f"[{fetch_code}] Error parsing sportsonline data: {str(e)}")
        return []

def group_sportsonline_matches(parsed_matches: List[Tuple[str, str, str]], fetch_code: str) -> List[dict]:
    """Group matches by event and combine duplicate streams."""
    grouped = defaultdict(list)
    for time, title, stream_url in parsed_matches:
        grouped[(time, title)].append(stream_url)
    
    matches, match_groups = [], []
    for key, urls in grouped.items():
        match_groups.append((*key, urls))
    
    sorted_groups = sorted(match_groups, key=lambda x: time_to_minutes(x[0]))
    
    for time, title, stream_urls in sorted_groups:
        teams = title.split(' vs ', 1)
        team1_name, team2_name = teams[0].strip(), teams[1].strip()
        if not is_valid_team_data(team1_name, team2_name):
            continue
        unique_streams = list(dict.fromkeys(stream_urls))
        if not unique_streams:
            continue
        
        match_entry = {
            "source_name": "THE BETTER BASTARD",
            "source_icon_url": "https://static01.nyt.com/images/2016/06/20/arts/ramsay/ramsay-jumbo.jpg?quality=75&auto=webp",
            "match_title_from_api": title,
            "team1": {"name": team1_name, "logo_url": DEFAULT_LOGO_URL},
            "team2": {"name": team2_name, "logo_url": DEFAULT_LOGO_URL},
            "time": time,
            "date": datetime.now().strftime("%d-%m-%Y"),
            "links": unique_streams
        }
        matches.append(match_entry)
    return matches

def fetch_sportsonline_matches(fetch_code: str) -> List[dict]:
    """Fetch matches from sportsonline.gl"""
    logger.info(f"[{fetch_code}] Fetching matches from sportsonline.gl...")
    raw_data = fetch_sportsonline_data()
    if not raw_data:
        return []
    parsed_matches = parse_sportsonline_data(raw_data, fetch_code)
    matches = group_sportsonline_matches(parsed_matches, fetch_code)
    logger.info(f"[{fetch_code}] Fetched {len(matches)} valid matches from sportsonline.gl")
    return matches

def load_existing_data() -> List[dict]:
    """Load existing data from the output file"""
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            logger.warning(f"Could not load existing data: {e}")
    return []

def merge_with_existing_data(new_matches: List[dict], existing_matches: List[dict], fetch_code: str) -> List[dict]:
    """Merge new matches with existing data, updating where necessary (backwards-compatible)"""
    logger.info(f"[{fetch_code}] Merging with existing data...")
    existing_matches = cleanup_old_matches(existing_matches, fetch_code)
    
    existing_lookup = {}
    for match in existing_matches:
        if "source_name" not in match:
            team1 = match.get("team1", {}).get("name", "Unknown")
            team2 = match.get("team2", {}).get("name", "Unknown")
            logger.warning(f"[{fetch_code}] Skipping existing match in old format (missing 'source_name'): {team1} vs {team2}")
            continue
        key = (match["source_name"], match["team1"]["name"], match["team2"]["name"], match["date"])
        existing_lookup[key] = match
    
    merged_matches = []
    updated_count = 0
    new_count = 0
    for new_match in new_matches:
        key = (new_match["source_name"], new_match["team1"]["name"], new_match["team2"]["name"], new_match["date"])
        if key in existing_lookup:
            existing_match = existing_lookup[key]
            needs_update = False
            if (existing_match["team1"]["logo_url"] == DEFAULT_LOGO_URL and new_match["team1"]["logo_url"] != DEFAULT_LOGO_URL):
                existing_match["team1"]["logo_url"] = new_match["team1"]["logo_url"]
                needs_update = True
            if (existing_match["team2"]["logo_url"] == DEFAULT_LOGO_URL and new_match["team2"]["logo_url"] != DEFAULT_LOGO_URL):
                existing_match["team2"]["logo_url"] = new_match["team2"]["logo_url"]
                needs_update = True
            
            existing_links = set(existing_match["links"])
            new_links = set(new_match["links"])
            if not new_links.issubset(existing_links):
                combined_links = list(existing_links.union(new_links))
                random.shuffle(combined_links)
                existing_match["links"] = combined_links
                needs_update = True
            
            if needs_update:
                updated_count += 1
                logger.info(f"[{fetch_code}] Updated: {new_match['team1']['name']} vs {new_match['team2']['name']}")
            merged_matches.append(existing_match)
            del existing_lookup[key]
        else:
            new_count += 1
            merged_matches.append(new_match)
            logger.info(f"[{fetch_code}] New match: {new_match['team1']['name']} vs {new_match['team2']['name']}")
    
    merged_matches.extend(existing_lookup.values())
    logger.info(f"[{fetch_code}] Merge complete: {new_count} new, {updated_count} updated, {len(merged_matches)} total")
    return merged_matches

def save_data(data: List[dict], fetch_code: str):
    """Save data to output file"""
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"[{fetch_code}] Data saved to {OUTPUT_FILE}")
    except Exception as e:
        logger.error(f"[{fetch_code}] Error saving data: {e}")

def main():
    """Main function to fetch from both sources and merge results"""
    fetch_code = generate_fetch_code()
    logger.info(f"[{fetch_code}] Starting combined football match scraper...")
    logger.info("=" * 60)
    
    try:
        logger.info(f"[{fetch_code}] Starting cleanup operations...")
        cleanup_old_logs(fetch_code)
        cleanup_old_log_files(fetch_code)
        
        streamed_matches = fetch_streamed_matches(fetch_code)
        sportsonline_matches = fetch_sportsonline_matches(fetch_code)
        
        all_new_matches = streamed_matches + sportsonline_matches
        
        existing_data = load_existing_data()
        final_matches = merge_with_existing_data(all_new_matches, existing_data, fetch_code)
        
        save_data(final_matches, fetch_code)
        
        logger.info(f"[{fetch_code}] Summary:")
        logger.info(f"[{fetch_code}] - Streamed.su ('Drogon') matches: {len(streamed_matches)}")
        logger.info(f"[{fetch_code}] - Sportsonline ('THE BETTER BASTARD') matches: {len(sportsonline_matches)}")
        logger.info(f"[{fetch_code}] - Final total matches in file: {len(final_matches)}")
        
        logger.info(f"[{fetch_code}] Scraper run completed successfully")
        
    except Exception as e:
        logger.error(f"[{fetch_code}] Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()
