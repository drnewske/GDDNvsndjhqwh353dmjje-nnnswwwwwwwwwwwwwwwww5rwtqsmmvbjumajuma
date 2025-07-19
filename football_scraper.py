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
            # Parse the match date and time
            match_date = match.get("date", "")
            match_time = match.get("time", "")
            
            if not match_date or not match_time or match_date == "Not Found" or match_time == "Not Found":
                # Keep matches with invalid dates for now (they might be current)
                valid_matches.append(match)
                continue
            
            # Parse date (format: DD-MM-YYYY)
            day, month, year = map(int, match_date.split('-'))
            # Parse time (format: HH:MM)
            hour, minute = map(int, match_time.split(':'))
            
            # Create datetime object
            match_datetime = datetime(year, month, day, hour, minute)
            
            # Check if match is older than cutoff
            if match_datetime < cutoff_time:
                removed_count += 1
                team1_name = match.get("team1", {}).get("name", "Unknown")
                team2_name = match.get("team2", {}).get("name", "Unknown")
                logger.info(f"[{fetch_code}] Removed old match: {team1_name} vs {team2_name} ({match_date} {match_time})")
            else:
                valid_matches.append(match)
                
        except (ValueError, KeyError, AttributeError) as e:
            # Keep matches with parsing errors (they might be current)
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
        logger.info(f"[{fetch_code}] No log file found, skipping log cleanup")
        return
    
    try:
        cutoff_time = datetime.now() - timedelta(hours=LOG_CLEANUP_HOURS)
        valid_lines = []
        removed_count = 0
        
        with open(LOG_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        for line in lines:
            try:
                # Extract timestamp from log line (format: YYYY-MM-DD HH:MM:SS,mmm)
                timestamp_match = re.match(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+', line)
                if timestamp_match:
                    timestamp_str = timestamp_match.group(1)
                    log_datetime = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                    
                    if log_datetime >= cutoff_time:
                        valid_lines.append(line)
                    else:
                        removed_count += 1
                else:
                    # Keep lines without timestamps (might be continuation lines)
                    valid_lines.append(line)
                    
            except (ValueError, AttributeError):
                # Keep lines with parsing errors
                valid_lines.append(line)
        
        # Write back the cleaned log file
        if removed_count > 0:
            with open(LOG_FILE, 'w', encoding='utf-8') as f:
                f.writelines(valid_lines)
            logger.info(f"[{fetch_code}] Log cleanup complete: Removed {removed_count} old log entries")
        else:
            logger.info(f"[{fetch_code}] No old log entries to remove")
            
    except Exception as e:
        logger.error(f"[{fetch_code}] Error during log cleanup: {e}")

def cleanup_old_log_files(fetch_code: str):
    """Clean up old rotated log files if they exist"""
    try:
        # Look for rotated log files (scraper.log.1, scraper.log.2, etc.)
        log_pattern = f"{LOG_FILE}.*"
        log_files = glob.glob(log_pattern)
        
        cutoff_time = datetime.now() - timedelta(hours=LOG_CLEANUP_HOURS)
        removed_files = 0
        
        for log_file in log_files:
            if log_file == LOG_FILE:  # Skip the main log file
                continue
                
            try:
                # Check file modification time
                file_mtime = datetime.fromtimestamp(os.path.getmtime(log_file))
                if file_mtime < cutoff_time:
                    os.remove(log_file)
                    removed_files += 1
                    logger.info(f"[{fetch_code}] Removed old log file: {log_file}")
            except OSError as e:
                logger.warning(f"[{fetch_code}] Could not remove log file {log_file}: {e}")
        
        if removed_files == 0:
            logger.info(f"[{fetch_code}] No old log files to remove")
            
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
        # Convert to UTC datetime
        dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        
        formatted_time = dt_utc.strftime("%H:%M")
        formatted_date = dt_utc.strftime("%d-%m-%Y")
        
        return formatted_time, formatted_date
    except (ValueError, OSError, OverflowError) as e:
        logger.error(f"Error processing timestamp {timestamp_ms}: {e}")
        return "Not Found", "Not Found"

def fetch_streamed_matches(fetch_code: str) -> List[dict]:
    """Fetch matches from streamed.su API"""
    logger.info(f"[{fetch_code}] Fetching matches from streamed.su...")
    matches_url = f"{STREAMED_API_BASE_URL}{STREAMED_MATCHES_ENDPOINT}"
    api_matches = fetch_data(matches_url)

    if not api_matches:
        logger.error(f"[{fetch_code}] Could not fetch streamed.su match data.")
        return []
    
    output_data = []
    logger.info(f"[{fetch_code}] Found {len(api_matches)} total matches. Filtering for football...")

    for match in api_matches:
        title = match.get("title", "Title Not Found")
        category = match.get("category")
        
        if category != "football":
            continue
        
        team1 = {"name": "Not Found", "logo_url": DEFAULT_LOGO_URL}
        team2 = {"name": "Not Found", "logo_url": DEFAULT_LOGO_URL}
        all_stream_links = []
        
        match_timestamp_ms = match.get("date")
        if match_timestamp_ms and isinstance(match_timestamp_ms, (int, float)) and match_timestamp_ms > 0:
            formatted_time, formatted_date = get_match_date_from_timestamp(match_timestamp_ms)
        else:
            logger.warning(f"[{fetch_code}] Invalid or missing timestamp for match: {title}")
            formatted_time, formatted_date = "Not Found", "Not Found"

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
            logger.warning(f"[{fetch_code}] Skipping match with invalid team data: {title} (Team1: {team1['name']}, Team2: {team2['name']})")
            continue

        if formatted_time == "Not Found" or formatted_date == "Not Found":
            logger.warning(f"[{fetch_code}] Skipping match with invalid date/time: {title}")
            continue

        sources = match.get("sources", [])
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

        if not all_stream_links:
            logger.warning(f"[{fetch_code}] Skipping match with no valid stream links: {title}")
            continue

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

    logger.info(f"[{fetch_code}] Fetched {len(output_data)} valid football matches from streamed.su")
    return output_data

def fetch_sportsonline_data() -> str:
    """Fetch the raw text data from sportsonline.gl"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Charset': 'utf-8'
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
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            return time_str
            
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
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            return 0
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
            
            if re.match(r'^(HD|BR)\d+\s+[A-Z]+$', line):
                continue
            
            if not in_current_day_section:
                continue
                
            if '|' in line:
                parts = line.split('|', 1)
                if len(parts) == 2:
                    left_part = parts[0].strip()
                    stream_url = parts[1].strip()
                    
                    if not stream_url.startswith(('http://', 'https://')):
                        continue
                    
                    time_match = re.match(r'^(\d{1,2}:\d{2})\s+(.+)$', left_part)
                    if time_match:
                        time = time_match.group(1)
                        title = time_match.group(2).strip()
                        
                        if ':' in title:
                            continue
                        
                        if not (' vs ' in title or ' x ' in title):
                            continue
                        
                        if ' vs ' in title:
                            teams = title.split(' vs ')
                        else:
                            teams = title.split(' x ')
                            
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
        key = (time, title)
        grouped[key].append(stream_url)
    
    matches = []
    match_groups = []
    for (time, title), stream_urls in grouped.items():
        match_groups.append((time, title, stream_urls))
    
    def sort_key(match_tuple):
        time_str = match_tuple[0]
        minutes = time_to_minutes(time_str)
        if 0 <= minutes < 360:
            return minutes + 1440
        return minutes
    
    sorted_groups = sorted(match_groups, key=sort_key)
    
    for time, title, stream_urls in sorted_groups:
        if ' vs ' in title:
            teams = title.split(' vs ', 1)
            team1_name = teams[0].strip()
            team2_name = teams[1].strip()
        else:
            team1_name = title.strip()
            team2_name = ""
        
        if not is_valid_team_data(team1_name, team2_name):
            continue
        
        unique_streams = []
        seen = set()
        for url in stream_urls:
            if url not in seen and url.startswith(('http://', 'https://')):
                unique_streams.append(url)
                seen.add(url)
        
        if not unique_streams:
            continue
        
        current_date = datetime.now()
        try:
            match_hour = int(time.split(':')[0])
            current_hour = current_date.hour
            
            if 0 <= match_hour <= 5 and current_hour >= 18:
                next_day = current_date + timedelta(days=1)
                formatted_date = next_day.strftime("%d-%m-%Y")
            else:
                formatted_date = current_date.strftime("%d-%m-%Y")
        except (ValueError, IndexError):
            formatted_date = current_date.strftime("%d-%m-%Y")
        
        match_entry = {
            "source_name": "THE BETTER BASTARD",
            "source_icon_url": "https://static01.nyt.com/images/2016/06/20/arts/ramsay/ramsay-jumbo.jpg?quality=75&auto=webp",
            "match_title_from_api": title,
            "team1": {"name": team1_name, "logo_url": DEFAULT_LOGO_URL},
            "team2": {"name": team2_name, "logo_url": DEFAULT_LOGO_URL},
            "time": time,
            "date": formatted_date,
            "links": unique_streams
        }
        
        matches.append(match_entry)
    
    return matches

def fetch_sportsonline_matches(fetch_code: str) -> List[dict]:
    """Fetch matches from sportsonline.gl"""
    logger.info(f"[{fetch_code}] Fetching matches from sportsonline.gl...")
    raw_data = fetch_sportsonline_data()
    if not raw_data:
        logger.error(f"[{fetch_code}] Failed to fetch data from sportsonline.gl")
        return []

    parsed_matches = parse_sportsonline_data(raw_data, fetch_code)
    if not parsed_matches:
        logger.info(f"[{fetch_code}] No matches found for {get_current_day()}")
        return []

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
    
    # First, run cleanup on the existing matches to remove very old ones
    existing_matches = cleanup_old_matches(existing_matches, fetch_code)
    
    existing_lookup = {}
    for match in existing_matches:
        # **FIX**: Check if the match from the JSON file has the new source_name field.
        # If not, it's from an old run, and we should log and skip it to prevent a crash.
        if "source_name" not in match:
            team1_name = match.get("team1", {}).get("name", "Unknown")
            team2_name = match.get("team2", {}).get("name", "Unknown")
            logger.warning(f"[{fetch_code}] Skipping existing match in old format (missing 'source_name'): {team1_name} vs {team2_name}")
            continue

        # If the key exists, the match is in the new format and can be processed.
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
            
            # Update logos if they were default and now we have real ones
            if (existing_match["team1"]["logo_url"] == DEFAULT_LOGO_URL and 
                new_match["team1"]["logo_url"] != DEFAULT_LOGO_URL):
                existing_match["team1"]["logo_url"] = new_match["team1"]["logo_url"]
                needs_update = True
                
            if (existing_match["team2"]["logo_url"] == DEFAULT_LOGO_URL and 
                new_match["team2"]["logo_url"] != DEFAULT_LOGO_URL):
                existing_match["team2"]["logo_url"] = new_match["team2"]["logo_url"]
                needs_update = True
            
            # Merge links
            existing_links = set(existing_match["links"])
            new_links = set(new_match["links"])
            combined_links = list(existing_links.union(new_links))
            
            if len(combined_links) > len(existing_match["links"]):
                random.shuffle(combined_links)
                existing_match["links"] = combined_links
                needs_update = True
            
            if needs_update:
                updated_count += 1
                logger.info(f"[{fetch_code}] Updated: {new_match['team1']['name']} vs {new_match['team2']['name']}")
            
            merged_matches.append(existing_match)
            del existing_lookup[key]
        else:
            # New match
            new_count += 1
            merged_matches.append(new_match)
            logger.info(f"[{fetch_code}] New match: {new_match['team1']['name']} vs {new_match['team2']['name']}")
    
    # Add remaining existing matches that weren't updated
    for remaining_match in existing_lookup.values():
        merged_matches.append(remaining_match)
    
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
        
        # Fetch from both sources
        streamed_matches = fetch_streamed_matches(fetch_code)
        sportsonline_matches = fetch_sportsonline_matches(fetch_code)
        
        # Combine the lists from both sources into one
        all_new_matches = streamed_matches + sportsonline_matches
        
        # Load existing data and merge with new data
        existing_data = load_existing_data()
        final_matches = merge_with_existing_data(all_new_matches, existing_data, fetch_code)
        
        # Save the final data
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
