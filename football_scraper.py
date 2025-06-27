import requests
import json
import re
import time
import random
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Tuple, Optional
from collections import defaultdict
from difflib import SequenceMatcher
import uuid

# Configuration
STREAMED_API_BASE_URL = "https://streamed.su"
STREAMED_MATCHES_ENDPOINT = "/api/matches/all-today"
SPORTSONLINE_URL = "https://sportsonline.gl/"
DEFAULT_LOGO_URL = "https://cdn.jsdelivr.net/gh/drnewske/tyhdsjax-nfhbqsm/logos/default.png"
REQUEST_TIMEOUT = 10
SIMILARITY_THRESHOLD = 0.9
LOG_FILE = "scraper.log"
OUTPUT_FILE = "live_events.json"

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
        
        # For display purposes, you might want to convert to local timezone
        # For now, keeping UTC as per original code
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
        
        # Initialize with defaults
        team1 = {"name": "Not Found", "logo_url": DEFAULT_LOGO_URL}
        team2 = {"name": "Not Found", "logo_url": DEFAULT_LOGO_URL}
        all_stream_links = []
        
        # Handle timestamp - Fixed: Better error handling and validation
        match_timestamp_ms = match.get("date")
        if match_timestamp_ms and isinstance(match_timestamp_ms, (int, float)) and match_timestamp_ms > 0:
            formatted_time, formatted_date = get_match_date_from_timestamp(match_timestamp_ms)
        else:
            logger.warning(f"[{fetch_code}] Invalid or missing timestamp for match: {title}")
            formatted_time = "Not Found"
            formatted_date = "Not Found"

        # Handle teams
        teams_data = match.get("teams")
        if teams_data:
            if teams_data.get("home"):
                home_name = teams_data['home'].get('name', '').strip()
                if home_name:  # Only update if we have a valid name
                    team1['name'] = home_name
                    badge = teams_data['home'].get('badge')
                    if badge:
                        team1['logo_url'] = f"{STREAMED_API_BASE_URL}/api/images/badge/{badge}.webp"

            if teams_data.get("away"):
                away_name = teams_data['away'].get('name', '').strip()
                if away_name:  # Only update if we have a valid name
                    team2['name'] = away_name
                    badge = teams_data['away'].get('badge')
                    if badge:
                        team2['logo_url'] = f"{STREAMED_API_BASE_URL}/api/images/badge/{badge}.webp"

        # Skip matches where we couldn't find valid team data
        if not is_valid_team_data(team1['name'], team2['name']):
            logger.warning(f"[{fetch_code}] Skipping match with invalid team data: {title} (Team1: {team1['name']}, Team2: {team2['name']})")
            continue

        # Skip matches with invalid date/time data
        if formatted_time == "Not Found" or formatted_date == "Not Found":
            logger.warning(f"[{fetch_code}] Skipping match with invalid date/time: {title}")
            continue

        # Handle stream links
        sources = match.get("sources", [])
        for source in sources:
            source_name = source.get("source")
            source_id = source.get("id")
            
            if not source_name or not source_id:
                continue

            stream_url = f"{STREAMED_API_BASE_URL}/api/stream/{source_name}/{source_id}"
            streams_data = fetch_data(stream_url)
            
            if streams_data and isinstance(streams_data, list):  # Added type check
                for stream in streams_data:
                    embed_url = stream.get("embedUrl")
                    if embed_url and "admin" not in embed_url and embed_url.startswith(('http://', 'https://')):
                        all_stream_links.append(embed_url)
            time.sleep(0.5)

        # Skip matches with no valid stream links
        if not all_stream_links:
            logger.warning(f"[{fetch_code}] Skipping match with no valid stream links: {title}")
            continue

        formatted_match = {
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
        # Better validation of time values
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
        if not (0 <= hour <= 23 and 0 <= minute <= 59):  # Added validation
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
                    
                    # Better URL validation
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
                        
                        # Better team name validation
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
        if 0 <= minutes < 360:  # Early morning matches (00:00-05:59) are next day
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
        
        # Skip matches with invalid team names
        if not is_valid_team_data(team1_name, team2_name):
            continue
        
        unique_streams = []
        seen = set()
        for url in stream_urls:
            if url not in seen and url.startswith(('http://', 'https://')):  # Added URL validation
                unique_streams.append(url)
                seen.add(url)
        
        # Skip matches with no valid streams
        if not unique_streams:
            continue
        
        # Better date calculation logic
        current_date = datetime.now()
        try:
            match_hour = int(time.split(':')[0])
            current_hour = current_date.hour
            
            # If match is in early morning (00:00-05:59) and current time is late (18:00+), 
            # it's likely next day
            if 0 <= match_hour <= 5 and current_hour >= 18:
                next_day = current_date + timedelta(days=1)
                formatted_date = next_day.strftime("%d-%m-%Y")
            else:
                formatted_date = current_date.strftime("%d-%m-%Y")
        except (ValueError, IndexError):
            formatted_date = current_date.strftime("%d-%m-%Y")
        
        match_entry = {
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

def normalize_team_name(team_name: str) -> str:
    """Normalize team names for comparison, handling women's team suffixes"""
    if not team_name:
        return ""
    
    name = team_name.strip().lower()
    
    # Handle women's team variations
    # Convert "team w" or "team (w)" to "team women"
    name = re.sub(r'\s+w$', ' women', name)  # "spain w" -> "spain women"
    name = re.sub(r'\s+\(w\)$', ' women', name)  # "spain (w)" -> "spain women"
    
    # Handle other common variations
    name = re.sub(r'\s+women$', ' women', name)  # Normalize multiple spaces
    name = re.sub(r'\s+female$', ' women', name)  # "female" -> "women"
    
    return name.strip()

def calculate_team_similarity(team1_a: str, team1_b: str, team2_a: str, team2_b: str) -> float:
    """Calculate similarity between two matches based on team names"""
    # Added validation for empty/None values
    if not all([team1_a, team1_b, team2_a, team2_b]):
        return 0.0
    
    # Normalize team names for better matching
    norm_team1_a = normalize_team_name(team1_a)
    norm_team1_b = normalize_team_name(team1_b)
    norm_team2_a = normalize_team_name(team2_a)
    norm_team2_b = normalize_team_name(team2_b)
    
    # Try both combinations: A1 vs A2 compared to B1 vs B2, and A1 vs A2 compared to B2 vs B1
    similarity1 = (SequenceMatcher(None, norm_team1_a, norm_team1_b).ratio() + 
                   SequenceMatcher(None, norm_team2_a, norm_team2_b).ratio()) / 2
    
    similarity2 = (SequenceMatcher(None, norm_team1_a, norm_team2_b).ratio() + 
                   SequenceMatcher(None, norm_team2_a, norm_team1_b).ratio()) / 2
    
    return max(similarity1, similarity2)

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
    """Merge new matches with existing data, updating where necessary"""
    logger.info(f"[{fetch_code}] Merging with existing data...")
    
    # Create a lookup for existing matches
    existing_lookup = {}
    for match in existing_matches:
        key = (match["team1"]["name"], match["team2"]["name"], match["date"])
        existing_lookup[key] = match
    
    merged_matches = []
    updated_count = 0
    new_count = 0
    
    for new_match in new_matches:
        key = (new_match["team1"]["name"], new_match["team2"]["name"], new_match["date"])
        
        if key in existing_lookup:
            existing_match = existing_lookup[key]
            
            # Check if we need to update
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

def merge_matches(streamed_matches: List[dict], sportsonline_matches: List[dict], fetch_code: str) -> List[dict]:
    """Merge matches from both sources based on team name similarity"""
    logger.info(f"[{fetch_code}] Merging matches based on team similarity...")
    
    merged_matches = []
    used_sportsonline = set()
    
    for streamed_match in streamed_matches:
        best_match = None
        best_similarity = 0
        best_index = -1
        
        streamed_team1 = streamed_match["team1"]["name"]
        streamed_team2 = streamed_match["team2"]["name"]
        
        for i, sportsonline_match in enumerate(sportsonline_matches):
            if i in used_sportsonline:
                continue
                
            sportsonline_team1 = sportsonline_match["team1"]["name"]
            sportsonline_team2 = sportsonline_match["team2"]["name"]
            
            similarity = calculate_team_similarity(
                streamed_team1, sportsonline_team1, 
                streamed_team2, sportsonline_team2
            )
            
            if similarity > best_similarity:
                best_similarity = similarity
                best_match = sportsonline_match
                best_index = i
        
        if best_similarity >= SIMILARITY_THRESHOLD:
            # Merge the matches
            combined_links = streamed_match["links"] + best_match["links"]
            # Remove duplicates before shuffling
            unique_links = list(dict.fromkeys(combined_links))  # Preserves order while removing duplicates
            random.shuffle(unique_links)
            
            merged_match = {
                "match_title_from_api": streamed_match["match_title_from_api"],
                "team1": streamed_match["team1"],  # Use streamed logos
                "team2": streamed_match["team2"],  # Use streamed logos
                "time": streamed_match["time"],    # Use streamed time
                "date": streamed_match["date"],    # Use streamed date
                "links": unique_links
            }
            merged_matches.append(merged_match)
            used_sportsonline.add(best_index)
            logger.info(f"[{fetch_code}] Merged: {streamed_team1} vs {streamed_team2} (similarity: {best_similarity:.2f})")
        else:
            # Keep streamed match as separate
            merged_matches.append(streamed_match)
    
    # Add remaining sportsonline matches that weren't merged
    for i, sportsonline_match in enumerate(sportsonline_matches):
        if i not in used_sportsonline:
            merged_matches.append(sportsonline_match)
    
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
        # Fetch from both sources
        streamed_matches = fetch_streamed_matches(fetch_code)
        sportsonline_matches = fetch_sportsonline_matches(fetch_code)
        
        # Merge the results from both sources
        merged_matches = merge_matches(streamed_matches, sportsonline_matches, fetch_code)
        
        # Load existing data and merge with new data
        existing_data = load_existing_data()
        final_matches = merge_with_existing_data(merged_matches, existing_data, fetch_code)
        
        # Save the final data
        save_data(final_matches, fetch_code)
        
        logger.info(f"[{fetch_code}] Summary:")
        logger.info(f"[{fetch_code}] - Streamed.su matches: {len(streamed_matches)}")
        logger.info(f"[{fetch_code}] - Sportsonline matches: {len(sportsonline_matches)}")
        logger.info(f"[{fetch_code}] - Final merged matches: {len(final_matches)}")
        
        logger.info(f"[{fetch_code}] Scraper run completed successfully")
        
    except Exception as e:
        logger.error(f"[{fetch_code}] Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()
