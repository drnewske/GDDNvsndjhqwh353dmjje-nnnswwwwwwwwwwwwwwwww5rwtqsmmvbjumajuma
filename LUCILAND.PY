import requests
import json
from datetime import datetime, timedelta, timezone
import os
import logging
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Disable SSL warnings (for cases where we disable SSL verification)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configuration ---
JSON_API_URL = "https://matchstream.do/api/v1/api.php"
OUTPUT_FILE = "live_events.json" # The file where all events (including potentially others) are stored
FOOTBALL_SCRAPER_LOG_FILE = "football_scraper.log" # Dedicated log file for this scraper

# Hardcoded values for this specific scraper's output
THIS_SCRAPER_SOURCE_NAME = "D.S ALT 1"
THIS_SCRAPER_SOURCE_ICON_URL = "https://raw.githubusercontent.com/drnewske/tyhdsjax-nfhbqsm/866f438738537d321da57d21459654e68a1691fa/logos/TOOTHBRUSH.jpg"
DEFAULT_TEAM_LOGO_URL = "https://cdn.jsdelivr.net/gh/drnewske/tyhdsjax-nfhbqsm/logos/default.png"

# Define the threshold for old matches: 24 hours ago from the current run time
OLD_MATCH_THRESHOLD_HOURS = 24

# --- Timezone Configuration ---
# Specify the time zone offset of the source API's time to convert it to UTC.
# This value is the number of hours to add or subtract from UTC.
# Examples:
#  0 for UTC
#  3 for UTC+3 (e.g., East Africa Time)
# -5 for UTC-5 (e.g., Eastern Standard Time)
SOURCE_TIMEZONE_OFFSET_HOURS = 3 # <<-- EDIT THIS VALUE FOR THE SOURCE TIMEZONE

# Set up logging for this specific scraper
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(FOOTBALL_SCRAPER_LOG_FILE),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

# --- Helper Functions ---

def create_session_with_retries():
    """Creates a requests session with retry strategy and SSL handling."""
    session = requests.Session()
    
    # Set up retry strategy
    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=1
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Set headers to mimic a real browser
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    
    return session

def load_all_existing_data(file_path):
    """Loads ALL existing JSON data from a file, regardless of source."""
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if not isinstance(data, list):
                    logger.warning(f"File {file_path} contains non-list data. Starting with empty data.")
                    return []
                logger.info(f"Loaded {len(data)} total existing matches from {file_path}.")
                return data
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from {file_path}: {e}. Starting with empty data.")
            return []
        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}. Starting with empty data.")
            return []
    logger.info(f"No existing data file found at {file_path}. Starting with empty data.")
    return []

def save_data(file_path, data):
    """Saves data to a JSON file."""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logger.info(f"Successfully saved {len(data)} matches to {file_path}.")
    except Exception as e:
        logger.error(f"Error saving data to {file_path}: {e}")

def get_match_unique_id(match):
    """Generates a unique ID for a match based on its key properties."""
    # Combine team names, date, and time for a robust unique identifier
    team1 = match.get('team1', {}).get('name', '').lower().replace(' ', '')
    team2 = match.get('team2', {}).get('name', '').lower().replace(' ', '')
    date = match.get('date', '')
    time = match.get('time', '')
    # Ensure consistent order for team names in the ID
    if team1 > team2:
        team1, team2 = team2, team1
    # Include source_name in the unique ID to ensure this scraper only manages its own entries
    source_name = match.get('source_name', '').lower().replace(' ', '')
    return f"{source_name}-{team1}-{team2}-{date}-{time}"

def is_vuen_link(link):
    """Check if a link contains 'vuen' in the domain."""
    return 'vuen' in link.lower()

# --- Main Scraper Logic ---

def run_football_scraper():
    """
    Scrapes football data from the API, transforms it, and manages
    only the matches added by this specific scraper in the output file.
    """
    logger.info("Starting LUCILAND football match scraper...")
    logger.info("============================================================")

    # Use UTC now for all time comparisons to ensure consistency
    current_time = datetime.utcnow()
    logger.info(f"Current UTC timestamp for this run: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # 1. Load ALL existing data from the output file
    all_existing_matches = load_all_existing_data(OUTPUT_FILE)

    # Separate matches managed by THIS scraper from others
    other_scrapers_matches = []
    this_scrapers_matches_map = {} # Using a map for efficient updates/removals

    for existing_match in all_existing_matches:
        if existing_match.get('source_name') == THIS_SCRAPER_SOURCE_NAME:
            unique_id = get_match_unique_id(existing_match)
            this_scrapers_matches_map[unique_id] = existing_match
        else:
            other_scrapers_matches.append(existing_match)
    logger.info(f"Found {len(this_scrapers_matches_map)} matches managed by '{THIS_SCRAPER_SOURCE_NAME}'.")
    logger.info(f"Found {len(other_scrapers_matches)} matches from other sources.")

    # 2. Fetch new data from the designated API with improved error handling
    raw_api_matches = []
    session = create_session_with_retries()
    
    try:
        logger.info(f"Fetching matches from {JSON_API_URL}...")
        
        # Try with SSL verification first
        try:
            response = session.get(JSON_API_URL, timeout=30)
            response.raise_for_status()
        except requests.exceptions.SSLError:
            logger.warning("SSL verification failed, retrying without SSL verification...")
            response = session.get(JSON_API_URL, verify=False, timeout=30)
            response.raise_for_status()
        
        raw_data = response.json()
        raw_api_matches = raw_data.get('matches', [])
        if not isinstance(raw_api_matches, list):
            logger.warning("The 'matches' key in the API response is not a list. Attempting to process raw_data directly.")
            if isinstance(raw_data, list):
                raw_api_matches = raw_data # Fallback if the top level is directly a list
            else:
                logger.error("API response is neither a list nor contains a 'matches' list. Cannot process.")
                raw_api_matches = []
        logger.info(f"Found {len(raw_api_matches)} total matches from {JSON_API_URL}.")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from {JSON_API_URL}: {e}. No new matches will be processed from this source.")
        raw_api_matches = [] # Ensure it's an empty list to proceed gracefully
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from {JSON_API_URL}: {e}. No new matches will be processed from this source.")
        raw_api_matches = [] # Ensure it's an empty list to proceed gracefully
    finally:
        session.close()

    # 3. Process and transform newly fetched matches
    transformed_new_football_matches = []
    logger.info("Filtering and transforming new matches for 'Football' from API...")
    for match in raw_api_matches:
        if match.get('sport') == 'Football': # Filter for 'Football'
            match_date_str = match.get('matchDate')
            source_time_str = match.get('time')
            utc_date_str = None
            utc_time_str = None
            match_datetime_for_check = None # Naive UTC datetime for chronological checks

            if match_date_str and source_time_str:
                try:
                    # --- UTC Conversion Logic ---
                    # 1. Create a naive datetime object from the source data
                    naive_dt = datetime.strptime(f"{match_date_str} {source_time_str}", '%Y-%m-%d %H:%M')
                    # 2. Define the source timezone based on the configured offset
                    source_tz = timezone(timedelta(hours=SOURCE_TIMEZONE_OFFSET_HOURS))
                    # 3. Make the datetime object timezone-aware (localize it to its source timezone)
                    aware_dt = naive_dt.replace(tzinfo=source_tz)
                    # 4. Convert to UTC
                    utc_dt = aware_dt.astimezone(timezone.utc)
                    # 5. Prepare UTC-based strings for the JSON output and a naive datetime for comparison
                    utc_date_str = utc_dt.strftime('%d-%m-%Y')
                    utc_time_str = utc_dt.strftime('%H:%M')
                    match_datetime_for_check = utc_dt.replace(tzinfo=None)
                    # --- End UTC Conversion Logic ---
                except ValueError:
                    logger.warning(f"Could not parse date/time '{match_date_str} {source_time_str}' for match '{match.get('matchText')}'. Skipping.")
                    continue # Skip this match if date/time is invalid

            # Skip matches that are in the past relative to current run time (prevent adding old matches)
            if match_datetime_for_check and match_datetime_for_check < current_time - timedelta(hours=OLD_MATCH_THRESHOLD_HOURS):
                logger.info(f"Skipping newly fetched match, it's older than {OLD_MATCH_THRESHOLD_HOURS} hours: {match.get('matchText')} ({utc_date_str} {utc_time_str} UTC)")
                continue

            all_links = []
            vuen_links_filtered = 0
            channels = match.get('channels', [])
            if isinstance(channels, list):
                for channel in channels:
                    channel_links = channel.get('links', [])
                    if isinstance(channel_links, list):
                        # Ensure links are strings and non-empty, and filter out vuen links
                        for link in channel_links:
                            if isinstance(link, str) and link.strip():
                                if is_vuen_link(link):
                                    vuen_links_filtered += 1
                                    logger.debug(f"Filtered out vuen link: {link}")
                                else:
                                    all_links.append(link)

            if vuen_links_filtered > 0:
                logger.info(f"Filtered out {vuen_links_filtered} vuen links for match: {match.get('matchText')}")

            # Skip matches with no valid stream links (after filtering)
            if not all_links:
                logger.warning(f"Skipping match with no valid stream links after filtering: {match.get('matchText')}")
                continue

            # Handle potentially missing/null team data gracefully
            team1_name = match.get('team1')
            team2_name = match.get('team2')
            if not team1_name or not team2_name:
                logger.warning(f"Skipping match with invalid team data: {match.get('matchText')} (Team1: {team1_name}, Team2: {team2_name})")
                continue

            # Construct the transformed match dictionary
            transformed_match = {
                "source_name": THIS_SCRAPER_SOURCE_NAME, # Explicitly set this scraper's source name
                "source_icon_url": THIS_SCRAPER_SOURCE_ICON_URL,
                "match_title_from_api": match.get('matchText'),
                "team1": {
                    "name": team1_name,
                    "logo_url": DEFAULT_TEAM_LOGO_URL
                },
                "team2": {
                    "name": team2_name,
                    "logo_url": DEFAULT_TEAM_LOGO_URL
                },
                "time": utc_time_str,
                "date": utc_date_str,
                "links": all_links
            }
            transformed_new_football_matches.append(transformed_match)
    logger.info(f"Transformed {len(transformed_new_football_matches)} valid football matches from API response.")

    # 4. Merge new matches with THIS scraper's existing data
    updated_this_scraper_matches_count = 0
    new_this_scraper_matches_count = 0

    for new_match in transformed_new_football_matches:
        unique_id = get_match_unique_id(new_match)
        if unique_id in this_scrapers_matches_map:
            # Update existing match added by THIS scraper
            # This directly modifies the object in the map
            this_scrapers_matches_map[unique_id].update(new_match)
            updated_this_scraper_matches_count += 1
            # logger.info(f"Updated match: {new_match.get('match_title_from_api')} (managed by this scraper)")
        else:
            # Add new match for THIS scraper
            this_scrapers_matches_map[unique_id] = new_match
            new_this_scraper_matches_count += 1
            # logger.info(f"New match: {new_match.get('match_title_from_api')} (added by this scraper)")

    logger.info(f"Merge for '{THIS_SCRAPER_SOURCE_NAME}' data: {new_this_scraper_matches_count} new, {updated_this_scraper_matches_count} updated.")

    # 5. Filter out old matches ONLY from THIS scraper's managed matches
    cleaned_this_scrapers_matches = []
    removed_this_scrapers_old_count = 0

    for unique_id, match in this_scrapers_matches_map.items():
        match_date_str = match.get('date')
        match_time_str = match.get('time')

        if match_date_str and match_time_str:
            try:
                # Convert DD-MM-YYYY to YYYY-MM-DD for datetime parsing
                parsed_date_str = datetime.strptime(match_date_str, '%d-%m-%Y').strftime('%Y-%m-%d')
                match_datetime = datetime.strptime(f"{parsed_date_str} {match_time_str}", '%Y-%m-%d %H:%M')

                if match_datetime < current_time - timedelta(hours=OLD_MATCH_THRESHOLD_HOURS):
                    logger.info(f"Removed old match (managed by this scraper): {match.get('match_title_from_api')} ({match_date_str} {match_time_str})")
                    removed_this_scrapers_old_count += 1
                    continue # Skip this match, it's too old
            except ValueError:
                logger.warning(f"Could not parse stored date/time '{match_date_str} {match_time_str}' for cleanup. Keeping match (managed by this scraper).")

        cleaned_this_scrapers_matches.append(match) # Keep if not too old or date/time issue

    logger.info(f"Cleanup for '{THIS_SCRAPER_SOURCE_NAME}' data: Removed {removed_this_scrapers_old_count} old matches, {len(cleaned_this_scrapers_matches)} matches remaining.")

    # 6. Combine all matches: other scrapers' matches + this scraper's cleaned matches
    final_combined_matches = other_scrapers_matches + cleaned_this_scrapers_matches

    # 7. Sort all matches by date and time for consistent output
    def sort_key(match):
        date_str = match.get('date')
        time_str = match.get('time')
        if date_str and time_str:
            try:
                # Convert back to YYYY-MM-DD for reliable sorting
                parsed_date_str = datetime.strptime(date_str, '%d-%m-%Y').strftime('%Y-%m-%d')
                return datetime.strptime(f"{parsed_date_str} {time_str}", '%Y-%m-%d %H:%M')
            except ValueError:
                logger.warning(f"Could not parse date/time for sorting: {date_str} {time_str}. Will place at start of list.")
        return datetime.min # Fallback for invalid dates, placing them at the beginning

    final_combined_matches.sort(key=sort_key)

    # 8. Save the final combined data to live_events.json
    save_data(OUTPUT_FILE, final_combined_matches)

    logger.info("LUCILAND scraper run completed successfully.")
    logger.info(f"Summary for '{THIS_SCRAPER_SOURCE_NAME}':")
    logger.info(f"- Matches fetched from {JSON_API_URL}: {len(raw_api_matches)}")
    logger.info(f"- Valid Football matches processed from API: {len(transformed_new_football_matches)}")
    logger.info(f"- Final matches added/updated by this scraper: {len(cleaned_this_scrapers_matches)}")
    logger.info(f"- Total matches in {OUTPUT_FILE} after this run: {len(final_combined_matches)}")

    return final_combined_matches

if __name__ == "__main__":
    transformed_football_data = run_football_scraper()

    if transformed_football_data:
        logger.info("\n--- Final Transformed Football Data (Sample) ---")
        # Print a sample of the final data to console if it's too large, or all if small
        # For demonstration, printing the first 5 and last 5, or all if less than 10
        if len(transformed_football_data) > 10:
            print(json.dumps(transformed_football_data[:5], indent=4))
            print("...\n(showing first 5 matches and last 5 matches)\n...")
            print(json.dumps(transformed_football_data[-5:], indent=4))
        else:
            print(json.dumps(transformed_football_data, indent=4))
        luciland_matches = [m for m in transformed_football_data if m.get('source_name') == THIS_SCRAPER_SOURCE_NAME]
        logger.info(f"Total Football matches in final output managed by LUCILAND: {len(luciland_matches)}")
    else:
        logger.error("LUCILAND scraper encountered an error and did not produce data.")
