import requests
import json
from datetime import datetime, timedelta
import os
import logging

# --- Configuration ---
JSON_URL = "https://matchstream.do/api/v1/api.php"
OUTPUT_FILE = "live_events.json"
LOG_FILE = "scraper555.log"
DEFAULT_SOURCE_NAME = "LUCIFER'S LANDLORD"
DEFAULT_SOURCE_ICON_URL = "https://raw.githubusercontent.com/drnewske/tyhdsjax-nfhbqsm/866f438738537d321da57d21459654e68a1691fa/logos/TOOTHBRUSH.jpg"
DEFAULT_TEAM_LOGO_URL = "https://cdn.jsdelivr.net/gh/drnewske/tyhdsjax-nfhbqsm/logos/default.png"
# Define the threshold for old matches: 24 hours ago from the current run time
OLD_MATCH_THRESHOLD_HOURS = 24
# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(LOG_FILE),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

# --- Helper Functions ---

def load_existing_data(file_path):
    """Loads existing JSON data from a file."""
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                logger.info(f"Loaded {len(data)} existing matches from {file_path}")
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
        logger.info(f"Successfully saved {len(data)} matches to {file_path}")
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
    return f"{team1}-{team2}-{date}-{time}"

# --- Main Scraper Logic ---

def scrape_and_transform_football_data(url):
    """
    Scrapes JSON data from a given URL, filters for 'Football' matches,
    transforms it, merges with existing data, and cleans up old matches.
    """
    logger.info("Starting combined football match scraper...")
    logger.info("============================================================")

    current_time = datetime.now()
    logger.info(f"Current timestamp for this run: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # 1. Load existing data
    existing_matches = load_existing_data(OUTPUT_FILE)
    existing_match_ids = {get_match_unique_id(m): m for m in existing_matches}
    logger.info(f"Loaded {len(existing_matches)} existing matches for merging.")

    # 2. Fetch new data
    new_raw_matches = []
    try:
        logger.info(f"Fetching matches from {url}...")
        response = requests.get(url)
        response.raise_for_status()
        raw_data = response.json()
        new_raw_matches = raw_data.get('matches', [])
        if not isinstance(new_raw_matches, list):
            logger.warning("The 'matches' key in the API response is not a list. Attempting to process raw_data directly.")
            new_raw_matches = raw_data # Fallback
        logger.info(f"Found {len(new_raw_matches)} total matches from {url}.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from {url}: {e}")
        new_raw_matches = [] # Continue with no new matches if fetch fails
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from {url}: {e}")
        new_raw_matches = [] # Continue with no new matches if JSON is invalid

    # 3. Process and transform new matches
    transformed_new_matches = []
    logger.info("Filtering and transforming new matches for 'Football'...")
    for match in new_raw_matches:
        if match.get('sport') == 'Football':
            match_date_str = match.get('matchDate')
            match_time_str = match.get('time')
            formatted_date = None
            match_datetime = None

            if match_date_str and match_time_str:
                try:
                    date_obj = datetime.strptime(match_date_str, '%Y-%m-%d')
                    formatted_date = date_obj.strftime('%d-%m-%Y')
                    # Create full datetime object for comparison
                    match_datetime = datetime.strptime(f"{match_date_str} {match_time_str}", '%Y-%m-%d %H:%M')
                except ValueError:
                    logger.warning(f"Could not parse date/time '{match_date_str} {match_time_str}' for match '{match.get('matchText')}'. Skipping.")
                    continue # Skip this match if date/time is invalid

            # Skip matches that are in the past relative to current run time
            if match_datetime and match_datetime < current_time - timedelta(hours=OLD_MATCH_THRESHOLD_HOURS):
                logger.info(f"Skipping old match: {match.get('matchText')} ({formatted_date} {match_time_str})")
                continue # Skip old matches

            all_links = []
            channels = match.get('channels', [])
            if isinstance(channels, list):
                for channel in channels:
                    channel_links = channel.get('links', [])
                    if isinstance(channel_links, list):
                        # Ensure links are strings and valid (e.g., not empty)
                        valid_links = [link for link in channel_links if isinstance(link, str) and link.strip()]
                        all_links.extend(valid_links)

            # Skip matches with no valid stream links (similar to your log)
            if not all_links:
                logger.warning(f"Skipping match with no valid stream links: {match.get('matchText')}")
                continue

            # Handle potentially missing team data gracefully
            team1_name = match.get('team1')
            team2_name = match.get('team2')
            if not team1_name or not team2_name:
                logger.warning(f"Skipping match with invalid team data: {match.get('matchText')} (Team1: {team1_name}, Team2: {team2_name})")
                continue

            transformed_match = {
                "source_name": DEFAULT_SOURCE_NAME,
                "source_icon_url": DEFAULT_SOURCE_ICON_URL,
                "match_title_from_api": match.get('matchText'),
                "team1": {
                    "name": team1_name,
                    "logo_url": DEFAULT_TEAM_LOGO_URL
                },
                "team2": {
                    "name": team2_name,
                    "logo_url": DEFAULT_TEAM_LOGO_URL
                },
                "time": match.get('time'),
                "date": formatted_date,
                "links": all_links
            }
            transformed_new_matches.append(transformed_match)
    logger.info(f"Transformed {len(transformed_new_matches)} new football matches from API response.")

    # 4. Merge new matches with existing data
    updated_matches_count = 0
    new_matches_count = 0
    final_matches_map = existing_match_ids.copy() # Start with existing matches

    for new_match in transformed_new_matches:
        unique_id = get_match_unique_id(new_match)
        if unique_id in final_matches_map:
            # Update existing match (e.g., if links have changed)
            final_matches_map[unique_id].update(new_match) # This will overwrite existing keys with new_match's values
            updated_matches_count += 1
            # logger.info(f"Updated: {new_match.get('match_title_from_api')} ({new_match.get('date')} {new_match.get('time')})")
        else:
            # Add new match
            final_matches_map[unique_id] = new_match
            new_matches_count += 1
            # logger.info(f"New match: {new_match.get('match_title_from_api')} ({new_match.get('date')} {new_match.get('time')})")

    logger.info(f"Merge complete: {new_matches_count} new, {updated_matches_count} updated.")

    # 5. Filter out old matches from the combined list
    cleaned_matches = []
    removed_old_count = 0
    
    for unique_id, match in final_matches_map.items():
        match_date_str = match.get('date')
        match_time_str = match.get('time')
        
        if match_date_str and match_time_str:
            try:
                # Convert DD-MM-YYYY to YYYY-MM-DD for datetime parsing
                parsed_date_str = datetime.strptime(match_date_str, '%d-%m-%Y').strftime('%Y-%m-%d')
                match_datetime = datetime.strptime(f"{parsed_date_str} {match_time_str}", '%Y-%m-%d %H:%M')
                
                if match_datetime < current_time - timedelta(hours=OLD_MATCH_THRESHOLD_HOURS):
                    logger.info(f"Removed old match: {match.get('match_title_from_api')} ({match_date_str} {match_time_str})")
                    removed_old_count += 1
                    continue # Skip this match, it's too old
            except ValueError:
                logger.warning(f"Could not parse stored date/time '{match_date_str} {match_time_str}' for cleanup. Keeping match.")
        
        cleaned_matches.append(match) # Keep if not too old or date/time issue

    logger.info(f"Cleanup complete: Removed {removed_old_count} old matches, {len(cleaned_matches)} matches remaining.")

    # 6. Sort matches by date and time for consistent output
    def sort_key(match):
        date_str = match.get('date')
        time_str = match.get('time')
        if date_str and time_str:
            try:
                # Convert back to YYYY-MM-DD for reliable sorting
                parsed_date_str = datetime.strptime(date_str, '%d-%m-%Y').strftime('%Y-%m-%d')
                return datetime.strptime(f"{parsed_date_str} {time_str}", '%Y-%m-%d %H:%M')
            except ValueError:
                logger.warning(f"Could not parse date/time for sorting: {date_str} {time_str}")
        return datetime.min # Fallback for invalid dates
    
    cleaned_matches.sort(key=sort_key)

    # 7. Save the final data
    save_data(OUTPUT_FILE, cleaned_matches)

    logger.info("Scraper run completed successfully.")
    logger.info(f"Summary:")
    logger.info(f"- Matches from {url}: {len(new_raw_matches)}")
    logger.info(f"- Final processed football matches: {len(transformed_new_matches)}")
    logger.info(f"- Final merged matches in {OUTPUT_FILE}: {len(cleaned_matches)}")

    return cleaned_matches

if __name__ == "__main__":
    transformed_football_data = scrape_and_transform_football_data(JSON_URL)

    if transformed_football_data:
        logger.info("\n--- Final Transformed Football Data ---")
        # Print the transformed data in a human-readable format to console
        print(json.dumps(transformed_football_data, indent=4))
        logger.info(f"Total Football matches in final output: {len(transformed_football_data)}")
    else:
        logger.error("Failed to scrape or transform Football data.")
