import requests
import json
import time
import logging
from datetime import datetime, timezone, timedelta
import os
import argparse

# The Realm's Configuration
SCROLL_ORIGIN = "https://streamed.pk/api"
EVENTS_SCROLL = "/matches/all"
VISION_PATH = "/stream/{source}/{id}"
ARCHIVES_LOCATION = "streamed_events.json"
SCRIBE_LOG = "winterfell_scribe.log"
DEFAULT_SIGIL = "https://cdn.jsdelivr.net/gh/drnewske/tyhdsjax-nfhbqsm/logos/myicon.png"
CITADEL_SOURCE_NAME = "The Citadel"

# 12 Hours (in seconds)
ANCIENT_SCROLL_LIMIT = 12 * 3600

# Set up the Maester's logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(SCRIBE_LOG),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("GrandMaester")

def consult_the_scrolls(url):
    """Fetch data from the ether."""
    try:
        logger.info(f"Sending raven to: {url}")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"The raven was lost on the way to {url}: {e}")
        return None

def is_ancient_history(event_timestamp_ms):
    """Check if the event is older than the allowed limit (12 hours)."""
    if not event_timestamp_ms:
        return False # Assume valid if no time, or handle otherwise.
    
    event_time = datetime.fromtimestamp(event_timestamp_ms / 1000, tz=timezone.utc)
    current_time = datetime.now(timezone.utc)
    
    # Check if event started more than 12 hours ago
    age = current_time - event_time
    is_old = age.total_seconds() > ANCIENT_SCROLL_LIMIT
    
    if is_old:
        logger.debug(f"Event from {event_time} is ancient ({age}). Discarding.")
        
    return is_old

def scribe_events(limit=None):
    """Gather events and write them to the archives."""
    scroll_data = consult_the_scrolls(f"{SCROLL_ORIGIN}{EVENTS_SCROLL}")
    
    if not scroll_data:
        logger.error("The archives are empty or inaccessible.")
        return {}

    new_knowledge = {}
    
    logger.info(f"Found {len(scroll_data)} potential entries in the scrolls.")

    count = 0
    for entry in scroll_data:
        if limit and count >= limit:
            logger.info(f"The Maester is tired. Stopping after {limit} entries.")
            break

        timestamp = entry.get("date") # Unix timestamp in ms
        
        # Filter out ancient events immediately from api data if they are already too old
        if timestamp and is_ancient_history(timestamp):
            continue

        match_id = entry.get("id")
        title = entry.get("title")
        
        # Convert timestamp to human readable date/time (Local/System time)
        if timestamp:
            dt_object = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
            dt_local = dt_object.astimezone() 
            date_str = dt_local.strftime("%d-%m-%Y")
            time_str = dt_local.strftime("%H:%M")
        else:
            date_str = ""
            time_str = ""

        sources = entry.get("sources", [])
        
        if not sources:
            continue

        # Fetch visions (streams)
        visions = []
        logger.info(f"Consulting visions for: {title}")

        for source_entry in sources:
            s_name = source_entry.get("source")
            s_id = source_entry.get("id")
            
            vision_url = f"{SCROLL_ORIGIN}{VISION_PATH.format(source=s_name, id=s_id)}"
            vision_data = consult_the_scrolls(vision_url)
            
            if vision_data:
                for vision in vision_data:
                    embed_url = vision.get("embedUrl")
                    if embed_url:
                        visions.append(embed_url)
            
            # Rest the raven briefly
            time.sleep(0.1)

        if visions:
            count += 1
            
            teams = entry.get("teams", {})
            home_team = teams.get("home", {})
            away_team = teams.get("away", {})
            
            home_badge = home_team.get("badge")
            away_badge = away_team.get("badge")
            
            home_logo = f"{SCROLL_ORIGIN}/images/badge/{home_badge}.webp" if home_badge else DEFAULT_SIGIL
            away_logo = f"{SCROLL_ORIGIN}/images/badge/{away_badge}.webp" if away_badge else DEFAULT_SIGIL

            event_record = {
                "source_name": CITADEL_SOURCE_NAME,
                "source_icon_url": DEFAULT_SIGIL, 
                "match_title_from_api": title,
                "team1": {
                    "name": home_team.get("name", "Unknown House"),
                    "logo_url": home_logo
                },
                "team2": {
                    "name": away_team.get("name", "Unknown House"),
                    "logo_url": away_logo
                },
                "time": time_str,
                "date": date_str,
                "links": list(set(visions)), # De-duplicate
                "match_id": match_id,
                "_timestamp": timestamp # Keep for validaton/cleanup comparison
            }
            new_knowledge[match_id] = event_record

    return new_knowledge

def load_archives():
    """Load the existing archives."""
    if os.path.exists(ARCHIVES_LOCATION):
        try:
            with open(ARCHIVES_LOCATION, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Convert list to dict keyed by match_id for easy lookup
                archives = {}
                for item in data:
                    mid = item.get("match_id")
                    if mid:
                        archives[mid] = item
                return archives
        except Exception as e:
            logger.warning(f"Could not read the ancient texts: {e}")
    return {}

def update_archives(new_data):
    """Merge new knowledge with the ancient archives."""
    archives = load_archives()
    
    # Update existing records with new data (this handles changes in data)
    # and add new records.
    for m_id, record in new_data.items():
        if m_id in archives:
            logger.info(f"Updating the chronicles for: {record['match_title_from_api']}")
        else:
            logger.info(f"Inscribing new event: {record['match_title_from_api']}")
        archives[m_id] = record
        
    # Cleanup Phase: Remove records that are too old (from ALL archives, not just new)
    clean_archives = []
    removed_count = 0
    
    for m_id, record in archives.items():
        # Check _timestamp first, if missing try to parse date/time or keep safely
        ts = record.get("_timestamp")
        
        # If we don't have _timestamp (legacy data), try to infer or check if we should keep it.
        # For this refactor, we trust the new scraper adds _timestamp. 
        # If it's missing, we might keep it or discard. Let's assume we keep unless validly proven old.
        # ACTUALLY, the user said "remove these old events too".
        # If we just fetched it and it wasn't returned, maybe it's over? 
        # But for now, we stick to the time-based rule logic requested: "older than 12 hours from their kick off time".
        
        is_old = False
        if ts:
             is_old = is_ancient_history(ts)
        else:
            # Try to parse string date/time if needed, or pass
            # live_events format: "date": "18-01-2026", "time": "11:15"
            try:
                d_str = record.get("date")
                t_str = record.get("time")
                if d_str and t_str:
                    # Parse dd-mm-yyyy HH:MM
                    dt_str = f"{d_str} {t_str}"
                    dt = datetime.strptime(dt_str, "%d-%m-%Y %H:%M")
                    # Assume local time.. complicating comparison.
                    # Best to rely on _timestamp if available.
                    # If legacy data doesn't have it, we might just leave it until it's overwritten or we're sure.
                    # However, "filter out ... older than 12 hours".
                    # Let's perform a rough check.
                    now = datetime.now()
                    if (now - dt).total_seconds() > ANCIENT_SCROLL_LIMIT:
                        is_old = True
            except:
                pass # Can't determine, keep it safe
        
        if is_old:
            removed_count += 1
            logger.info(f"Removing ancient scroll: {record.get('match_title_from_api')}")
        else:
            # Remove internal usage key before saving if desired, OR keep for next run
            # User said "keep the m out of the json", probably meant "them" (old events) or internal keys?
            # "keep the m out of the json" -> likely typo for "keep them out".
            # I will keep _timestamp for internal tracking but maybe remove if strict format needed.
            # But "keep track of each game ... update to effet the changes" implies persistence.
            # I'll keep _timestamp in file for robust tracking on next run. It's metadata. 
            # Wait, "keep the m out of the json" -> "keep them out" (the old events).
            clean_archives.append(record)

    logger.info(f"The Archives have been updated. Total: {len(clean_archives)}. Removed: {removed_count} ancient scrolls.")
    
    # Sort by time/date if possible? Or just list. List is fine.
    # Optional: Sort by timestamp
    clean_archives.sort(key=lambda x: x.get("_timestamp", 0) or 0)
    
    # Save
    with open(ARCHIVES_LOCATION, 'w', encoding='utf-8') as f:
        json.dump(clean_archives, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Winterfell Scribe')
    parser.add_argument('--limit', type=int, help='Limit number of visions to consult')
    args = parser.parse_args()

    logger.info("The Winter is Coming. The Scribe begins his work.")
    
    # 1. Fetch new data
    fresh_scrolls = scribe_events(limit=args.limit)
    
    # 2. Merge and Clean
    update_archives(fresh_scrolls)
    
    logger.info("The Scribe rests.")
