import requests
import json
import base64
import hashlib
import secrets
import hmac
import struct
import time
import logging
import os
import re
import uuid
import glob
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# --- CONFIGURATION ---
# URL to your remote config.json file (e.g., a raw GitHub Gist URL).
# This is the ONLY URL you need to configure here.
CONFIG_URL = "https://gist.githubusercontent.com/your-username/your-gist-id/raw/config.json"

# Log file configuration
LOG_FILE = "encryptor_service.log"
LOG_CLEANUP_HOURS = 72  # Clean up log entries older than 3 days

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- UTILITY FUNCTIONS ---

def generate_run_code() -> str:
    """Generate a unique code for this encryption run."""
    return f"ENCRYPT-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{str(uuid.uuid4())[:8].upper()}"

def cleanup_old_logs(run_code: str):
    """Clean up old log entries from the log file."""
    logger.info(f"[{run_code}] Starting cleanup of old log entries...")
    if not os.path.exists(LOG_FILE):
        logger.info(f"[{run_code}] No log file found at '{LOG_FILE}', skipping cleanup.")
        return

    cutoff_time = datetime.now() - timedelta(hours=LOG_CLEANUP_HOURS)
    valid_lines = []
    removed_count = 0

    try:
        with open(LOG_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        for line in lines:
            timestamp_match = re.match(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
            if timestamp_match:
                log_datetime = datetime.strptime(timestamp_match.group(1), '%Y-%m-%d %H:%M:%S')
                if log_datetime < cutoff_time:
                    removed_count += 1
                    continue
            valid_lines.append(line)

        if removed_count > 0:
            with open(LOG_FILE, 'w', encoding='utf-8') as f:
                f.writelines(valid_lines)
            logger.info(f"[{run_code}] Log cleanup complete. Removed {removed_count} old log entries.")
        else:
            logger.info(f"[{run_code}] No old log entries to remove.")

    except Exception as e:
        logger.error(f"[{run_code}] An error occurred during log cleanup: {e}", exc_info=True)

def cleanup_old_log_files(run_code: str):
    """Clean up old rotated log files (e.g., encryptor_service.log.1)."""
    log_pattern = f"{LOG_FILE}.*"
    log_files = glob.glob(log_pattern)
    cutoff_time = datetime.now() - timedelta(hours=LOG_CLEANUP_HOURS)
    removed_files = 0

    for log_file in log_files:
        if log_file == LOG_FILE:  # Skip the active log file
            continue
        try:
            file_mtime = datetime.fromtimestamp(os.path.getmtime(log_file))
            if file_mtime < cutoff_time:
                os.remove(log_file)
                removed_files += 1
                logger.info(f"[{run_code}] Removed old log file: {log_file}")
        except OSError as e:
            logger.warning(f"[{run_code}] Could not remove old log file {log_file}: {e}")

    if removed_files > 0:
        logger.info(f"[{run_code}] Removed {removed_files} old log files.")
    else:
        logger.info(f"[{run_code}] No old rotated log files to remove.")

# --- CORE ENCRYPTOR CLASS ---

class LiveDataEncryptor:
    def __init__(self, run_code: str):
        """Initializes the encryptor."""
        self.run_code = run_code
        self.config: Optional[Dict[str, Any]] = None
        # Generate the long, nonsensical output filename
        self.output_file = f"{secrets.token_hex(25)}.json"
        self.max_data_size = 10 * 1024 * 1024  # 10 MB

    def fetch_remote_config(self) -> bool:
        """Fetches and validates the remote configuration file."""
        logger.info(f"[{self.run_code}] Fetching remote configuration from {CONFIG_URL}...")
        try:
            response = requests.get(CONFIG_URL, timeout=15)
            response.raise_for_status()
            config_data = response.json()

            required_keys = ["app_salt", "app_identifier", "version", "live_data_url", "key_iterations"]
            for key in required_keys:
                if key not in config_data:
                    raise ValueError(f"Missing required key in config: '{key}'")

            self.config = config_data
            logger.info(f"[{self.run_code}] ✅ Remote configuration loaded successfully.")
            return True
        except Exception as e:
            logger.error(f"[{self.run_code}] ❌ FAILED to fetch or validate remote config: {e}", exc_info=True)
            return False

    def fetch_live_data(self) -> Optional[Dict[Any, Any]]:
        """Fetches the raw live data from the URL specified in the config."""
        if not self.config:
            logger.error(f"[{self.run_code}] Cannot fetch live data: configuration not loaded.")
            return None

        live_data_url = self.config['live_data_url']
        logger.info(f"[{self.run_code}] Fetching live data from {live_data_url}...")
        try:
            response = requests.get(live_data_url, timeout=20)
            response.raise_for_status()
            logger.info(f"[{self.run_code}] ✅ Live data fetched successfully.")
            return response.json()
        except Exception as e:
            logger.error(f"[{self.run_code}] ❌ FAILED to fetch live data: {e}", exc_info=True)
            return None

    def generate_deterministic_key(self, seed: str, salt: bytes, purpose: str) -> bytes:
        """Generates a deterministic key using PBKDF2-HMAC-SHA256."""
        combined = f"{seed}:{self.config['app_identifier']}:{self.config['version']}:{purpose}".encode('utf-8')
        dk = hashlib.pbkdf2_hmac('sha256', combined, salt, self.config['key_iterations'])
        return dk[:32]

    def stream_encrypt(self, data: bytes, key: bytes, iv: bytes) -> bytes:
        """Encrypts data using a stream cipher approach."""
        result = bytearray()
        key_hash = hashlib.sha256(key + iv).digest()
        for i, byte in enumerate(data):
            pos_key = hashlib.sha256(key_hash + struct.pack('<I', i)).digest()[0]
            result.append(byte ^ pos_key)
        return bytes(result)

    def create_hmac(self, data: bytes, key: bytes) -> bytes:
        """Creates an HMAC-SHA256 tag for authentication."""
        return hmac.new(key, data, hashlib.sha256).digest()

    def encrypt_payload(self, data: Dict[Any, Any]) -> Optional[Dict[str, Any]]:
        """Encrypts the given data payload using the loaded configuration."""
        logger.info(f"[{self.run_code}] Starting encryption process...")
        try:
            json_str = json.dumps(data, separators=(',', ':'))
            json_bytes = json_str.encode('utf-8')

            if len(json_bytes) > self.max_data_size:
                raise ValueError(f"Data size ({len(json_bytes)}) exceeds max size ({self.max_data_size})")

            app_salt = self.config['app_salt'].encode('utf-8')
            master_seed = f"{self.config['app_identifier']}:{self.config['version']}"

            key1 = self.generate_deterministic_key(master_seed, app_salt, "layer1")
            key2 = self.generate_deterministic_key(master_seed, app_salt, "layer2")
            hmac_key = self.generate_deterministic_key(master_seed, app_salt, "hmac")

            iv = secrets.token_bytes(16)
            timestamp = struct.pack('<Q', int(time.time()))
            data_with_timestamp = timestamp + json_bytes

            encrypted_layer1 = self.stream_encrypt(data_with_timestamp, key1, iv)
            encrypted_layer2 = self.stream_encrypt(encrypted_layer1, key2, iv)

            message_to_auth = iv + encrypted_layer2
            auth_tag = self.create_hmac(message_to_auth, hmac_key)

            final_payload = iv + encrypted_layer2 + auth_tag
            encrypted_string = base64.b64encode(final_payload).decode('ascii')

            logger.info(f"[{self.run_code}] ✅ Encryption successful.")
            return {
                "encrypted_data": encrypted_string,
                "timestamp": int(time.time()),
                "status": "success",
                "data_size": len(json_bytes),
            }
        except Exception as e:
            logger.error(f"[{self.run_code}] ❌ FAILED during encryption: {e}", exc_info=True)
            return None

    def save_encrypted_data(self, encrypted_result: Dict[str, Any]) -> bool:
        """Saves the final encrypted blob to its unique file."""
        logger.info(f"[{self.run_code}] Saving encrypted data to '{self.output_file}'...")
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(encrypted_result, f, indent=2)
            logger.info(f"[{self.run_code}] ✅ Data saved successfully.")
            return True
        except Exception as e:
            logger.error(f"[{self.run_code}] ❌ FAILED to save encrypted data: {e}", exc_info=True)
            return False

    def run_encryption_cycle(self):
        """Runs the complete cycle: fetch config, fetch data, encrypt, save."""
        if not self.fetch_remote_config():
            return

        live_data = self.fetch_live_data()
        if live_data is None:
            return

        encrypted_result = self.encrypt_payload(live_data)
        if encrypted_result is None:
            return

        self.save_encrypted_data(encrypted_result)

def main():
    """Main function to run the encryption service."""
    run_code = generate_run_code()
    logger.info(f"[{run_code}] 🚀 Starting Encryptor Service Run")
    logger.info("="*60)

    try:
        cleanup_old_logs(run_code)
        cleanup_old_log_files(run_code)

        encryptor = LiveDataEncryptor(run_code)
        encryptor.run_encryption_cycle()

    except Exception as e:
        logger.critical(f"[{run_code}] 💥 A critical error occurred in the main execution: {e}", exc_info=True)
    finally:
        logger.info(f"[{run_code}] 🏁 Encryptor Service Run Finished")
        logger.info("="*60 + "\n")

if __name__ == "__main__":
    main()
