import sys
import os
import platform
import jsonpickle
import redis
from minio import Minio
from minio.error import S3Error
import requests
import subprocess
import shutil
import time

# --- Configuration (Uses environment variables for Kubernetes deployment) ---
REDIS_HOST = os.getenv("REDIS_HOST") or "localhost"
REDIS_PORT = os.getenv("REDIS_PORT") or 6379
QUEUE_KEY = "toWorker"

MINIO_HOST = os.getenv("MINIO_HOST") or "localhost"
MINIO_PORT = os.getenv("MINIO_PORT") or 9000
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY") or "rootuser"
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY") or "rootpass123"
MINIO_ENDPOINT = f"{MINIO_HOST}:{MINIO_PORT}"

# Min.io Buckets
QUEUE_BUCKET = "queue"
OUTPUT_BUCKET = "output"

# Demucs paths and settings
DEMUCS_OUTPUT_DIR_NAME = "mdx_extra_q" 
TEMP_INPUT_DIR = "/tmp/demucs_input"
TEMP_OUTPUT_DIR = "/tmp/demucs_output"
OUTPUT_TRACKS = ['vocals.mp3', 'drums.mp3', 'bass.mp3', 'other.mp3']


# --- Initialization and Clients ---

# The Worker requires a high memory limit (8Gi) due to Demucs/PyTorch.

# 1. Initialize Redis Client
try:
    redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    redis_client.ping()
    print(f"INFO: Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except Exception as e:
    print(f"FATAL: Could not connect to Redis: {e}", file=sys.stderr)
    sys.exit(1)

# 2. Initialize Min.io Client
try:
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    print(f"INFO: Connected to Min.io at {MINIO_ENDPOINT}")
except Exception as e:
    print(f"FATAL: Could not initialize Min.io client: {e}", file=sys.stderr)
    sys.exit(1)


# --- Logging Utility (Pushing to the centralized 'logging' list in Redis) ---

infoKey = f"{platform.node()}.worker.info"
debugKey = f"{platform.node()}.worker.debug"

def log_message(level, message, key=None):
    """Pushes a structured log message to the Redis 'logging' list."""
    if level == "INFO":
        key = key or infoKey
    elif level == "DEBUG":
        key = key or debugKey
    else:
        key = key or infoKey
        
    log_text = f"{level}:{key}:{message}"
        
    print(log_text, file=sys.stdout)
    
    try:
        redis_client.lpush('logging', log_text.encode('utf-8'))
    except Exception as e:
        print(f"ERROR: Failed to push log message to Redis: {e}", file=sys.stderr)


# --- Core Worker Loop ---

def process_tasks():
    """Continuously monitors the Redis queue and processes separation jobs."""
    log_message("INFO", "Worker started. Waiting for tasks...")
    
    # Ensure temporary directories exist and are clean
    shutil.rmtree(TEMP_INPUT_DIR, ignore_errors=True)
    shutil.rmtree(TEMP_OUTPUT_DIR, ignore_errors=True)
    os.makedirs(TEMP_INPUT_DIR, exist_ok=True)
    os.makedirs(TEMP_OUTPUT_DIR, exist_ok=True)


    while True:
        try:
            # BLPOP waits indefinitely (timeout=0) for a task on the QUEUE_KEY ('toWorker')
            # The payload is expected to be a JSON string from the REST server
            work = redis_client.blpop(QUEUE_KEY, timeout=0)
            
            # work is a tuple: (queue_name, payload_bytes)
            payload_bytes = work[1]
            task = jsonpickle.decode(payload_bytes.decode('utf-8'))
            
            songhash = task.get('songhash')
            object_name = f"{songhash}.mp3" # Assume REST server saved it as hash.mp3
            callback = task.get('callback', {})
            
            log_message("INFO", f"Received job for hash: {songhash}")

            # 1. Download MP3 from Min.io (Queue Bucket)
            local_input_path = os.path.join(TEMP_INPUT_DIR, object_name)
            
            try:
                # fget_object is used for downloading to a local file path
                minio_client.fget_object(QUEUE_BUCKET, object_name, local_input_path)
                log_message("INFO", f"Downloaded {object_name} from {QUEUE_BUCKET}")
            except S3Error as e:
                log_message("ERROR", f"Failed to download {object_name}. Check if object exists. Error: {e}")
                continue # Skip to next task
            
            # 2. Execute Demucs Separation (The resource-intensive step)
            
            # Command: python3 -m demucs.separate --mp3 --out /tmp/output /path/to/input.mp3
            # Demucs automatically creates output structure: /tmp/output/mdx_extra_q/songhash_without_ext/...
            
            demucs_command = (
                f"python3 -m demucs.separate "
                f"--mp3 " # Output separated tracks as MP3s
                f"--out {TEMP_OUTPUT_DIR} "
                f"{local_input_path}"
            )
            
            log_message("DEBUG", f"Running Demucs: {demucs_command}")
            
            # subprocess.run is used to execute the external command
            result = subprocess.run(demucs_command, shell=True, capture_output=True, text=True)
            
            if result.returncode != 0:
                log_message("ERROR", f"Demucs failed (Code: {result.returncode}). Stderr: {result.stderr.strip()}")
                continue
            
            log_message("INFO", "Demucs separation complete.")

            # 3. Upload Results to Min.io (Output Bucket)
            
            # Demucs directory naming uses the input file name minus the extension
            demucs_output_name = songhash 
            demucs_song_dir = os.path.join(TEMP_OUTPUT_DIR, DEMUCS_OUTPUT_DIR_NAME, demucs_output_name)
            
            uploaded_tracks = []
            
            # Iterate through the expected output files
            for track_file in os.listdir(demucs_song_dir):
                if track_file.endswith(".mp3"):
                    local_track_path = os.path.join(demucs_song_dir, track_file)
                    
                    # Target object name format: <songhash>-<track>.mp3 (e.g., hash-vocals.mp3)
                    target_object_name = f"{songhash}-{track_file}"
                    
                    # fput_object uploads a local file path to the Min.io object storage
                    minio_client.fput_object(
                        OUTPUT_BUCKET,
                        target_object_name,
                        local_track_path,
                        content_type='audio/mpeg'
                    )
                    uploaded_tracks.append(target_object_name)
                    
            log_message("INFO", f"Uploaded {len(uploaded_tracks)} tracks to {OUTPUT_BUCKET}.")

            # 4. Handle Callback (Optional)
            if callback and callback.get('url'):
                try:
                    # Pass the original data back to the callback URL
                    requests.post(callback['url'], json={"songhash": songhash, "status": "completed"})
                    log_message("INFO", f"Sent completion callback to {callback['url']}")
                except Exception as e:
                    log_message("WARNING", f"Callback to {callback['url']} failed: {e}")

            # 5. Cleanup local files
            shutil.rmtree(TEMP_INPUT_DIR, ignore_errors=True)
            shutil.rmtree(TEMP_OUTPUT_DIR, ignore_errors=True)
            os.makedirs(TEMP_INPUT_DIR, exist_ok=True)
            os.makedirs(TEMP_OUTPUT_DIR, exist_ok=True)
            log_message("DEBUG", f"Cleaned up temporary storage for {songhash}.")

        except Exception as e:
            log_message("ERROR", f"Unhandled exception in worker loop: {e}. Waiting 5s before restart.")
            time.sleep(5) # Wait before trying to dequeue again

if __name__ == '__main__':
    process_tasks()

