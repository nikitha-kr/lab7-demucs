import sys
import os
import platform
import base64
import hashlib
import io
import json

# Third-party libraries
from flask import Flask, request, jsonify, send_file
import redis
from minio import Minio
from minio.error import S3Error
import jsonpickle
import requests # Needed for potential callback later

# --- Configuration ---

# Redis connection (default port-forwarded access)
REDIS_HOST = os.getenv("REDIS_HOST") or "localhost"
REDIS_PORT = os.getenv("REDIS_PORT") or 6379
QUEUE_KEY = "toWorker" 

# Min.io connection (default port-forwarded access and credentials)
MINIO_HOST = os.getenv("MINIO_HOST") or "localhost"
MINIO_PORT = os.getenv("MINIO_PORT") or 9000
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY") or "rootuser"
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY") or "rootpass123"

# Min.io Buckets (as required by the lab)
QUEUE_BUCKET = "queue"
OUTPUT_BUCKET = "output"

# --- Initialization and Clients ---

app = Flask(__name__)

# Initialize connections (using globals for simplicity in this microservice)
try:
    redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    # Check connectivity
    redis_client.ping()
except Exception as e:
    print(f"FATAL: Could not connect to Redis at {REDIS_HOST}:{REDIS_PORT}. Error: {e}")
    sys.exit(1)

try:
    minio_endpoint = f"{MINIO_HOST}:{MINIO_PORT}"
    minio_client = Minio(
        minio_endpoint,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # Assuming local dev, no SSL configured
    )
except Exception as e:
    print(f"FATAL: Could not initialize Min.io client. Error: {e}")
    sys.exit(1)


# --- Logging Utility (Helper functions to push messages to the 'logs' key) ---
infoKey = f"{platform.node()}.rest.info"
debugKey = f"{platform.node()}.rest.debug"

def log_message(level, message, key=None):
    """Pushes a structured log message to the Redis 'logging' list."""
    if level == "INFO":
        key = key or infoKey
        log_text = f"INFO:{key}:{message}"
    elif level == "DEBUG":
        key = key or debugKey
        log_text = f"DEBUG:{key}:{message}"
    else:
        # For WARNING/ERROR, use the infoKey structure
        key = key or infoKey
        log_text = f"{level}:{key}:{message}"
        
    print(log_text, file=sys.stdout) # Also print locally for immediate feedback
    
    try:
        # Note: log.py expects raw bytes, so encoding to UTF-8
        redis_client.lpush('logging', log_text.encode('utf-8'))
    except Exception as e:
        print(f"ERROR: Failed to push log message to Redis: {e}", file=sys.stderr)


# --- Min.io Setup Function ---

def setup_minio_buckets():
    """Ensures the necessary Min.io buckets ('queue' and 'output') exist."""
    log_message("INFO", f"Checking Min.io buckets: {QUEUE_BUCKET} and {OUTPUT_BUCKET}")
    
    try:
        # Check and create QUEUE bucket
        if not minio_client.bucket_exists(QUEUE_BUCKET):
            minio_client.make_bucket(QUEUE_BUCKET)
            log_message("INFO", f"Bucket '{QUEUE_BUCKET}' created.")

        # Check and create OUTPUT bucket
        if not minio_client.bucket_exists(OUTPUT_BUCKET):
            minio_client.make_bucket(OUTPUT_BUCKET)
            log_message("INFO", f"Bucket '{OUTPUT_BUCKET}' created.")
            
        log_message("INFO", "Min.io buckets verified successfully.")

    except Exception as e:
        log_message("ERROR", f"Failed to initialize Min.io buckets: {e}")
        # In a real app, this would be a fatal error, but for the lab, we proceed.

# Execute Min.io setup on startup
setup_minio_buckets()

# --- Flask App Routes ---

# Simple health check required by GKE load balancer [cite: I added the following route: @app.route('/', methods=['GET']) def hello(): return '<h1> Music Separation Server</h1><p> Use a valid endpoint </p>']
@app.route('/', methods=['GET'])
def health_check():
    return '<h1> Music Separation Server</h1><p> Status: Running </p>', 200

@app.route('/apiv1/separate', methods=['POST'])
def separate_audio():
    """
    Receives Base64-encoded MP3 data, stores it in Min.io, and queues a task in Redis.
    """
    try:
        # Use jsonpickle.decode for Flask requests where Content-Type is application/json
        data = jsonpickle.decode(request.data)
        
        # 1. Decode Input
        mp3_base64 = data.get('mp3')
        callback = data.get('callback', {})
        
        if not mp3_base64:
            log_message("WARNING", "Missing 'mp3' data in request.")
            return jsonify({"status": "error", "reason": "Missing mp3 data"}), 400
        
        # The client sends a UTF-8 decoded string of Base64, convert it back to bytes for decoding
        # Decoding from Base64 gives us the raw MP3 binary data.
        mp3_binary = base64.b64decode(mp3_base64)

        # 2. Hashing (creates the songhash)
        songhash = hashlib.sha256(mp3_binary).hexdigest()
        log_message("DEBUG", f"Received data, hash generated: {songhash}")

        # Prepare for Min.io upload
        mp3_stream = io.BytesIO(mp3_binary)
        mp3_length = len(mp3_binary)
        
        # 3. Upload to Min.io (Queue Bucket)
        object_name = f"{songhash}.mp3"
        
        # Check if object already exists to avoid redundant upload/processing
        try:
            minio_client.stat_object(QUEUE_BUCKET, object_name)
            log_message("INFO", f"File {object_name} already exists. Skipping upload.")
        except S3Error as err:
            # If NoSuchKey (object not found), proceed with upload
            if err.code == 'NoSuchKey':
                minio_client.put_object(
                    QUEUE_BUCKET,
                    object_name,
                    mp3_stream,
                    mp3_length,
                    content_type='audio/mpeg'
                )
                log_message("INFO", f"Successfully uploaded {object_name} to {QUEUE_BUCKET}")
            else:
                log_message("ERROR", f"Min.io storage error (Code: {err.code}): {err}")
                return jsonify({"status": "error", "reason": f"Min.io storage error: {err}"}), 500
        
        # 4. Enqueue Task in Redis
        task_payload = jsonpickle.encode({
            "songhash": songhash,
            "object_name": object_name,
            "callback": callback
        })
        
        # Use lpush to add the task to the left (head) of the list
        redis_client.lpush(QUEUE_KEY, task_payload)
        log_message("INFO", f"Task {songhash} pushed to Redis queue key '{QUEUE_KEY}'.")

        return jsonify({
            "hash": songhash,
            "reason": "Song enqueued for separation"
        }), 200

    except Exception as e:
        log_message("ERROR", f"Exception in POST /separate: {e}")
        return jsonify({"status": "error", "reason": str(e)}), 500

@app.route('/apiv1/queue', methods=['GET'])
def dump_queue():
    """Dumps the current contents of the Redis worker queue."""
    try:
        # Lrange gets items without removing them
        queued_tasks = redis_client.lrange(QUEUE_KEY, 0, -1)
        
        # Decode the task payloads to get just the songhash for display
        hashes = []
        for task in queued_tasks:
            try:
                # Task is initially bytes, decode to string, then un-jsonpickle
                payload = jsonpickle.decode(task.decode('utf-8'))
                hashes.append(payload.get('songhash', 'unknown'))
            except:
                # Handle malformed data in the queue
                hashes.append(task.decode('utf-8'))
        
        return jsonify({
            "queue": hashes,
            "status": "ok"
        }), 200

    except Exception as e:
        log_message("ERROR", f"Exception in GET /queue: {e}")
        return jsonify({"status": "error", "reason": str(e)}), 500
        
@app.route('/apiv1/track/<songhash>/<track>', methods=['GET'])
def retrieve_track(songhash, track):
    """Retrieves a separated track from the Min.io output bucket."""
    track_filename = f"{songhash}-{track}.mp3"
    
    try:
        # Use a temporary local file name
        temp_path = f"/tmp/{track_filename}" 
        
        # fget_object downloads the object directly to a local file path
        minio_client.fget_object(OUTPUT_BUCKET, track_filename, temp_path)
        log_message("INFO", f"Retrieved {track_filename} from Min.io.")
        
        # Flask's send_file handles streaming the file content to the client
        # as a binary download and handles cleanup (safe to use 'as_attachment=True')
        return send_file(
            temp_path,
            mimetype="audio/mpeg",
            as_attachment=True,
            download_name=track_filename
        )
        # Note: Flask's send_file is designed to handle file closing/cleanup
        
    except S3Error as e:
        if e.code == 'NoSuchKey':
            log_message("WARNING", f"Track {track_filename} not found in output bucket.")
            return jsonify({"status": "error", "reason": "Track not found or separation not complete."}), 404
        log_message("ERROR", f"Min.io retrieval failed: {e}")
        return jsonify({"status": "error", "reason": "Min.io error during retrieval."}), 500
    except Exception as e:
        log_message("ERROR", f"Exception in GET /track: {e}")
        return jsonify({"status": "error", "reason": str(e)}), 500
        
@app.route('/apiv1/remove/<songhash>/<track>', methods=['GET', 'DELETE'])
def remove_track(songhash, track):
    """Removes a specified separated track from the Min.io output bucket."""
    
    # Although the lab specifies GET, DELETE is more RESTful. We accept both.
    if request.method == 'GET':
        log_message("WARNING", f"Using non-RESTful GET to delete track {songhash}/{track}.")

    track_filename = f"{songhash}-{track}.mp3"
    
    try:
        minio_client.remove_object(OUTPUT_BUCKET, track_filename)
        log_message("INFO", f"Successfully removed {track_filename} from Min.io.")
        
        return jsonify({
            "status": "ok",
            "message": f"Track {track_filename} removed successfully."
        }), 200
        
    except S3Error as e:
        if e.code == 'NoSuchKey':
            log_message("WARNING", f"Attempted removal of non-existent track {track_filename}.")
            return jsonify({"status": "error", "reason": "Track not found."}), 404
        
        log_message("ERROR", f"Min.io removal failed: {e}")
        return jsonify({"status": "error", "reason": "Min.io error during removal."}), 500
    except Exception as e:
        log_message("ERROR", f"Exception in GET/DELETE /remove: {e}")
        return jsonify({"status": "error", "reason": str(e)}), 500


if __name__ == '__main__':
    # IMPORTANT: Run on 0.0.0.0:5000 for Docker deployment compatibility
    log_message("INFO", "Starting REST server...")
    app.run(host='0.0.0.0', port=5000, debug=True)

