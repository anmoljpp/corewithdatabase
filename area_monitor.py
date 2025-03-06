import sqlite3
import json
import time
import os
import threading
from queue import Queue

# Correct paths for your system
HOME_DIR = "/config"
AREA_REGISTRY_FILE = os.path.join(HOME_DIR, "/config/.storage/core.area_registry")
DATABASE_FILE = os.path.join(HOME_DIR, "/config/home-assistant_v2.db")

# Queue for thread-safe communication
update_queue = Queue()

def initialize_database():
    """Create the area table if it doesn't exist"""
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS area (
                        id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        floor_id TEXT,
                        icon TEXT,
                        picture TEXT,
                        created_at TEXT,
                        modified_at TEXT,
                        aliases TEXT,      -- Storing as JSON string
                        labels TEXT        -- Storing as JSON string
                        )''')
        
        conn.commit()
        conn.close()
        print("Database table initialized successfully")
    except Exception as e:
        print(f"Error initializing database: {e}")
        raise

def update_database_with_new_areas(area_data):
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()

        # Get current areas in the database
        cursor.execute("SELECT id FROM area")
        existing_ids = {row[0] for row in cursor.fetchall()}

        # Get current areas in the registry
        current_ids = {area['id'] for area in area_data}

        # Delete areas that are no longer in the registry
        for area_id in existing_ids - current_ids:
            cursor.execute("DELETE FROM area WHERE id = ?", (area_id,))
            print(f"Deleted area with ID: {area_id}")

        # Insert or update areas
        for area in area_data:
            # Convert lists to JSON strings for storage
            aliases = json.dumps(area.get('aliases', []))
            labels = json.dumps(area.get('labels', []))

            cursor.execute('''INSERT OR REPLACE INTO area 
                            (id, name, floor_id, icon, picture, created_at, modified_at, aliases, labels)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                            (area['id'],
                             area['name'],
                             area.get('floor_id'),
                             area.get('icon'),
                             area.get('picture'),
                             area.get('created_at'),
                             area.get('modified_at'),
                             aliases,
                             labels))
            
            if cursor.rowcount > 0:
                print(f"Updated area: {area['name']} (ID: {area['id']})")

        conn.commit()
        conn.close()

    except Exception as e:
        print(f"Database error: {e}")

def poll_file_changes():
    """Poll the file for changes every 5 seconds"""
    last_modified_time = None
    while True:
        try:
            # Get the last modified time of the file
            current_modified_time = os.path.getmtime(AREA_REGISTRY_FILE)

            # If the file has been modified, process it
            if last_modified_time is None or current_modified_time != last_modified_time:
                print("\nDetected changes in core.area_registry...")
                with open(AREA_REGISTRY_FILE, 'r') as file:
                    data = json.load(file)
                    areas = data.get('data', {}).get('areas', [])
                
                # Put the new areas data in the queue for processing
                update_queue.put(areas)
                
                # Update the last modified time
                last_modified_time = current_modified_time

        except Exception as e:
            print(f"Error polling file: {e}")

        # Wait for 5 seconds before checking again
        time.sleep(5)

def database_updater():
    """Thread function to process updates from the queue"""
    while True:
        if not update_queue.empty():
            areas = update_queue.get()
            update_database_with_new_areas(areas)
            print("Database update completed successfully")
        time.sleep(1)

def main():
    # Initialize database first
    initialize_database()
    
    # Verify paths exist
    if not os.path.exists(AREA_REGISTRY_FILE):
        print(f"Error: File {AREA_REGISTRY_FILE} does not exist!")
        return

    # Start the file polling thread
    polling_thread = threading.Thread(target=poll_file_changes, daemon=True)
    polling_thread.start()

    # Start the database updater thread
    updater_thread = threading.Thread(target=database_updater, daemon=True)
    updater_thread.start()

    print(f"Monitoring {AREA_REGISTRY_FILE} for changes every 5 seconds...")
    try:
        # Initial sync
        with open(AREA_REGISTRY_FILE, 'r') as file:
            data = json.load(file)
            areas = data.get('data', {}).get('areas', [])
        update_database_with_new_areas(areas)
        print("Initial database sync completed")
        
        # Keep running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping the script...")

if __name__ == "__main__":
    main()