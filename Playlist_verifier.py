import os
import time
import configparser
import argparse
import mysql.connector
import shutil
from datetime import datetime

# Import the setup_logger function
from logging_setup import setup_logger

# Initialize logger
logger = setup_logger("BPCM_Agent", "general.log", "error.log")

class DatabaseConnection:
    def __init__(self, config):
        """Initialize database connection using configuration from the INI file."""
        self.host = config.get('database', 'host')
        self.port = config.getint('database', 'port', fallback=3306)
        self.user = config.get('database', 'user')
        self.password = config.get('database', 'password')
        self.database = config.get('database', 'database')
        self.conn = self.connect_to_mysql()

    def connect_to_mysql(self):
        """Connect to MySQL database using credentials from the INI file."""
        try:
            conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            return conn
        except mysql.connector.Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            return None

    def execute_query(self, query, params=None):
        """Execute a SQL query."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, params or ())
            self.conn.commit()
            return cursor
        except mysql.connector.Error as e:
            logger.error(f"Error executing query: {e}")
            return None

    def fetch_one(self, query, params=None):
        """Fetch a single row from the database."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, params or ())
            return cursor.fetchone()
        except mysql.connector.Error as e:
            logger.error(f"Error fetching data: {e}")
            return None

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()


class FileProcessor:
    def __init__(self, db_connection):
        self.db_connection = db_connection
        self.config = self.fetch_playlist_configuration()
        self.watch_dirs = self.parse_watch_directories(self.config['playlistwatchfolder'])
        self.processed_files = set()  # Track processed files to avoid reprocessing

    def fetch_playlist_configuration(self):
        """Fetch playlist configuration from the database."""
        query = """
        SELECT channeluid, playlistwatchfolder, playlistinputlocation, playlistoutputlocation, 
            playlistnameprefix, playlistdateformat, playlistextension, createdby, updatedby
        FROM playlist_configuration
        LIMIT 1
        """
        result = self.db_connection.fetch_one(query)
        if result:
            # Log the raw value of playlistwatchfolder for debugging
            logger.info(f"playlistwatchfolder value from database: {result[1]}")
            
            return {
                'channeluid': result[0],
                'playlistwatchfolder': result[1],
                'playlistinputlocation': result[2],
                'playlistoutputlocation': result[3],
                'playlistnameprefix': result[4],
                'playlistdateformat': result[5],
                'playlistextension': result[6],
                'createdby': result[7],
                'updatedby': result[8]
            }
        else:
            raise ValueError("No playlist configuration found in the database.")

    def parse_watch_directories(self, watch_folder_config):
        """Parse the watch folder configuration into a list of directories."""
        if not watch_folder_config:
            raise ValueError("No watch directories configured.")
        
        # Log the raw watch_folder_config for debugging
        logger.info(f"Raw watch_folder_config: {watch_folder_config}")
        
        # Split by comma and strip extra spaces
        watch_dirs = [dir_path.strip() for dir_path in watch_folder_config.split(",") if dir_path.strip()]
        
        # Log the parsed watch directories for debugging
        logger.info(f"Parsed watch directories: {watch_dirs}")
        
        # Ensure all directories exist
        valid_watch_dirs = []
        for dir_path in watch_dirs:
            if os.path.exists(dir_path):
                valid_watch_dirs.append(dir_path)
            else:
                logger.warning(f"Watch directory does not exist: {dir_path}")
        
        if not valid_watch_dirs:
            raise ValueError("No valid watch directories found.")
        
        return valid_watch_dirs

    def scan_and_process_files(self):
        """Scan watch directories and process new files."""
        for watch_dir in self.watch_dirs:
            if not os.path.exists(watch_dir):
                logger.warning(f"Watch directory does not exist: {watch_dir}")
                continue

            for filename in os.listdir(watch_dir):
                file_path = os.path.join(watch_dir, filename)
                if os.path.isfile(file_path) and file_path not in self.processed_files:
                    self.process_file(file_path)
                    self.processed_files.add(file_path)

    def process_file(self, file_path):
        """Process the file."""
        filename = os.path.basename(file_path)
        prefix = filename[0:3]
        extension = os.path.splitext(filename)[1].lower()
        date_string = filename.split('-')[0][4:]
        fileversion = filename.split('-')[1].split('.')[0]
        try:
            playlist_date = datetime.strptime(date_string, "%d%m%Y").strftime("%Y-%m-%d")
        except ValueError:
            self.handle_validation_failure(file_path, filename, fileversion, "Invalid date format")
            return

        # Validate file
        if prefix != self.config['playlistnameprefix']:
            self.handle_validation_failure(file_path, filename, fileversion, "Invalid prefix")
            return
        if extension != self.config['playlistextension']:
            self.handle_validation_failure(file_path, filename, fileversion, "Invalid file extension")
            return

        # Move file to input directory
        input_file_path = os.path.join(self.config['playlistinputlocation'], filename)
        try:
            shutil.move(file_path, input_file_path)
            logger.info(f"File {filename} moved successfully to {input_file_path}.")
        except Exception as e:
            logger.error(f"Error moving file: {e}")
            return

        # Insert metadata into playlist_process table
        self.insert_into_playlist_process(filename, playlist_date, fileversion)

    def handle_validation_failure(self, file_path, filename, fileversion, reason):
        """Handle validation failure by updating the database."""
        query = """
        INSERT INTO playlist_process (channeluid, playlistfilename, playlistinputpath, playlistoutputpath, 
                                     playlistfileversion, status, remarks, createdby, updatedby)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        self.db_connection.execute_query(query, (
            self.config['channeluid'], filename, self.config['playlistinputlocation'],
            self.config['playlistoutputlocation'], fileversion, 99, reason,
            self.config['createdby'], self.config['updatedby']
        ))
        logger.warning(f"Validation failed for {filename}: {reason}")

    def insert_into_playlist_process(self, filename, playlist_date, fileversion):
        """Insert metadata into playlist_process table."""
        query = """
        INSERT INTO playlist_process (channeluid, playlistfilename, playlistfileversion, playlistinputpath, 
                                     playlistoutputpath, playlistdate, status, createdby, updatedby)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        self.db_connection.execute_query(query, (
            self.config['channeluid'], filename, fileversion, self.config['playlistinputlocation'],
            self.config['playlistoutputlocation'], playlist_date, 0,
            self.config['createdby'], self.config['updatedby']
        ))
        logger.info(f"File {filename} processed successfully and added to playlist_process table.")


def load_config(customer_name):
    """Load database configuration from the INI file."""
    customer_name = customer_name.replace(" ", "")
    config_path = f"/etc/mdmgr/{customer_name}.ini"
    if not os.path.exists(config_path):
        logger.error(f"Configuration file not found: {config_path}")
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    config = configparser.ConfigParser()
    config.read(config_path)
    logger.info(f"Configuration file loaded: {config_path}")
    return config


def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Watch for new files and process them.")
    parser.add_argument("-c", "--customer", required=True, help="Customer name (INI file name without extension)")
    args = parser.parse_args()

    # Load database configuration
    config = load_config(args.customer)
    if not config:
        logger.error("Config File Not Found")
        return

    # Initialize database connection
    db_connection = DatabaseConnection(config)

    # Initialize file processor
    file_processor = FileProcessor(db_connection)

    # Polling loop
    try:
        while True:
            file_processor.scan_and_process_files()
            time.sleep(10)  # Poll every 10 seconds
    except KeyboardInterrupt:
        logger.info("Stopping file processor...")
    finally:
        db_connection.close()


if __name__ == "__main__":
    main()