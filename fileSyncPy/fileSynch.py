import os
import argparse
import shutil
import logging
import hashlib
from time import sleep
from datetime import datetime

def parse_arguments():
    parser = argparse.ArgumentParser(description="Synchronize content of the source folder to the replica folder.")
    parser.add_argument("source", help="Path to the source folder")
    parser.add_argument("replica", help="Path to the replica folder")
    parser.add_argument("interval", type=int, help="Synchronization interval in seconds")
    parser.add_argument("log_file", help="Path to the log file")
    return parser.parse_args()

def setup_logging(log_file):
    logging.basicConfig(filename=log_file, filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

def md5(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def sync_folders(source, replica):
    for root, dirs, files in os.walk(source):
        # Replicate the directory structure
        relative_path = os.path.relpath(root, source)
        replica_path = os.path.join(replica, relative_path)
        if not os.path.exists(replica_path):
            os.makedirs(replica_path)
            logging.info(f"Directory created: {replica_path}")

        for file in files:
            source_file = os.path.join(root, file)
            replica_file = os.path.join(replica_path, file)

            # Copy or update files
            if not os.path.exists(replica_file) or md5(source_file) != md5(replica_file):
                shutil.copy2(source_file, replica_file)
                logging.info(f"File copied/updated: {replica_file}")

    # Remove extra files and directories in replica
    for root, dirs, files in os.walk(replica):
        relative_path = os.path.relpath(root, replica)
        source_path = os.path.join(source, relative_path)

        if not os.path.exists(source_path):
            shutil.rmtree(root)
            logging.info(f"Directory removed: {root}")
            continue

        for file in files:
            replica_file = os.path.join(root, file)
            source_file = os.path.join(source_path, file)

            if not os.path.exists(source_file):
                os.remove(replica_file)
                logging.info(f"File removed: {replica_file}")

def main():
    args = parse_arguments()
    setup_logging(args.log_file)

    logging.info("Starting synchronization...")
    while True:
        sync_folders(args.source, args.replica)
        logging.info("Synchronization completed. Waiting for the next interval...")
        sleep(args.interval)

if __name__ == "__main__":
    main()