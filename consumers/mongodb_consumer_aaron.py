""" mongodb_consumer_aaron.py 

Has the following functions:
- init_db(config): Initialize the MongoDB database and create the 'category' based tables if it doesn't exist.
- insert_message(message, config): Insert a single processed message into the MongoDB database.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

"""

#####################################
# Import Modules
#####################################

# import from standard library
import os
import pathlib
from pymongo import MongoClient

# import from local modules
import utils.utils_config as config
from utils.utils_logger import logger

#####################################
# Define Function to Initialize SQLite Database
#####################################


def init_db(config: dict):
    """
    Initialize MongoDB connection and test connectivity.

    Args:
        config (dict): Dictionary containing MongoDB config, e.g., URI and DB name.
    """
    try:
        client = MongoClient(config["MONGO_URI"])
        db = client[config["DB_NAME"]]
        logger.info(f"Connected to MongoDB at {config['MONGO_URI']}, DB: {config['DB_NAME']}")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize MongoDB: {e}")



#####################################
# Define Function to Insert a Processed Message into the Database
#####################################


def insert_message(message: dict, config: dict) -> None:
    """
    Insert a single processed message into a MongoDB collection based on category.

    Args:
        message (dict): The message to insert.
        config (dict): MongoDB config with URI and DB name.
    """
    logger.info("Calling MongoDB insert_message() with:")
    logger.info(f"{message=}")
    logger.info(f"{config=}")

    try:
        client = MongoClient(config["MONGO_URI"])
        db = client[config["DB_NAME"]]

        # Use category as collection name (sanitized)
        category = message["category"].lower().replace(" ", "_")
        collection = db[f"messages_{category}"]

        result = collection.insert_one(message)
        logger.info(f"Inserted message with _id: {result.inserted_id} into collection: messages_{category}")
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message into MongoDB: {e}")



#####################################
# Define Function to Delete a Message from the Database
#####################################





#####################################
# Define main() function for testing
#####################################
def main():
    logger.info("Starting MongoDB db testing.")

    # Load config for MongoDB
    db_config = {
        "MONGO_URI": "mongodb://localhost:27017/",
        "DB_NAME": "buzz_test_db"
    }

    init_db(db_config)

    test_message = {
        "message": "I just shared a meme! It was amazing.",
        "author": "Charlie",
        "timestamp": "2025-01-29 14:35:20",
        "category": "humor",
        "sentiment": 0.87,
        "keyword_mentioned": "meme",
        "message_length": 42,
    }

    insert_message(test_message, db_config)

    # Retrieve and delete test message
   # try:
    #    client = MongoClient(db_config["MONGO_URI"])
   #     db = client[db_config["DB_NAME"]]
    #    collection = db["messages_humor"]
     #   found = collection.find_one({"message": test_message["message"], "author": test_message["author"]})
      #  if found:
       #     delete_message(str(found["_id"]), "humor", db_config)
        #else:
         #   logger.warning("Test message not found; nothing to delete.")
  #  except Exception as e:
   #     logger.error(f"ERROR: Failed to retrieve or delete test message: {e}")

    logger.info("Finished MongoDB testing.")


# #####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
