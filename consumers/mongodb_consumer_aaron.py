# Standard Library Imports
import os
import pathlib
from pymongo import MongoClient
from bson import ObjectId

# Local Module Imports
import utils.utils_config as config
from utils.utils_logger import logger
from utils.utils_config import get_mongodb_uri, get_mongodb_db, get_mongodb_collection


#####################################
# Initialize MongoDB
#####################################
def init_db():
    """
    Initialize MongoDB connection and test connectivity using environment config.
    """
    try:
        uri = get_mongodb_uri()
        db_name = get_mongodb_db()

        client = MongoClient(uri)
        db = client[db_name]

        logger.info(f"Connected to MongoDB at {uri}, DB: {db_name}")
        return db  # Optional: return db if needed elsewhere
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize MongoDB: {e}")
        raise


#####################################
# Insert a Message into MongoDB
#####################################
def insert_message(message: dict) -> None:
    """
    Insert a single processed message into a MongoDB collection based on category.

    Args:
        message (dict): The message to insert.
    """
    logger.info("Calling MongoDB insert_message() with:")
    logger.info(f"{message=}")

    try:
        uri = get_mongodb_uri()
        db_name = get_mongodb_db()

        client = MongoClient(uri)
        db = client[db_name]

        category = message["category"].lower().replace(" ", "_")
        collection = db[f"messages_{category}"]

        result = collection.insert_one(message)
        logger.info(f"Inserted message with _id: {result.inserted_id} into collection: messages_{category}")
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message into MongoDB: {e}")


#####################################
# Delete a Message from MongoDB
#####################################
def delete_message(message_id: str, category: str) -> None:
    """
    Delete a message from the MongoDB collection by its message ID.

    Args:
        message_id (str): The ID of the message to delete.
        category (str): The category of the message (used to determine collection).
    """
    try:
        uri = get_mongodb_uri()
        db_name = get_mongodb_db()

        client = MongoClient(uri)
        db = client[db_name]

        sanitized_category = category.lower().replace(" ", "_")
        collection = db[f"messages_{sanitized_category}"]

        result = collection.delete_one({"_id": ObjectId(message_id)})

        if result.deleted_count == 1:
            logger.info(f"Deleted message with _id {message_id} from collection messages_{sanitized_category}.")
        else:
            logger.warning(f"No message found with _id {message_id} in collection messages_{sanitized_category}.")
    except Exception as e:
        logger.error(f"ERROR: Failed to delete message from MongoDB: {e}")


#####################################
# Main Function for Testing
#####################################
def main():
    logger.info("Starting MongoDB db testing.")

    # Initialize DB (check connection)
    init_db()

    # Sample test message
    test_message = {
        "message": "I just shared a meme! It was amazing.",
        "author": "Charlie",
        "timestamp": "2025-01-29 14:35:20",
        "category": "humor",
        "sentiment": 0.87,
        "keyword_mentioned": "meme",
        "message_length": 42,
    }

    # Insert test message
    insert_message(test_message)

    # Retrieve and delete the test message
    try:
        uri = get_mongodb_uri()
        db_name = get_mongodb_db()
        client = MongoClient(uri)
        db = client[db_name]
        collection = db["messages_humor"]

        found = collection.find_one({
            "message": test_message["message"],
            "author": test_message["author"]
        })

        if found:
            delete_message(str(found["_id"]), "humor")
        else:
            logger.warning("Test message not found; nothing to delete.")
    except Exception as e:
        logger.error(f"ERROR: Failed to retrieve or delete test message: {e}")

    logger.info("Finished MongoDB testing.")


#####################################
# Run if Script is Executed Directly
#####################################
if __name__ == "__main__":
    main()
