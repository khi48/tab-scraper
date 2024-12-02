"""
To use this code, you'll need to:

1. Install the required package:
```bash
pip install pymongo
```

2. Fill in the following details in the `MongoDBHandler` class:
   - `connection_string`: Your MongoDB connection string (e.g., "mongodb://username:password@localhost:27017")
   - `database_name`: The name of your database
   - `collection_name`: The name of your collection

The code includes:

1. A `MongoDBHandler` class that handles:
   - Connecting to MongoDB
   - Posting JSON data
   - Updating time-sensitive data
   - Closing connections

2. Error handling for common MongoDB operations

3. Automatic timestamp addition for both insertions and updates

4. Type hints and documentation for better code readability

5. Example usage in the `main()` function

The code follows the black formatting style and includes proper error handling and connection management. When updating time-sensitive data, it automatically adds a timestamp to track when the update occurred.

To use this in production:

1. Replace the empty connection details with your actual MongoDB credentials
2. Modify the data structure according to your needs
3. Implement any additional error handling specific to your use case
4. Consider adding indexes for better query performance
5. Add any necessary authentication or security measures

Remember to never commit sensitive information like connection strings directly in the code. Consider using environment variables or configuration files for such sensitive data.
"""
import os
from datetime import datetime, timezone
from typing import Dict, Any
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure


class MongoDBHandler:
    def __init__(self, database_name, collection_name=None):
        # Load environment variables from .env file
        load_dotenv()

        self.database_name = database_name
        self.collection_name = collection_name
        # Fill in your MongoDB connection details
        self.client = None
        self.db = None
        self.collection = None
        

    def _get_connection_string(self) -> str:
        """
        Constructs MongoDB connection string from environment variables
        """
        host = os.getenv('MONGODB_HOST', 'localhost')
        port = os.getenv('MONGODB_PORT', '27017')
        username = os.getenv('MONGODB_USERNAME')
        password = os.getenv('MONGODB_PASSWORD')
        auth_source = os.getenv('MONGODB_AUTH_SOURCE', 'admin')

        if username and password:
            return f"mongodb://{username}:{password}@{host}:{port}/?authSource={auth_source}"
        return f"mongodb://{host}:{port}/"
    
    def connect_to_collection(self):
        if not self.collection_name:
            return None
        return self.db[self.collection_name]
    
    def connect(self) -> bool:
        """Establish connection to MongoDB database."""
        try:
            connection_string = self._get_connection_string()
            self.client = MongoClient(connection_string)
            self.db = self.client[self.database_name]
            self.collection = self.connect_to_collection()

            # Test connection
            self.client.server_info()
            return True
        
        except ConnectionFailure as e:
            print(f"Failed to connect to MongoDB: {e}")
            return False

    def close_connection(self):
        """Close the MongoDB connection."""
        if self.client:
            self.client.close()

    def create_collection(self, collection_name=None):
        """
        Create a new collection in the existing MongoDB database.
        
        Args:
        collection_name (str, optional): Name of the collection to create. 
                                        If None, generates a name with current date.
        
        Returns:
        str: The name of the created collection
        """
        # If no collection name is provided, generate one with current date
        if collection_name is None:
            current_date = datetime.now().strftime('_%Y%m%d')
            collection_name = current_date
        
        # Create the collection
        new_collection = self.db[collection_name]
        
        # Insert a placeholder document to ensure collection is created immediately
        # This is optional but ensures the collection exists even if empty
        new_collection.insert_one({"created_at": datetime.now()})
        
        return collection_name
    

    def check_collection_in_db(self, input_collection_name) -> bool:
        """
        Checks a collection is found in a db
        
        Returns:
        dict: A dictionary with collection names as keys and boolean 
            validation results as values
        """
        
        # Get list of all collection names in the database
        collection_names = self.db.list_collection_names()
        
        # Validate each collection name
        for collection_name in collection_names:
            # Check if collection name matches current date format
            if collection_name == input_collection_name:
                return True
        
        return False

    def post_data(self, data: Dict[str, Any]) -> bool:
        """
        Post JSON data to MongoDB.
        
        Args:
            data: Dictionary containing the data to be posted
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:            
            # Insert the data into MongoDB
            result = self.collection.insert_one(data)
            print(f"Data inserted with ID: {result.inserted_id}")
            return True
        except OperationFailure as e:
            print(f"Failed to insert data: {e}")
            return False
        
        
    def append_to_existing_document(
        self, document_id: str, updated_data: Dict[str, Any]
    ) -> bool:
        """
        Appends item to existing document in MongoDB.
        
        Args:
            document_id: ID of the document to update
            updated_data: Dictionary containing the updated data
            
        Returns:
            bool: True if successful, False otherwise

        https://stackoverflow.com/questions/31956696/how-to-append-existing-array-in-existing-collection-in-mongodb-using-java-with-n
        """
        try:
            # Update the document
            result = self.collection.update_one(
                {"_id": document_id}, {"$push": updated_data}
            )

            if result.modified_count > 0:
                print(f"Document {document_id} updated successfully")
                return True
            else:
                print(f"No document found with ID: {document_id}")
                return False

        except OperationFailure as e:
            print(f"Failed to update data: {e}")
            return False
        

    def update_document(
        self, document_id: str, updated_data: Dict[str, Any]
    ) -> bool:
        """
        Update document in MongoDB.
        
        Args:
            document_id: ID of the document to update
            updated_data: Dictionary containing the updated data
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Update the document
            result = self.collection.update_one(
                {"_id": document_id}, {"$set": updated_data}
            )

            if result.modified_count > 0:
                print(f"Document {document_id} updated successfully")
                return True
            else:
                print(f"No document found with ID: {document_id}")
                return False

        except OperationFailure as e:
            print(f"Failed to update data: {e}")
            return False


# Example usage
def main():
    # Initialize the MongoDB handler
    mongo_handler = MongoDBHandler()

    # Connect to the database
    if not mongo_handler.connect():
        return

    try:
        # Example JSON data
        sample_data = {
            "user_id": "12345",
            "name": "John Doe",
            "email": "john@example.com",
            "status": "active",
        }

        # Post data to MongoDB
        success = mongo_handler.post_data(sample_data)
        if success:
            print("Data posted successfully")

        # Example of updating time-sensitive data
        updated_data = {
            "status": "inactive",
            "last_login": datetime.now(datetime.timezone.utc),
        }

        # Replace with actual document ID
        document_id = "your_document_id"
        success = mongo_handler.update_time_sensitive_data(document_id, updated_data)
        if success:
            print("Data updated successfully")

    finally:
        # Close the connection
        mongo_handler.close_connection()


if __name__ == "__main__":
    main()
