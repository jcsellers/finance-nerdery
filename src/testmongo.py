from datetime import datetime

from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["finance_nerdery"]
collection = db["raw_financial_data"]

# Insert a test document
test_document = {
    "ticker": "UPRO",
    "source": "yfinance",
    "data": {"2025-01-01": {"open": 100, "close": 110}},
    "metadata": {"retrieval_timestamp": datetime.utcnow(), "schema_version": "1.0"},
}
collection.insert_one(test_document)
print("Test document inserted!")

# Retrieve and print the document
result = collection.find_one({"ticker": "UPRO"})
print("Retrieved document:", result)
