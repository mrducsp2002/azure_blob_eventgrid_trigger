import dotenv
import os
from src.database import get_mongo_db
from pprint import pprint  # Added for better printing

# 1. Load env before anything else
dotenv.load_dotenv()
print(f"Connecting to DB: {os.environ.get('DB_NAME')}")
# 2. Connection
db = get_mongo_db()
try:
    # This forces a network call to check connection
    db.command('ping')
    print("✅ Successfully connected to MongoDB!")
except Exception as e:
    print(f"❌ Connection failed: {e}")
collection = db["iviva-student-assignments"]

# 3. Use a cursor with a limit to be safe during dev
cursor = collection.find({}).limit(10)

print(f"--- Showing first 10 docs from {collection.name} ---")
for doc in cursor:
    pprint(doc)  # pprint makes nested JSON/Dicts readable
