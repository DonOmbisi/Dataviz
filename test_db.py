#!/usr/bin/env python3
"""Test MongoDB connection and diagnose issues"""

import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

print("🔍 Testing MongoDB Connection...")
print("=" * 50)

# Check if pymongo is installed
try:
    import subprocess
    import sys
    
    # Try importing pymongo
    try:
        from pymongo import MongoClient
        from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, OperationFailure
        print("✅ PyMongo is installed")
    except ImportError:
        print("⚠️ PyMongo not in current Python path, trying to install...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pymongo", "dnspython", "-q"])
        from pymongo import MongoClient
        from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, OperationFailure
        print("✅ PyMongo installed and loaded")
except Exception as e:
    print(f"❌ Failed to install/load PyMongo: {e}")
    print("   Run manually: pip install pymongo dnspython")
    exit(1)

# Get MongoDB URI
mongodb_uri = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI")

if not mongodb_uri:
    print("❌ MONGODB_URI not found in .env file")
    exit(1)

print(f"📝 MongoDB URI found: {mongodb_uri[:50]}...")

# Test connection
try:
    print("\n🔄 Connecting to MongoDB Atlas...")
    client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=10000)
    
    print("🔄 Testing connection with ping...")
    client.admin.command('ping')
    print("✅ Connection successful!")
    
    # Get database
    db_name = os.getenv("MONGODB_DB_NAME", "dataviz_pro")
    db = client[db_name]
    print(f"✅ Database '{db_name}' selected")
    
    # List collections
    print("\n📂 Collections in database:")
    collections = db.list_collection_names()
    if collections:
        for coll in collections:
            print(f"   - {coll}")
    else:
        print("   (No collections yet - will be created on first use)")
    
    print("\n✅ All tests passed! Database is ready.")
    
except ServerSelectionTimeoutError as e:
    print(f"\n❌ Server Selection Timeout")
    print(f"   Error: {e}")
    print("\n🔧 Possible fixes:")
    print("   1. Check your internet connection")
    print("   2. Whitelist your IP in MongoDB Atlas (Network Access)")
    print("   3. Verify the cluster is running (not paused)")
    print("   4. Check if port 27017 is blocked by firewall")
    
except OperationFailure as e:
    # Bad credentials and similar command failures surface as OperationFailure (e.g. code 18).
    print(f"\n❌ MongoDB command / authentication failure")
    print(f"   Error: {e}")
    print("\n🔧 Possible fixes:")
    print("   1. Check username and password in the connection string")
    print("   2. Reset password in MongoDB Atlas")
    print("   3. Ensure the database user has access to the target database")

except ConnectionFailure as e:
    print(f"\n❌ Connection Failed")
    print(f"   Error: {e}")
    print("\n🔧 Possible fixes:")
    print("   1. Check if cluster is paused (free tier pauses after inactivity)")
    print("   2. Whitelist your IP address")
    print("   3. Verify network connectivity")
    
except Exception as e:
    print(f"\n❌ Unexpected Error: {type(e).__name__}")
    print(f"   Error: {e}")

finally:
    if 'client' in locals():
        client.close()
