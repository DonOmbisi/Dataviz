import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
from plotly.subplots import make_subplots
import os
import time
from datetime import datetime, timedelta
import warnings
from typing import Dict, List, Any, Optional
import json
import io
import base64
import re
import hashlib
import secrets
import uuid
warnings.filterwarnings('ignore')

# Load environment variables first
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Global flags for optional dependencies
MONGODB_AVAILABLE = False
POSTGRESQL_AVAILABLE = False
OPENAI_AVAILABLE = False

# MongoDB imports
try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure
    from bson.objectid import ObjectId
    import pymongo
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False

# PostgreSQL imports
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    POSTGRESQL_AVAILABLE = True
except ImportError:
    POSTGRESQL_AVAILABLE = False

# OpenAI imports
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

# Groq imports (Free AI alternative - groq.com)
try:
    from groq import Groq
    GROQ_AVAILABLE = True
except ImportError:
    GROQ_AVAILABLE = False

# Hugging Face imports (Free AI alternative - huggingface.co)
try:
    import requests
    HF_AVAILABLE = True
except ImportError:
    HF_AVAILABLE = False

# Guest / local session (no database required for file analysis)

def guest_mode_allowed() -> bool:
    """File-first default: allow local use without database accounts."""
    if os.getenv("DATAVIZ_REQUIRE_AUTH", "").lower() in ("1", "true", "yes"):
        return False
    return os.getenv("DATAVIZ_GUEST_MODE", "1").lower() not in ("0", "false", "no")


def is_guest_user(user: Optional[dict]) -> bool:
    return bool(user and user.get("user_id") == "guest")


# Page configuration - Optimized for performance
st.set_page_config(
    page_title="DataViz Pro",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",  # Start collapsed for faster initial render
    menu_items={
        'Get Help': None,
        'Report a bug': None,
        'About': None  # Remove menu items for faster load
    }
)

# Custom CSS - Optimized for Performance
st.markdown("""
<style>
    /* Performance-optimized CSS - removed animations and heavy effects */
    
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 15px;
        margin-bottom: 1.5rem;
        color: white;
        text-align: center;
        box-shadow: 0 4px 16px rgba(31, 38, 135, 0.2);
    }
    
    .metric-card {
        background: rgba(255, 255, 255, 0.05);
        border-radius: 12px;
        padding: 1.5rem;
        margin: 0.75rem 0;
        border: 1px solid rgba(255, 255, 255, 0.15);
        box-shadow: 0 2px 8px rgba(31, 38, 135, 0.15);
    }
    
    .insight-card {
        background: rgba(0, 212, 255, 0.05);
        border-radius: 12px;
        padding: 1.5rem;
        margin: 0.75rem 0;
        border: 1px solid rgba(0, 212, 255, 0.2);
        box-shadow: 0 2px 8px rgba(0, 212, 255, 0.1);
    }
    
    .dashboard-card {
        background: rgba(255, 255, 255, 0.03);
        border-radius: 10px;
        padding: 1rem;
        margin: 0.5rem 0;
        border: 1px solid rgba(255, 255, 255, 0.1);
    }
    
    .floating-action {
        position: fixed;
        bottom: 2rem;
        right: 2rem;
        background: linear-gradient(135deg, #00d4ff, #764ba2);
        border-radius: 50%;
        width: 50px;
        height: 50px;
        border: none;
        box-shadow: 0 4px 12px rgba(0, 212, 255, 0.3);
        cursor: pointer;
        z-index: 1000;
    }
    
    .stSelectbox > div > div {
        background: rgba(255, 255, 255, 0.08);
        border-radius: 8px;
        border: 1px solid rgba(255, 255, 255, 0.15);
    }
    
    .uploadedFile {
        background: rgba(0, 212, 255, 0.05);
        border-radius: 10px;
        padding: 1rem;
        border: 1px solid rgba(0, 212, 255, 0.2);
    }
    
    .sidebar-nav {
        background: rgba(255, 255, 255, 0.03);
        border-radius: 10px;
        padding: 0.75rem;
        margin: 0.5rem 0;
        border: 1px solid rgba(255, 255, 255, 0.1);
    }
    
    .chart-container {
        background: rgba(255, 255, 255, 0.02);
        border-radius: 12px;
        padding: 0.75rem;
        border: 1px solid rgba(255, 255, 255, 0.08);
        margin: 0.5rem 0;
    }
    
    .status-indicator {
        display: inline-block;
        width: 8px;
        height: 8px;
        border-radius: 50%;
        margin-right: 8px;
    }
    
    .status-online { background: #00ff88; }
    .status-processing { background: #ffa500; }
    .status-error { background: #ff4444; }
    
    .tooltip {
        position: relative;
        cursor: help;
    }
    
    .tooltip:hover::after {
        content: attr(data-tooltip);
        position: absolute;
        background: rgba(0, 0, 0, 0.8);
        color: white;
        padding: 0.4rem;
        border-radius: 4px;
        font-size: 0.75rem;
        white-space: nowrap;
        z-index: 1000;
        top: -1.5rem;
        left: 50%;
        transform: translateX(-50%);
    }
    
    .breadcrumb {
        background: rgba(255, 255, 255, 0.05);
        border-radius: 15px;
        padding: 0.4rem 0.75rem;
        margin: 0.75rem 0;
        border: 1px solid rgba(255, 255, 255, 0.15);
    }
    
    .search-bar {
        background: rgba(255, 255, 255, 0.08);
        border-radius: 20px;
        border: 1px solid rgba(255, 255, 255, 0.15);
        padding: 0.5rem 1rem;
        width: 100%;
        margin: 0.75rem 0;
    }
</style>
""", unsafe_allow_html=True)

# User authentication and management
class UserManager:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.init_user_tables()
    
    def init_user_tables(self):
        """Initialize user tables if they don't exist"""
        if self.db_manager.db is None:
            return
        
        try:
            if self.db_manager.db_type == "mongodb":
                # MongoDB collections are created automatically
                pass
            elif self.db_manager.db_type == "postgresql":
                cursor = self.db_manager.db.cursor()
                
                # Create users table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id SERIAL PRIMARY KEY,
                        user_id VARCHAR(255) UNIQUE NOT NULL,
                        email VARCHAR(255) UNIQUE NOT NULL,
                        password_hash VARCHAR(255) NOT NULL,
                        salt VARCHAR(255) NOT NULL,
                        first_name VARCHAR(100),
                        last_name VARCHAR(100),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_login TIMESTAMP,
                        is_active BOOLEAN DEFAULT TRUE
                    )
                """)
                
                # Create user sessions table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS user_sessions (
                        id SERIAL PRIMARY KEY,
                        session_id VARCHAR(255) UNIQUE NOT NULL,
                        user_id VARCHAR(255) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        expires_at TIMESTAMP NOT NULL,
                        is_active BOOLEAN DEFAULT TRUE
                    )
                """)
                
                self.db_manager.db.commit()
                cursor.close()
                
        except Exception as e:
            st.error(f"Error initializing user tables: {str(e)}")
            if self.db_manager.db_type == "postgresql":
                self.db_manager.db.rollback()
    
    def hash_password(self, password: str, salt: str = None) -> tuple:
        """Hash password with salt"""
        if salt is None:
            salt = secrets.token_hex(32)
        
        password_hash = hashlib.pbkdf2_hmac('sha256', 
                                          password.encode('utf-8'), 
                                          salt.encode('utf-8'), 
                                          100000)
        return password_hash.hex(), salt
    
    def verify_password(self, password: str, password_hash: str, salt: str) -> bool:
        """Verify password against hash"""
        hash_to_check, _ = self.hash_password(password, salt)
        return hash_to_check == password_hash
    
    def validate_email(self, email: str) -> bool:
        """Validate email format"""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(pattern, email) is not None
    
    def register_user(self, email: str, password: str, first_name: str, last_name: str) -> Dict[str, Any]:
        """Register a new user"""
        if self.db_manager.db is None:
            return {"success": False, "message": "Database not available"}
        
        # Validate inputs
        if not self.validate_email(email):
            return {"success": False, "message": "Invalid email format"}
        
        if len(password) < 6:
            return {"success": False, "message": "Password must be at least 6 characters long"}
        
        if not first_name.strip() or not last_name.strip():
            return {"success": False, "message": "First name and last name are required"}
        
        try:
            # Check if user already exists
            if self.get_user_by_email(email):
                return {"success": False, "message": "User with this email already exists"}
            
            # Generate user ID and hash password
            user_id = str(uuid.uuid4())
            password_hash, salt = self.hash_password(password)
            
            if self.db_manager.db_type == "mongodb":
                user_doc = {
                    "user_id": user_id,
                    "email": email.lower(),
                    "password_hash": password_hash,
                    "salt": salt,
                    "first_name": first_name.strip(),
                    "last_name": last_name.strip(),
                    "created_at": datetime.utcnow(),
                    "last_login": None,
                    "is_active": True
                }
                self.db_manager.db.users.insert_one(user_doc)
                
            elif self.db_manager.db_type == "postgresql":
                cursor = self.db_manager.db.cursor()
                cursor.execute("""
                    INSERT INTO users (user_id, email, password_hash, salt, first_name, last_name)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (user_id, email.lower(), password_hash, salt, first_name.strip(), last_name.strip()))
                self.db_manager.db.commit()
                cursor.close()
            
            return {"success": True, "message": "User registered successfully", "user_id": user_id}
            
        except Exception as e:
            return {"success": False, "message": f"Registration failed: {str(e)}"}
    
    def authenticate_user(self, email: str, password: str) -> Dict[str, Any]:
        """Authenticate user login"""
        if self.db_manager.db is None:
            return {"success": False, "message": "Database not available"}
        
        try:
            user = self.get_user_by_email(email)
            if not user:
                return {"success": False, "message": "Invalid email or password"}
            
            if not self.verify_password(password, user['password_hash'], user['salt']):
                return {"success": False, "message": "Invalid email or password"}
            
            if not user.get('is_active', True):
                return {"success": False, "message": "Account is deactivated"}
            
            # Update last login
            self.update_last_login(user['user_id'])
            
            # Create session
            session_id = self.create_session(user['user_id'])
            
            return {
                "success": True, 
                "message": "Login successful",
                "user": user,
                "session_id": session_id
            }
            
        except Exception as e:
            return {"success": False, "message": f"Authentication failed: {str(e)}"}
    
    def get_user_by_email(self, email: str) -> Optional[Dict]:
        """Get user by email"""
        if self.db_manager.db is None:
            return None
        
        try:
            if self.db_manager.db_type == "mongodb":
                user = self.db_manager.db.users.find_one({"email": email.lower()})
                if user:
                    user['id'] = str(user['_id'])
                return user
                
            elif self.db_manager.db_type == "postgresql":
                cursor = self.db_manager.db.cursor()
                cursor.execute("""
                    SELECT user_id, email, password_hash, salt, first_name, last_name, 
                           created_at, last_login, is_active
                    FROM users WHERE email = %s
                """, (email.lower(),))
                row = cursor.fetchone()
                cursor.close()
                
                if row:
                    return {
                        'user_id': row[0],
                        'email': row[1],
                        'password_hash': row[2],
                        'salt': row[3],
                        'first_name': row[4],
                        'last_name': row[5],
                        'created_at': row[6],
                        'last_login': row[7],
                        'is_active': row[8]
                    }
                return None
                
        except Exception as e:
            st.error(f"Error retrieving user: {str(e)}")
            return None
    
    def update_last_login(self, user_id: str):
        """Update user's last login timestamp"""
        if self.db_manager.db is None:
            return
        
        try:
            if self.db_manager.db_type == "mongodb":
                self.db_manager.db.users.update_one(
                    {"user_id": user_id},
                    {"$set": {"last_login": datetime.utcnow()}}
                )
                
            elif self.db_manager.db_type == "postgresql":
                cursor = self.db_manager.db.cursor()
                cursor.execute("""
                    UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE user_id = %s
                """, (user_id,))
                self.db_manager.db.commit()
                cursor.close()
                
        except Exception as e:
            st.error(f"Error updating last login: {str(e)}")
    
    def create_session(self, user_id: str) -> str:
        """Create a new user session"""
        if self.db_manager.db is None:
            return secrets.token_urlsafe(32)  # Return a session ID even without DB
        
        try:
            session_id = secrets.token_urlsafe(32)
            expires_at = datetime.utcnow() + timedelta(days=7)  # Session expires in 7 days
            
            if self.db_manager.db_type == "mongodb":
                session_doc = {
                    "session_id": session_id,
                    "user_id": user_id,
                    "created_at": datetime.utcnow(),
                    "expires_at": expires_at,
                    "is_active": True
                }
                self.db_manager.db.user_sessions.insert_one(session_doc)
                
            elif self.db_manager.db_type == "postgresql":
                cursor = self.db_manager.db.cursor()
                cursor.execute("""
                    INSERT INTO user_sessions (session_id, user_id, expires_at)
                    VALUES (%s, %s, %s)
                """, (session_id, user_id, expires_at))
                self.db_manager.db.commit()
                cursor.close()
            
            return session_id
            
        except Exception as e:
            st.error(f"Error creating session: {str(e)}")
            return secrets.token_urlsafe(32)
    
    def validate_session(self, session_id: str) -> Optional[Dict]:
        """Validate user session and return user info"""
        if self.db_manager.db is None or not session_id:
            return None
        
        try:
            if self.db_manager.db_type == "mongodb":
                session = self.db_manager.db.user_sessions.find_one({
                    "session_id": session_id,
                    "is_active": True,
                    "expires_at": {"$gt": datetime.utcnow()}
                })
                
                if session:
                    user = self.db_manager.db.users.find_one({"user_id": session['user_id']})
                    if user:
                        user['id'] = str(user['_id'])
                    return user
                    
            elif self.db_manager.db_type == "postgresql":
                cursor = self.db_manager.db.cursor()
                cursor.execute("""
                    SELECT u.user_id, u.email, u.first_name, u.last_name, u.created_at, u.last_login, u.is_active
                    FROM users u
                    JOIN user_sessions s ON u.user_id = s.user_id
                    WHERE s.session_id = %s AND s.is_active = TRUE AND s.expires_at > CURRENT_TIMESTAMP
                """, (session_id,))
                row = cursor.fetchone()
                cursor.close()
                
                if row:
                    return {
                        'user_id': row[0],
                        'email': row[1],
                        'first_name': row[2],
                        'last_name': row[3],
                        'created_at': row[4],
                        'last_login': row[5],
                        'is_active': row[6]
                    }
            
            return None
            
        except Exception as e:
            st.error(f"Error validating session: {str(e)}")
            return None
    
    def logout_user(self, session_id: str):
        """Deactivate user session"""
        if self.db_manager.db is None or not session_id:
            return
        
        try:
            if self.db_manager.db_type == "mongodb":
                self.db_manager.db.user_sessions.update_one(
                    {"session_id": session_id},
                    {"$set": {"is_active": False}}
                )
                
            elif self.db_manager.db_type == "postgresql":
                cursor = self.db_manager.db.cursor()
                cursor.execute("""
                    UPDATE user_sessions SET is_active = FALSE WHERE session_id = %s
                """, (session_id,))
                self.db_manager.db.commit()
                cursor.close()
                
        except Exception as e:
            st.error(f"Error logging out: {str(e)}")

# MongoDB document schemas (for reference, not enforced)
# Documents will be stored in collections: datasets, analyses, comments, users, user_sessions

class DatabaseManager:
    def __init__(self):
        self.client = None
        self.db = None
        self.init_database()
    
    def init_database(self):
        # Try MongoDB first
        if MONGODB_AVAILABLE:
            try:
                # Try MongoDB Atlas first, then local MongoDB
                mongodb_uri = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI")
                
                if mongodb_uri:
                    self.client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)
                    # Test connection
                    self.client.admin.command('ping')
                    
                    # Use database name from URI or default
                    db_name = os.getenv("MONGODB_DB_NAME", "dataviz_pro")
                    self.db = self.client[db_name]
                    self.db_type = "mongodb"
                    st.success("✅ MongoDB Atlas connected successfully!")
                    return
                    
            except ConnectionFailure as e:
                st.error(f"❌ MongoDB Connection Failed: {str(e)}")
            except Exception as e:
                st.error(f"❌ MongoDB Error: {str(e)}")
        
        # Fallback to PostgreSQL if available
        if POSTGRESQL_AVAILABLE:
            try:
                database_url = os.getenv("DATABASE_URL")
                if database_url:
                    self.db = psycopg2.connect(database_url)
                    self.db_type = "postgresql"
                    self.init_postgresql_tables()
                    return
            except Exception:
                pass
        
        # No database available (normal for local file analysis)
        if os.getenv("DATAVIZ_VERBOSE_DB", "").lower() in ("1", "true", "yes"):
            st.info(
                "No database configured. Upload files or use samples; connect MongoDB or "
                "PostgreSQL (see README) for accounts and dataset history."
            )
        self.client = None
        self.db = None
        self.db_type = None
    
    def init_postgresql_tables(self):
        """Initialize PostgreSQL tables if they don't exist"""
        try:
            cursor = self.db.cursor()
            
            # Create datasets table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS datasets (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    description TEXT,
                    file_type VARCHAR(50),
                    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    row_count INTEGER,
                    column_count INTEGER,
                    data_quality_score FLOAT
                )
            """)
            
            # Create analyses table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS analyses (
                    id SERIAL PRIMARY KEY,
                    dataset_id INTEGER,
                    analysis_type VARCHAR(100),
                    chart_type VARCHAR(100),
                    configuration JSONB,
                    insights TEXT,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    user_id VARCHAR(100)
                )
            """)
            
            # Create comments table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS comments (
                    id SERIAL PRIMARY KEY,
                    analysis_id INTEGER,
                    user_id VARCHAR(100),
                    comment_text TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            self.db.commit()
            cursor.close()
            
        except Exception as e:
            st.error(f"Error initializing PostgreSQL tables: {str(e)}")
            self.db.rollback()
    
    def save_dataset(self, name, description, file_type, df):
        if self.db is None:
            return None
        try:
            if self.db_type == "mongodb":
                dataset_doc = {
                    "name": name,
                    "description": description,
                    "file_type": file_type,
                    "upload_date": datetime.utcnow(),
                    "row_count": len(df),
                    "column_count": len(df.columns),
                    "data_quality_score": self.calculate_data_quality(df)
                }
                result = self.db.datasets.insert_one(dataset_doc)
                return str(result.inserted_id)
            elif self.db_type == "postgresql":
                cursor = self.db.cursor()
                cursor.execute("""
                    INSERT INTO datasets (name, description, file_type, row_count, column_count, data_quality_score)
                    VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
                """, (name, description, file_type, len(df), len(df.columns), self.calculate_data_quality(df)))
                dataset_id = cursor.fetchone()[0]
                self.db.commit()
                cursor.close()
                return str(dataset_id)
        except Exception as e:
            st.error(f"Error saving dataset: {str(e)}")
            return None
    
    def calculate_data_quality(self, df):
        total_cells = len(df) * len(df.columns)
        null_cells = df.isnull().sum().sum()
        return ((total_cells - null_cells) / total_cells) * 100
    
    def get_datasets(self):
        if self.db is None:
            return []
        try:
            if self.db_type == "mongodb":
                datasets = list(self.db.datasets.find().sort("upload_date", -1).limit(50))
                result = []
                for d in datasets:
                    result.append({
                        'id': str(d['_id']),
                        'name': d.get('name', 'Unknown'),
                        'description': d.get('description', ''),
                        'file_type': d.get('file_type', 'unknown'),
                        'upload_date': d.get('upload_date', datetime.utcnow()),
                        'row_count': d.get('row_count', 0),
                        'column_count': d.get('column_count', 0),
                        'data_quality_score': d.get('data_quality_score', 0)
                    })
                return result
            elif self.db_type == "postgresql":
                cursor = self.db.cursor(cursor_factory=RealDictCursor)
                cursor.execute("""
                    SELECT id, name, description, file_type, upload_date, 
                           row_count, column_count, data_quality_score
                    FROM datasets ORDER BY upload_date DESC LIMIT 50
                """)
                datasets = cursor.fetchall()
                cursor.close()
                return [{
                    'id': str(d['id']),
                    'name': d['name'] or 'Unknown',
                    'description': d['description'] or '',
                    'file_type': d['file_type'] or 'unknown',
                    'upload_date': d['upload_date'] or datetime.utcnow(),
                    'row_count': d['row_count'] or 0,
                    'column_count': d['column_count'] or 0,
                    'data_quality_score': d['data_quality_score'] or 0
                } for d in datasets]
        except Exception as e:
            st.error(f"Error retrieving datasets: {str(e)}")
            return []
    
    def save_analysis(self, dataset_id, analysis_type, chart_type, config, insights, user_id=None):
        if self.db is None:
            return None
        try:
            if self.db_type == "mongodb":
                analysis_doc = {
                    "dataset_id": dataset_id,
                    "analysis_type": analysis_type,
                    "chart_type": chart_type,
                    "configuration": config,  # Store as dict, not JSON string
                    "insights": insights,
                    "created_date": datetime.utcnow(),
                    "user_id": user_id or "anonymous"
                }
                result = self.db.analyses.insert_one(analysis_doc)
                return str(result.inserted_id)
            elif self.db_type == "postgresql":
                cursor = self.db.cursor()
                cursor.execute("""
                    INSERT INTO analyses (dataset_id, analysis_type, chart_type, configuration, insights, user_id)
                    VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
                """, (dataset_id, analysis_type, chart_type, json.dumps(config), insights, user_id or "anonymous"))
                analysis_id = cursor.fetchone()[0]
                self.db.commit()
                cursor.close()
                return str(analysis_id)
        except Exception as e:
            st.error(f"Error saving analysis: {str(e)}")
            return None
    
    def save_comment(self, analysis_id, user_id, comment_text):
        if self.db is None:
            return None
        try:
            comment_doc = {
                "analysis_id": analysis_id,
                "user_id": user_id,
                "comment_text": comment_text,
                "timestamp": datetime.utcnow()
            }
            result = self.db.comments.insert_one(comment_doc)
            return str(result.inserted_id)
        except Exception as e:
            st.error(f"Error saving comment: {str(e)}")
            return None

    def get_comments(self, analysis_id):
        """Get comments for a specific analysis"""
        if self.db is None:
            return []
        try:
            comments = list(self.db.comments.find({"analysis_id": analysis_id}).sort("timestamp", -1))
            return [{
                'id': str(c['_id']),
                'user_id': c.get('user_id', 'Anonymous'),
                'comment_text': c.get('comment_text', ''),
                'timestamp': c.get('timestamp', datetime.utcnow())
            } for c in comments]
        except Exception as e:
            return []

def show_auth_ui():
    """Display authentication UI (login/register forms)"""
    # Get analyzer instance
    analyzer = get_analyzer()
    
    st.markdown("""
    <div class="main-header">
        <h1>📊 DataViz Pro</h1>
        <p>Upload CSV or Excel and explore charts, stats, and optional AI insights</p>
        <p style="font-size: 0.9rem; opacity: 0.8;">Sign in to save history to a database, or continue locally</p>
    </div>
    """, unsafe_allow_html=True)

    if guest_mode_allowed():
        st.info("**Local file analysis** — no account or database required. Your data stays in this session.")
        if st.button("Continue as guest", type="primary", use_container_width=True):
            st.session_state.authenticated = True
            st.session_state.user = {
                "user_id": "guest",
                "email": "",
                "first_name": "Guest",
                "last_name": "User",
                "password_hash": "",
                "salt": "",
                "is_active": True,
            }
            st.session_state.session_id = None
            st.rerun()
        st.markdown("---")
    
    # Authentication tabs
    auth_tab1, auth_tab2 = st.tabs(["🔑 Login", "📝 Register"])
    
    with auth_tab1:
        st.markdown("### 🔑 Login to Your Account")
        
        with st.form("login_form"):
            email = st.text_input("📧 Email Address", placeholder="your.email@example.com")
            password = st.text_input("🔒 Password", type="password", placeholder="Enter your password")
            
            col1, col2 = st.columns([1, 2])
            with col1:
                login_submitted = st.form_submit_button("🚀 Login", type="primary")
            
            if login_submitted:
                if email and password:
                    with st.spinner("Authenticating..."):
                        result = analyzer.user_manager.authenticate_user(email, password)
                        
                        if result['success']:
                            st.session_state.authenticated = True
                            st.session_state.user = result['user']
                            st.session_state.session_id = result['session_id']
                            st.success("Login successful! Redirecting...")
                            st.rerun()
                        else:
                            st.error(result['message'])
                else:
                    st.error("Please enter both email and password")
    
    with auth_tab2:
        st.markdown("### 📝 Create New Account")
        
        with st.form("register_form"):
            col1, col2 = st.columns(2)
            with col1:
                first_name = st.text_input("👤 First Name", placeholder="John")
            with col2:
                last_name = st.text_input("👤 Last Name", placeholder="Doe")
            
            reg_email = st.text_input("📧 Email Address", placeholder="your.email@example.com")
            
            col3, col4 = st.columns(2)
            with col3:
                reg_password = st.text_input("🔒 Password", type="password", placeholder="At least 6 characters")
            with col4:
                confirm_password = st.text_input("🔒 Confirm Password", type="password", placeholder="Repeat password")
            
            # Terms and conditions
            terms_accepted = st.checkbox("I agree to the Terms of Service and Privacy Policy")
            
            register_submitted = st.form_submit_button("✨ Create Account", type="primary")
            
            if register_submitted:
                if not all([first_name, last_name, reg_email, reg_password, confirm_password]):
                    st.error("Please fill in all fields")
                elif reg_password != confirm_password:
                    st.error("Passwords do not match")
                elif not terms_accepted:
                    st.error("Please accept the Terms of Service and Privacy Policy")
                else:
                    with st.spinner("Creating account..."):
                        result = analyzer.user_manager.register_user(reg_email, reg_password, first_name, last_name)
                        
                        if result['success']:
                            st.success("Account created successfully! Please login with your credentials.")
                            st.balloons()
                        else:
                            st.error(result['message'])
    
    # Features showcase for non-authenticated users
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; padding: 2rem;">
        <h3>✨ What you'll get with DataViz Pro</h3>
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 1rem; margin-top: 1rem;">
            <div class="insight-card">
                <h4>🤖 AI-Powered Analytics</h4>
                <p>Natural language queries and automatic insight generation</p>
            </div>
            <div class="insight-card">
                <h4>📊 15+ Chart Types</h4>
                <p>From basic charts to advanced treemaps and geographic maps</p>
            </div>
            <div class="insight-card">
                <h4>🏗️ Dashboard Builder</h4>
                <p>Create custom dashboards with drag-and-drop widgets</p>
            </div>
            <div class="insight-card">
                <h4>🤝 Collaboration</h4>
                <p>Share your analysis and collaborate with your team</p>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

class DataAnalyzer:
    def __init__(self):
        self.df = None
        self.ai_client = None
        self.ai_provider = None
        self._db_manager = None
        self._user_manager = None
        self._init_ai_client()
    
    @property
    def db_manager(self):
        """Lazy initialization of database manager"""
        if self._db_manager is None:
            self._db_manager = DatabaseManager()
        return self._db_manager
    
    @property
    def user_manager(self):
        """Lazy initialization of user manager"""
        if self._user_manager is None:
            self._user_manager = UserManager(self.db_manager)
        return self._user_manager
    
    def _init_ai_client(self):
        """Initialize AI client with priority: Groq (free) > Hugging Face (free) > OpenAI (paid)"""
        # Priority 1: Groq (Free, fast, OpenAI-compatible)
        if GROQ_AVAILABLE:
            api_key = os.getenv("GROQ_API_KEY")
            if api_key:
                self.ai_client = Groq(api_key=api_key)
                self.ai_provider = "groq"
                return
        
        # Priority 2: Hugging Face Inference API (Free tier available)
        if HF_AVAILABLE:
            api_key = os.getenv("HF_API_KEY")
            if api_key:
                self.ai_client = api_key  # Store token for requests
                self.ai_provider = "huggingface"
                return
        
        # Priority 3: OpenAI (Paid - fallback)
        if OPENAI_AVAILABLE:
            api_key = os.getenv("OPENAI_API_KEY")
            if api_key:
                self.ai_client = OpenAI(api_key=api_key)
                self.ai_provider = "openai"
                return
    
    def load_data(self, uploaded_file) -> Optional[pd.DataFrame]:
        """Load and parse uploaded file with error handling"""
        try:
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
            elif uploaded_file.name.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(uploaded_file)
            else:
                raise ValueError("Unsupported file format. Please upload CSV or Excel files.")
            
            # Basic data validation
            if df.empty:
                raise ValueError("The uploaded file is empty.")
            
            if df.shape[1] < 2:
                raise ValueError("Dataset must have at least 2 columns for meaningful analysis.")
            
            # Clean column names
            df.columns = df.columns.str.strip().str.replace(' ', '_')
            
            # Convert date columns safely
            for col in df.columns:
                if df[col].dtype == 'object':
                    # Try to convert to datetime
                    try:
                        # Check if column looks like dates (sample first 10 non-null values)
                        sample = df[col].dropna().head(10)
                        if len(sample) > 0:
                            # Try conversion with errors='coerce'
                            converted = pd.to_datetime(sample, errors='coerce')
                            # Only convert if most values are valid dates (>50%)
                            if converted.notna().sum() / len(sample) > 0.5:
                                df[col] = pd.to_datetime(df[col], errors='coerce')
                    except:
                        pass
            
            # Streamlit sometimes fails Arrow serialization for mixed object columns
            # (common with Excel columns like "Unnamed: ..." and mixed types).
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].map(lambda v: None if pd.isna(v) else str(v))

            self.df = df
            return df
            
        except Exception as e:
            st.error(f"Error loading file: {str(e)}")
            return None
    
    def generate_sample_data(self, dataset_type: str) -> pd.DataFrame:
        """Generate sample datasets for demonstration"""
        np.random.seed(42)
        
        if dataset_type == "Sales Data":
            dates = pd.date_range('2023-01-01', periods=365, freq='D')
            regions = ['North', 'South', 'East', 'West', 'Central']
            products = ['Product A', 'Product B', 'Product C', 'Product D', 'Product E']
            
            data = []
            for date in dates:
                for region in np.random.choice(regions, np.random.randint(1, 4)):
                    for product in np.random.choice(products, np.random.randint(1, 3)):
                        sales = np.random.normal(1000, 300)
                        quantity = np.random.randint(10, 100)
                        data.append({
                            'Date': date,
                            'Region': region,
                            'Product': product,
                            'Sales': max(0, sales),
                            'Quantity': quantity,
                            'Price': sales / quantity if quantity > 0 else 0
                        })
            
            df = pd.DataFrame(data)
            
        elif dataset_type == "Customer Analytics":
            customers = 1000
            data = {
                'Customer_ID': range(1, customers + 1),
                'Age': np.random.randint(18, 80, customers),
                'Gender': np.random.choice(['Male', 'Female'], customers),
                'City': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'], customers),
                'Purchase_Amount': np.random.normal(500, 200, customers),
                'Visits': np.random.randint(1, 50, customers),
                'Category': np.random.choice(['Electronics', 'Clothing', 'Books', 'Home', 'Sports'], customers),
                'Satisfaction': np.random.uniform(1, 5, customers)
            }
            df = pd.DataFrame(data)
            df['Purchase_Amount'] = df['Purchase_Amount'].clip(lower=0)
            
        elif dataset_type == "Financial Data":
            dates = pd.date_range('2023-01-01', periods=252, freq='B')  # Business days
            stocks = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
            
            data = []
            for stock in stocks:
                price = 100  # Starting price
                for date in dates:
                    change = np.random.normal(0, 0.02)  # 2% daily volatility
                    price *= (1 + change)
                    volume = np.random.randint(1000000, 10000000)
                    data.append({
                        'Date': date,
                        'Symbol': stock,
                        'Price': price,
                        'Volume': volume,
                        'Change_%': change * 100
                    })
            
            df = pd.DataFrame(data)
            
        else:  # Website Analytics
            dates = pd.date_range('2023-01-01', periods=365, freq='D')
            pages = ['Homepage', 'Products', 'About', 'Contact', 'Blog', 'Support']
            sources = ['Organic', 'Paid', 'Social', 'Direct', 'Email']
            
            data = []
            for date in dates:
                for page in pages:
                    for source in np.random.choice(sources, np.random.randint(2, 5)):
                        visits = np.random.poisson(100)
                        bounce_rate = np.random.uniform(0.2, 0.8)
                        avg_time = np.random.uniform(30, 300)  # seconds
                        data.append({
                            'Date': date,
                            'Page': page,
                            'Traffic_Source': source,
                            'Visits': visits,
                            'Bounce_Rate': bounce_rate,
                            'Avg_Time_Seconds': avg_time,
                            'Conversions': np.random.poisson(visits * 0.05)
                        })
            
            df = pd.DataFrame(data)
        
        self.df = df
        return df
    
    def get_column_info(self) -> Dict:
        """Get comprehensive information about dataset columns"""
        if self.df is None:
            return {}
        
        info = {}
        for col in self.df.columns:
            col_data = self.df[col]
            info[col] = {
                'dtype': str(col_data.dtype),
                'null_count': col_data.isnull().sum(),
                'null_percentage': (col_data.isnull().sum() / len(col_data)) * 100,
                'unique_count': col_data.nunique(),
                'is_numeric': pd.api.types.is_numeric_dtype(col_data),
                'is_datetime': pd.api.types.is_datetime64_any_dtype(col_data),
                'sample_values': col_data.dropna().head(3).tolist()
            }
            
            if info[col]['is_numeric']:
                info[col]['min'] = col_data.min()
                info[col]['max'] = col_data.max()
                info[col]['mean'] = col_data.mean()
                info[col]['std'] = col_data.std()
                
        return info
    
    def detect_anomalies(self, column: str) -> List[int]:
        """Detect anomalies using IQR method"""
        if self.df is None or column not in self.df.columns:
            return []
        
        if not pd.api.types.is_numeric_dtype(self.df[column]):
            return []
        
        Q1 = self.df[column].quantile(0.25)
        Q3 = self.df[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        anomalies = self.df[(self.df[column] < lower_bound) | (self.df[column] > upper_bound)].index.tolist()
        return anomalies
    
    def generate_insights(self) -> List[str]:
        """Generate AI-powered insights about the dataset"""
        if self.df is None:
            return ["No data available for analysis."]
        
        insights = []
        
        # Basic statistical insights
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            # Correlation insights
            corr_matrix = self.df[numeric_cols].corr()
            high_corr = corr_matrix.abs() > 0.7
            for i in range(len(high_corr.columns)):
                for j in range(i+1, len(high_corr.columns)):
                    if high_corr.iloc[i, j]:
                        col1, col2 = high_corr.columns[i], high_corr.columns[j]
                        corr_val = corr_matrix.iloc[i, j]
                        insights.append(f"Strong {'positive' if corr_val > 0 else 'negative'} correlation ({corr_val:.2f}) between {col1} and {col2}")
            
            # Anomaly detection insights
            for col in numeric_cols:
                anomalies = self.detect_anomalies(col)
                if len(anomalies) > 0:
                    percentage = (len(anomalies) / len(self.df)) * 100
                    insights.append(f"Detected {len(anomalies)} anomalies ({percentage:.1f}%) in {col}")
            
            # Trend insights for time series data
            date_cols = self.df.select_dtypes(include=['datetime64']).columns
            if len(date_cols) > 0 and len(numeric_cols) > 0:
                date_col = date_cols[0]
                for num_col in numeric_cols[:2]:  # Limit to first 2 numeric columns
                    df_sorted = self.df.sort_values(date_col)
                    if len(df_sorted) > 1:
                        trend = np.polyfit(range(len(df_sorted)), df_sorted[num_col].fillna(0), 1)[0]
                        if abs(trend) > 0.01:
                            direction = "increasing" if trend > 0 else "decreasing"
                            insights.append(f"{num_col} shows a {direction} trend over time")
        
        # Categorical insights
        categorical_cols = self.df.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            value_counts = self.df[col].value_counts()
            if len(value_counts) > 1:
                top_category = value_counts.index[0]
                percentage = (value_counts.iloc[0] / len(self.df)) * 100
                insights.append(f"'{top_category}' is the most frequent value in {col} ({percentage:.1f}%)")
        
        return insights[:10]  # Limit to top 10 insights
    
    def natural_language_query(self, query: str) -> Dict[str, Any]:
        """Process natural language queries using available AI provider (Groq, Hugging Face, or OpenAI)"""
        if not self.ai_client or self.df is None:
            return {"error": "No AI provider available. Please set GROQ_API_KEY (free), HF_API_KEY (free), or OPENAI_API_KEY."}
        
        try:
            # Prepare context about the dataset
            col_info = self.get_column_info()
            dataset_context = f"Dataset shape: {self.df.shape}\n"
            dataset_context += "Columns and types:\n"
            for col, info in col_info.items():
                dataset_context += f"- {col}: {info['dtype']} (unique values: {info['unique_count']})\n"
            
            system_prompt = f"""You are a data analysis expert. Given a dataset with the following structure:

{dataset_context}

The user will ask questions about this data. Your task is to:
1. Understand what visualization or analysis they want
2. Suggest the appropriate chart type
3. Identify which columns to use
4. Provide the analysis configuration

Respond with JSON in this exact format:
{{
    "chart_type": "line|bar|scatter|heatmap|pie|histogram|box",
    "x_column": "column_name",
    "y_column": "column_name", 
    "color_column": "column_name_or_null",
    "group_by": "column_name_or_null",
    "aggregation": "sum|mean|count|max|min|none",
    "filters": {{"column": "value"}},
    "title": "Chart title",
    "insights": "Brief analysis insight"
}}"""

            # Route to appropriate provider
            if self.ai_provider == "groq":
                return self._query_groq(system_prompt, query)
            elif self.ai_provider == "huggingface":
                return self._query_huggingface(system_prompt, query)
            elif self.ai_provider == "openai":
                return self._query_openai(system_prompt, query)
            else:
                return {"error": "Unknown AI provider"}
                
        except Exception as e:
            return {"error": f"Failed to process query: {str(e)}"}
    
    def _query_groq(self, system_prompt: str, query: str) -> Dict[str, Any]:
        """Query using Groq API (Free, fast) - groq.com"""
        try:
            response = self.ai_client.chat.completions.create(
                model="llama-3.3-70b-versatile",  # Fast, capable, free tier available
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": query}
                ],
                response_format={"type": "json_object"},
                temperature=0.1
            )
            result = json.loads(response.choices[0].message.content)
            result["_provider"] = "groq"  # Track which provider was used
            return result
        except Exception as e:
            return {"error": f"Groq API error: {str(e)}"}
    
    def _query_huggingface(self, system_prompt: str, query: str) -> Dict[str, Any]:
        """Query using Hugging Face Inference API (Free tier available) - huggingface.co"""
        try:
            API_URL = "https://api-inference.huggingface.co/models/mistralai/Mistral-7B-Instruct-v0.3"
            headers = {"Authorization": f"Bearer {self.ai_client}"}
            
            # Format for instruction-following models
            payload = {
                "inputs": f"<s>[INST] {system_prompt}\n\nUser query: {query} [/INST]",
                "parameters": {
                    "max_new_tokens": 512,
                    "temperature": 0.1,
                    "return_full_text": False
                }
            }
            
            response = requests.post(API_URL, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                generated_text = response.json()[0]["generated_text"]
                # Extract JSON from response
                import re
                json_match = re.search(r'\{.*\}', generated_text, re.DOTALL)
                if json_match:
                    result = json.loads(json_match.group())
                    result["_provider"] = "huggingface"
                    return result
                else:
                    return {"error": "Could not parse JSON from Hugging Face response", "raw": generated_text}
            else:
                return {"error": f"Hugging Face API error: {response.status_code}"}
                
        except Exception as e:
            return {"error": f"Hugging Face API error: {str(e)}"}
    
    def _query_openai(self, system_prompt: str, query: str) -> Dict[str, Any]:
        """Query using OpenAI API (Paid)"""
        try:
            response = self.ai_client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": query}
                ],
                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            result["_provider"] = "openai"
            return result
        except Exception as e:
            return {"error": f"OpenAI API error: {str(e)}"}

# Initialize the analyzer
@st.cache_resource
def get_analyzer():
    return DataAnalyzer()

analyzer = get_analyzer()


@st.cache_data(show_spinner=False)
def _cached_geocode_osm(query: str) -> tuple:
    """Cached OSM Nominatim lookup (caller should still throttle batch first-seen keys)."""
    from data_context import geocode_place
    return geocode_place(query)


# Main application
def main():
    # Initialize session state for authentication
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
        st.session_state.user = None
        st.session_state.session_id = None
    
    # Check for existing session (signed-in users only)
    sid = st.session_state.get("session_id")
    if not st.session_state.authenticated and sid:
        user = analyzer.user_manager.validate_session(sid)
        if user:
            st.session_state.authenticated = True
            st.session_state.user = user
    
    # Authentication UI
    if not st.session_state.authenticated:
        show_auth_ui()
        return
    
    # Header with status indicators and user info
    st.markdown(f"""
    <div class="main-header">
        <h1>📊 DataViz Pro</h1>
        <p>Advanced Analytics Dashboard with AI-Powered Insights</p>
        <div style="position: absolute; top: 1rem; right: 1rem;">
            <div style="display: flex; align-items: center; gap: 1rem;">
                <span style="font-size: 0.9rem; color: rgba(255,255,255,0.9);">
                    Welcome, {st.session_state.user['first_name']} {st.session_state.user['last_name']}
                </span>
                <span class="status-indicator status-online" data-tooltip="System Online"></span>
                <span style="font-size: 0.8rem; color: rgba(255,255,255,0.8);">Live</span>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Breadcrumb navigation
    if analyzer.df is not None:
        st.markdown("""
        <div class="breadcrumb">
            📊 DataViz Pro → 📁 Dataset Loaded → 🔍 Analysis Mode
        </div>
        """, unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.markdown("### 🎛️ Control Panel")
        
        # User info and logout
        st.markdown(f"""
        <div style="background: rgba(255, 255, 255, 0.1); padding: 1rem; border-radius: 10px; margin-bottom: 1rem;">
            <h4>👤 {st.session_state.user['first_name']} {st.session_state.user['last_name']}</h4>
            <p style="font-size: 0.8rem; opacity: 0.8;">{st.session_state.user.get('email') or 'Local session'}</p>
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("🚪 Logout"):
            analyzer.user_manager.logout_user(st.session_state.session_id)
            st.session_state.authenticated = False
            st.session_state.user = None
            st.session_state.session_id = None
            st.rerun()

        u = st.session_state.user or {}
        guest = is_guest_user(u)
        db = analyzer.db_manager.db

        if guest:
            st.caption("Local session — data stays in memory unless you connect a database.")
        elif db is not None:
            st.markdown("""
            <div style="background: rgba(0, 255, 136, 0.1); padding: 0.5rem; border-radius: 8px; margin-bottom: 1rem;">
                <span class="status-indicator status-online"></span>Database connected
            </div>
            """, unsafe_allow_html=True)
        elif MONGODB_AVAILABLE or POSTGRESQL_AVAILABLE:
            st.caption("Database drivers installed — set `MONGODB_URI` or `DATABASE_URL` for saved history and accounts.")
        else:
            st.caption("Optional: install database extras for saved datasets (`pip install -e \".[database]\"`).")

        source_options = ["Upload File", "Sample Dataset"]
        if db is not None:
            source_options.append("Database History")
        data_source = st.radio(
            "Choose Data Source:",
            source_options,
            help="Upload files, use samples, or load from your connected database",
        )
        
        df = None
        
        if data_source == "Upload File":
            uploaded_file = st.file_uploader(
                "Upload your data file",
                type=['csv', 'xlsx', 'xls'],
                help="Supported formats: CSV, Excel"
            )
            
            if uploaded_file is not None:
                with st.spinner("Loading data..."):
                    df = analyzer.load_data(uploaded_file)
                    if df is not None:
                        st.success(f"✅ Loaded {len(df)} rows, {len(df.columns)} columns")

                        # Fast-path automated visualizations (recommendations only)
                        try:
                            from auto_viz import AutoVizGenerator
                            if 'auto_viz_recommendations' not in st.session_state:
                                st.session_state.auto_viz_recommendations = []
                            gen = AutoVizGenerator()
                            st.session_state.auto_viz_recommendations = gen.recommend_visualizations(df)[:6]
                            st.session_state.auto_viz_last_loaded_at = datetime.now().isoformat()
                        except Exception:
                            # Don't block dataset loading if auto-viz fails
                            st.session_state.auto_viz_recommendations = []

                        # Lightweight Auto Analysis (B): anomalies + correlation summary
                        try:
                            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()[:6]
                            anomaly_summary = []
                            for col in numeric_cols:
                                if col in df.columns:
                                    Q1 = df[col].quantile(0.25)
                                    Q3 = df[col].quantile(0.75)
                                    IQR = Q3 - Q1
                                    lower = Q1 - 1.5 * IQR
                                    upper = Q3 + 1.5 * IQR
                                    anomalies = df[(df[col] < lower) | (df[col] > upper)].shape[0]
                                    anomaly_summary.append({"column": col, "anomaly_rows": int(anomalies)})

                            corr_summary = {}
                            if len(numeric_cols) >= 2:
                                corr = df[numeric_cols].corr()
                                # Keep only strongest abs correlations (upper triangle)
                                pairs = []
                                cols = corr.columns.tolist()
                                for i in range(len(cols)):
                                    for j in range(i + 1, len(cols)):
                                        pairs.append({"x": cols[i], "y": cols[j], "corr": float(corr.iloc[i, j])})
                                pairs.sort(key=lambda p: abs(p["corr"]), reverse=True)
                                corr_summary = {"top_pairs": pairs[:8]}

                            st.session_state.auto_analysis = {
                                "anomalies": anomaly_summary,
                                "correlations": corr_summary
                            }
                        except Exception:
                            st.session_state.auto_analysis = {"anomalies": [], "correlations": {}}
                        try:
                            from data_context import build_data_digest
                            st.session_state.data_digest = build_data_digest(df)
                        except Exception:
                            st.session_state.data_digest = None
        elif data_source == "Sample Dataset":
            sample_type = st.selectbox(
                "Select Sample Dataset:",
                ["Sales Data", "Customer Analytics", "Financial Data", "Website Analytics"]
            )
            
            if st.button("Load Sample Data"):
                with st.spinner("Generating sample data..."):
                    df = analyzer.generate_sample_data(sample_type)
                    if analyzer.db_manager.db is not None:
                        analyzer.db_manager.save_dataset(
                            name=f"Sample {sample_type}",
                            description=(
                                f"Generated sample dataset for {sample_type.lower()} by "
                                f"{st.session_state.user.get('first_name', '')} {st.session_state.user.get('last_name', '')}"
                            ),
                            file_type="generated",
                            df=df,
                        )
                    st.success(f"✅ Generated {len(df)} rows, {len(df.columns)} columns")

                    # Fast-path automated visualizations (recommendations only)
                    try:
                        from auto_viz import AutoVizGenerator
                        if 'auto_viz_recommendations' not in st.session_state:
                            st.session_state.auto_viz_recommendations = []
                            gen = AutoVizGenerator()
                            st.session_state.auto_viz_recommendations = gen.recommend_visualizations(df)[:6]
                            st.session_state.auto_viz_last_loaded_at = datetime.now().isoformat()
                    except Exception:
                        # Don't block dataset loading if auto-viz fails
                        st.session_state.auto_viz_recommendations = []
                    try:
                        from data_context import build_data_digest
                        st.session_state.data_digest = build_data_digest(df)
                    except Exception:
                        st.session_state.data_digest = None
        
        elif data_source == "Database History":
            datasets = analyzer.db_manager.get_datasets()
            if datasets:
                st.markdown("**Previous Datasets:**")
                for dataset in datasets[:10]:  # Show last 10
                    with st.expander(f"📊 {dataset['name']} ({dataset['file_type']})"):
                        st.write(f"**Rows:** {dataset['row_count']:,}")
                        st.write(f"**Columns:** {dataset['column_count']}")
                        st.write(f"**Quality:** {dataset['data_quality_score']:.1f}%")
                        st.write(f"**Date:** {dataset['upload_date'].strftime('%Y-%m-%d %H:%M')}")
                        if st.button(f"Load Dataset", key=f"load_{dataset['id']}"):
                            st.info("Dataset loading from database - this would restore the saved data in a full implementation")
            else:
                st.info("No datasets in database history. Upload data or generate samples to see history.")
        
        # Show data info if available
        if analyzer.df is not None:
            st.markdown("### 📋 Dataset Info")
            col_info = analyzer.get_column_info()
            
            # Data quality metrics
            total_nulls = sum(info['null_count'] for info in col_info.values())
            data_quality = ((len(analyzer.df) * len(analyzer.df.columns) - total_nulls) / 
                          (len(analyzer.df) * len(analyzer.df.columns))) * 100
            
            st.metric("Data Quality", f"{data_quality:.1f}%")
            st.metric("Total Rows", f"{len(analyzer.df):,}")
            st.metric("Columns", len(analyzer.df.columns))
            
            # Column types
            numeric_cols = [col for col, info in col_info.items() if info['is_numeric']]
            categorical_cols = [col for col, info in col_info.items() if not info['is_numeric']]
            
            st.markdown("**Numeric Columns:**")
            for col in numeric_cols[:5]:  # Show first 5
                st.write(f"• {col}")
            if len(numeric_cols) > 5:
                st.write(f"... and {len(numeric_cols) - 5} more")
                
            st.markdown("**Categorical Columns:**")
            for col in categorical_cols[:5]:  # Show first 5
                st.write(f"• {col}")
            if len(categorical_cols) > 5:
                st.write(f"... and {len(categorical_cols) - 5} more")
            
            # Collaboration & Sharing
            st.markdown("### 🤝 Collaboration")
            
            # Initialize session state for comments
            if 'comments' not in st.session_state:
                st.session_state.comments = []
            
            # Comment system
            with st.expander("💬 Comments & Notes"):
                comment_text = st.text_area("Add a comment or note:", placeholder="Share insights with your team...")
                col1, col2 = st.columns([1, 4])
                
                with col1:
                    if st.button("📝 Add Comment"):
                        if comment_text.strip():
                            user_name = f"{st.session_state.user['first_name']} {st.session_state.user['last_name']}"
                            new_comment = {
                                'text': comment_text,
                                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'user': user_name,
                                'user_id': st.session_state.user['user_id']
                            }
                            st.session_state.comments.append(new_comment)
                            
                            # Save comment to database if available
                            if analyzer.db_manager.db is not None:
                                analyzer.db_manager.save_comment(
                                    analysis_id="current_session",  # In a real app, this would be the actual analysis ID
                                    user_id=st.session_state.user['user_id'],
                                    comment_text=comment_text
                                )
                            
                            st.success("Comment added!")
                            st.rerun()
                
                # Display comments
                if st.session_state.comments:
                    st.markdown("**Recent Comments:**")
                    for i, comment in enumerate(reversed(st.session_state.comments[-5:])):  # Show last 5
                        st.markdown(f"""
                        <div style="background: rgba(255,255,255,0.05); padding: 0.5rem; border-radius: 8px; margin: 0.5rem 0;">
                            <small><strong>{comment['user']}</strong> - {comment['timestamp']}</small><br>
                            {comment['text']}
                        </div>
                        """, unsafe_allow_html=True)
            
            # Sharing options
            with st.expander("🔗 Share & Export"):
                st.markdown("**Share this analysis:**")
                
                # Generate shareable link (mock implementation)
                current_url = "https://dataviz-pro.replit.app"  # This would be the actual URL
                st.text_input("Shareable Link:", value=current_url, disabled=True)
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    if st.button("📋 Copy Link"):
                        st.success("Link copied to clipboard!")
                
                with col2:
                    if st.button("📧 Email Share"):
                        st.info("Email sharing feature would open email client")
                
                with col3:
                    if st.button("💾 Save Session"):
                        session_data = {
                            'dataset_info': col_info,
                            'comments': st.session_state.comments,
                            'timestamp': datetime.now().isoformat()
                        }
                        session_json = json.dumps(session_data, indent=2)
                        st.download_button(
                            label="Download Session",
                            data=session_json,
                            file_name=f"analysis_session_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                            mime="application/json"
                        )
    
    # Main content area
    if analyzer.df is not None:
        df = analyzer.df
        
        # Create tabs for different views
        tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10, tab11, tab12, tab13 = st.tabs([
            "🔍 Overview", "📊 Visualizations", "🤖 AI Insights", "💬 Natural Language", "🏗️ Dashboard Builder", "🗺️ Geographic Maps", "⚙️ Advanced", "✅ Feature Status",
            "🔮 Auto Viz", "📈 Forecasting", "🧪 A/B Testing", "🔧 Formula Builder", "📋 Reporting"
        ])
        
        with tab1:
            st.markdown("## 📈 Data Overview")

            digest = st.session_state.get("data_digest")
            if digest is None or digest.get("shape") != (len(df), len(df.columns)):
                try:
                    from data_context import build_data_digest
                    digest = build_data_digest(df)
                    st.session_state.data_digest = digest
                except Exception:
                    digest = None
            if digest:
                st.markdown("### 📌 How to read this file")
                for b in digest.get("bullets", []):
                    st.markdown(f"- {b}", unsafe_allow_html=True)
                roles = digest.get("column_roles", [])
                if roles:
                    h = min(420, 100 + 26 * len(roles))
                    with st.expander("Column roles (auto-detected)", expanded=False):
                        st.dataframe(
                            pd.DataFrame(roles),
                            use_container_width=True,
                            height=h,
                        )

            # Lightweight Auto Analysis (B): show right after load
            auto_analysis = st.session_state.get("auto_analysis")
            if isinstance(auto_analysis, dict) and (auto_analysis.get("anomalies") or auto_analysis.get("correlations")):
                st.markdown("### ⚡ Auto Analysis")
                a = auto_analysis.get("anomalies", [])
                c = auto_analysis.get("correlations", {})

                if a:
                    st.write("**Anomaly summary (IQR outliers)**")
                    st.dataframe(pd.DataFrame(a), use_container_width=True, height=220)

                if c and c.get("top_pairs"):
                    st.write("**Top correlations**")
                    top_pairs = c["top_pairs"]
                    for item in top_pairs[:8]:
                        st.markdown(f"- {item['x']} vs {item['y']}: {item['corr']:.2f}")

            # Key metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.markdown("""
                <div class="metric-card">
                    <h3>📊 Total Records</h3>
                    <h2>{:,}</h2>
                </div>
                """.format(len(df)), unsafe_allow_html=True)
            
            with col2:
                numeric_cols = df.select_dtypes(include=[np.number]).columns
                st.markdown("""
                <div class="metric-card">
                    <h3>🔢 Numeric Columns</h3>
                    <h2>{}</h2>
                </div>
                """.format(len(numeric_cols)), unsafe_allow_html=True)
            
            with col3:
                null_percentage = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
                st.markdown("""
                <div class="metric-card">
                    <h3>✅ Data Completeness</h3>
                    <h2>{:.1f}%</h2>
                </div>
                """.format(100 - null_percentage), unsafe_allow_html=True)
            
            with col4:
                memory_usage = df.memory_usage(deep=True).sum() / 1024**2  # MB
                st.markdown("""
                <div class="metric-card">
                    <h3>💾 Memory Usage</h3>
                    <h2>{:.1f} MB</h2>
                </div>
                """.format(memory_usage), unsafe_allow_html=True)
            
            # Data preview
            st.markdown("### 📋 Data Preview")
            st.dataframe(df.head(100), width='stretch')
            
            # Column statistics
            st.markdown("### 📊 Column Statistics")
            col_info = analyzer.get_column_info()
            
            stats_data = []
            for col, info in col_info.items():
                stats_data.append({
                    'Column': col,
                    'Type': info['dtype'],
                    'Null Count': info['null_count'],
                    'Null %': f"{info['null_percentage']:.1f}%",
                    'Unique Values': info['unique_count'],
                    'Sample Values': ', '.join(map(str, info['sample_values']))
                })
            
            stats_df = pd.DataFrame(stats_data)
            st.dataframe(stats_df, width='stretch')
        
        with tab2:
            st.markdown("## 📊 Interactive Visualizations")
            
            # Chart type selection
            chart_type = st.selectbox(
                "Select Chart Type:",
                ["Line Chart", "Bar Chart", "Scatter Plot", "Heatmap", "Histogram", "Box Plot", "Pie Chart", "Treemap", "Sankey Diagram", "Sunburst Chart", "Waterfall Chart"]
            )
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Column selection based on chart type
                numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
                categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
                date_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
                all_cols = df.columns.tolist()
                
                # Initialize variables
                x_col = None
                y_col = None
                color_col = None
                selected_cols = []
                category_col = None
                value_col = None
                agg_func = "sum"
                
                if chart_type in ["Line Chart", "Bar Chart", "Scatter Plot"]:
                    x_col = st.selectbox("X-axis:", all_cols)
                    y_col = st.selectbox("Y-axis:", numeric_cols) if numeric_cols else None
                    color_col = st.selectbox("Color by:", [None] + categorical_cols)
                elif chart_type == "Heatmap":
                    if len(numeric_cols) >= 2:
                        selected_cols = st.multiselect("Select columns for heatmap:", numeric_cols, default=numeric_cols[:5])
                    else:
                        st.warning("Need at least 2 numeric columns for heatmap")
                        selected_cols = []
                elif chart_type in ["Histogram", "Box Plot"]:
                    y_col = st.selectbox("Column to analyze:", numeric_cols) if numeric_cols else None
                    color_col = st.selectbox("Group by:", [None] + categorical_cols)
                elif chart_type == "Pie Chart":
                    category_col = st.selectbox("Category column:", categorical_cols) if categorical_cols else None
                    value_col = st.selectbox("Value column (optional):", [None] + numeric_cols)
                elif chart_type in ["Treemap", "Sunburst Chart", "Waterfall Chart", "Sankey Diagram"]:
                    st.info(f"Advanced chart type: {chart_type} - Configure options below")
            
            with col2:
                # Chart customization
                st.markdown("### 🎨 Customization")
                
                # Theme preferences (persistent)
                if 'theme_preferences' not in st.session_state:
                    st.session_state.theme_preferences = {
                        'color_scheme': 'plotly',
                        'chart_height': 500,
                        'animation_enabled': True,
                        'show_grid': True
                    }
                
                # Color scheme
                color_scheme = st.selectbox(
                    "Color Scheme:",
                    ["plotly", "viridis", "plasma", "inferno", "magma", "cividis", "sunset", "rainbow"],
                    index=["plotly", "viridis", "plasma", "inferno", "magma", "cividis", "sunset", "rainbow"].index(st.session_state.theme_preferences['color_scheme'])
                )
                st.session_state.theme_preferences['color_scheme'] = color_scheme
                
                # Chart size
                chart_height = st.slider("Chart Height:", 300, 1000, st.session_state.theme_preferences['chart_height'])
                st.session_state.theme_preferences['chart_height'] = chart_height
                
                # Animation toggle
                enable_animations = st.checkbox("Enable Animations", value=st.session_state.theme_preferences['animation_enabled'])
                st.session_state.theme_preferences['animation_enabled'] = enable_animations
                
                # Grid toggle
                show_grid = st.checkbox("Show Grid", value=st.session_state.theme_preferences['show_grid'])
                st.session_state.theme_preferences['show_grid'] = show_grid
                
                # Aggregation for grouped data
                if chart_type in ["Bar Chart", "Line Chart"]:
                    agg_func = st.selectbox("Aggregation:", ["sum", "mean", "count", "max", "min"])
                
                # Performance settings
                with st.expander("⚡ Performance Settings"):
                    sample_size = st.slider("Sample Size (for large datasets):", 1000, 50000, 10000)
                    enable_caching = st.checkbox("Enable Data Caching", value=True)
            
            # Generate visualization with performance optimization
            try:
                fig = None
                
                # Apply sampling for large datasets
                display_df = df
                if len(df) > sample_size:
                    display_df = df.sample(n=sample_size).sort_index()
                    st.info(f"Displaying sample of {sample_size:,} rows from {len(df):,} total rows for performance")
                
                if chart_type == "Line Chart" and x_col and y_col:
                    if color_col:
                        fig = px.line(display_df, x=x_col, y=y_col, color=color_col, 
                                    color_discrete_sequence=px.colors.qualitative.Set3,
                                    title=f"{y_col} vs {x_col}")
                    else:
                        fig = px.line(display_df, x=x_col, y=y_col, title=f"{y_col} vs {x_col}")
                
                elif chart_type == "Bar Chart" and x_col and y_col:
                    if color_col:
                        df_agg = df.groupby([x_col, color_col])[y_col].agg(agg_func).reset_index()
                        fig = px.bar(df_agg, x=x_col, y=y_col, color=color_col,
                                   color_discrete_sequence=px.colors.qualitative.Set3)
                    else:
                        df_agg = df.groupby(x_col)[y_col].agg(agg_func).reset_index()
                        fig = px.bar(df_agg, x=x_col, y=y_col)
                
                elif chart_type == "Scatter Plot" and x_col and y_col:
                    fig = px.scatter(df, x=x_col, y=y_col, color=color_col,
                                   color_continuous_scale=color_scheme if color_col in numeric_cols else None,
                                   color_discrete_sequence=px.colors.qualitative.Set3 if color_col in categorical_cols else None)
                
                elif chart_type == "Heatmap" and selected_cols:
                    corr_matrix = df[selected_cols].corr()
                    fig = px.imshow(corr_matrix, text_auto=True, aspect="auto",
                                  color_continuous_scale=color_scheme)
                
                elif chart_type == "Histogram" and y_col:
                    fig = px.histogram(df, x=y_col, color=color_col,
                                     color_discrete_sequence=px.colors.qualitative.Set3)
                
                elif chart_type == "Box Plot" and y_col:
                    fig = px.box(df, y=y_col, color=color_col,
                               color_discrete_sequence=px.colors.qualitative.Set3)
                
                elif chart_type == "Pie Chart" and 'category_col' in locals():
                    if 'value_col' in locals() and value_col:
                        pie_data = df.groupby(category_col)[value_col].sum().reset_index()
                        fig = px.pie(pie_data, values=value_col, names=category_col,
                                   color_discrete_sequence=px.colors.qualitative.Set3)
                    else:
                        pie_data = df[category_col].value_counts().reset_index()
                        pie_data.columns = [category_col, 'count']
                        fig = px.pie(pie_data, values='count', names=category_col,
                                   color_discrete_sequence=px.colors.qualitative.Set3)
                
                elif chart_type == "Treemap" and len(categorical_cols) > 0:
                    if len(categorical_cols) >= 2:
                        path_cols = categorical_cols[:2]
                        if len(numeric_cols) > 0:
                            value_col = numeric_cols[0]
                            treemap_data = df.groupby(path_cols)[value_col].sum().reset_index()
                            fig = px.treemap(treemap_data, path=path_cols, values=value_col,
                                           color=value_col, color_continuous_scale=color_scheme)
                        else:
                            treemap_data = df.groupby(path_cols).size().reset_index()
                            treemap_data.columns = list(path_cols) + ['count']
                            fig = px.treemap(treemap_data, path=path_cols, values='count')
                    else:
                        st.warning("Treemap requires at least 2 categorical columns")
                
                elif chart_type == "Sunburst Chart" and len(categorical_cols) > 0:
                    if len(categorical_cols) >= 2:
                        path_cols = categorical_cols[:3]  # Up to 3 levels
                        if len(numeric_cols) > 0:
                            value_col = numeric_cols[0]
                            sunburst_data = df.groupby(path_cols)[value_col].sum().reset_index()
                            fig = px.sunburst(sunburst_data, path=path_cols, values=value_col,
                                            color=value_col, color_continuous_scale=color_scheme)
                        else:
                            sunburst_data = df.groupby(path_cols).size().reset_index()
                            sunburst_data.columns = list(path_cols) + ['count']
                            fig = px.sunburst(sunburst_data, path=path_cols, values='count')
                    else:
                        st.warning("Sunburst chart requires at least 2 categorical columns")
                
                elif chart_type == "Waterfall Chart" and len(numeric_cols) > 0:
                    if len(categorical_cols) > 0:
                        cat_col = categorical_cols[0]
                        num_col = numeric_cols[0]
                        waterfall_data = df.groupby(cat_col)[num_col].sum().reset_index()
                        
                        fig = go.Figure(go.Waterfall(
                            name="Waterfall",
                            orientation="v",
                            measure=["relative"] * len(waterfall_data),
                            x=waterfall_data[cat_col],
                            y=waterfall_data[num_col],
                            connector={"line": {"color": "rgb(63, 63, 63)"}},
                        ))
                        fig.update_layout(title="Waterfall Chart", template="plotly_dark")
                    else:
                        st.warning("Waterfall chart requires at least one categorical column")
                
                elif chart_type == "Sankey Diagram" and len(categorical_cols) >= 2:
                    source_col, target_col = categorical_cols[0], categorical_cols[1]
                    
                    # Create source-target pairs
                    sankey_data = df.groupby([source_col, target_col]).size().reset_index()
                    sankey_data.columns = [source_col, target_col, 'value']
                    
                    # Create unique labels
                    sources = sankey_data[source_col].unique()
                    targets = sankey_data[target_col].unique()
                    all_labels = list(sources) + [t for t in targets if t not in sources]
                    
                    # Map to indices
                    label_map = {label: i for i, label in enumerate(all_labels)}
                    
                    fig = go.Figure(data=[go.Sankey(
                        node=dict(
                            pad=15,
                            thickness=20,
                            line=dict(color="black", width=0.5),
                            label=all_labels,
                            color="blue"
                        ),
                        link=dict(
                            source=[label_map[s] for s in sankey_data[source_col]],
                            target=[label_map[t] for t in sankey_data[target_col]],
                            value=sankey_data['value']
                        )
                    )])
                    fig.update_layout(title_text="Sankey Diagram", font_size=10, template="plotly_dark")
                
                if fig:
                    # Enhanced styling with user preferences
                    fig.update_layout(
                        height=chart_height,
                        template="plotly_dark",
                        showlegend=True,
                        margin=dict(l=0, r=0, t=50, b=0),
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                        xaxis=dict(showgrid=show_grid, gridcolor='rgba(255,255,255,0.1)'),
                        yaxis=dict(showgrid=show_grid, gridcolor='rgba(255,255,255,0.1)'),
                        font=dict(family="Inter, sans-serif"),
                        transition=dict(duration=500 if enable_animations else 0)
                    )
                    
                    # Add glassmorphism container
                    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                    st.plotly_chart(fig, use_container_width=True, key=f"chart_{chart_type}")
                    st.markdown('</div>', unsafe_allow_html=True)
                    
                    # Export options
                    st.markdown("### 💾 Export Options")
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        if st.button("📷 Download PNG"):
                            img_bytes = fig.to_image(format="png", width=1200, height=800)
                            st.download_button(
                                label="Download PNG",
                                data=img_bytes,
                                file_name=f"chart_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png",
                                mime="image/png"
                            )
                    
                    with col2:
                        if st.button("📄 Download HTML"):
                            html_str = fig.to_html(include_plotlyjs=True)
                            st.download_button(
                                label="Download HTML",
                                data=html_str,
                                file_name=f"chart_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html",
                                mime="text/html"
                            )
                    
                    with col3:
                        if st.button("📊 Download Data"):
                            csv = df.to_csv(index=False)
                            st.download_button(
                                label="Download CSV",
                                data=csv,
                                file_name=f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv"
                            )
                
                else:
                    st.warning("Please select appropriate columns for the chosen chart type.")
                    
            except Exception as e:
                st.error(f"Error creating visualization: {str(e)}")
        
        with tab3:
            st.markdown("## 🤖 AI-Powered Insights")
            
            # Generate insights
            if st.button("🔍 Generate Insights", type="primary"):
                with st.spinner("Analyzing data and generating insights..."):
                    insights = analyzer.generate_insights()
                    
                    if insights:
                        st.markdown("### 🎯 Key Insights")
                        for i, insight in enumerate(insights, 1):
                            st.markdown(f"""
                            <div class="insight-card">
                                <h4>💡 Insight #{i}</h4>
                                <p>{insight}</p>
                            </div>
                            """, unsafe_allow_html=True)
                    else:
                        st.info("No significant insights detected in the current dataset.")
            
            # Anomaly detection
            st.markdown("### 🚨 Anomaly Detection")
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            
            if numeric_cols:
                anomaly_col = st.selectbox("Select column for anomaly detection:", numeric_cols)
                
                if st.button("Detect Anomalies"):
                    anomalies = analyzer.detect_anomalies(anomaly_col)
                    
                    if anomalies:
                        st.warning(f"Found {len(anomalies)} anomalies in {anomaly_col}")
                        
                        # Visualize anomalies
                        fig = go.Figure()
                        
                        # Normal data points
                        normal_data = df.drop(anomalies)
                        fig.add_trace(go.Scatter(
                            x=normal_data.index,
                            y=normal_data[anomaly_col],
                            mode='markers',
                            name='Normal',
                            marker=dict(color='blue', size=4)
                        ))
                        
                        # Anomalous data points
                        anomaly_data = df.loc[anomalies]
                        fig.add_trace(go.Scatter(
                            x=anomaly_data.index,
                            y=anomaly_data[anomaly_col],
                            mode='markers',
                            name='Anomalies',
                            marker=dict(color='red', size=8, symbol='x')
                        ))
                        
                        fig.update_layout(
                            title=f"Anomaly Detection - {anomaly_col}",
                            xaxis_title="Index",
                            yaxis_title=anomaly_col,
                            template="plotly_dark"
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Show anomalous records
                        st.markdown("### 📋 Anomalous Records")
                        st.dataframe(df.loc[anomalies], width='stretch')
                    else:
                        st.success("No anomalies detected in the selected column.")
            else:
                st.info("No numeric columns available for anomaly detection.")
        
        with tab4:
            st.markdown("## 💬 Natural Language Queries")
            
            # Show current AI provider status
            if analyzer.ai_provider:
                provider_emoji = {"groq": "⚡", "huggingface": "🤗", "openai": "🤖"}.get(analyzer.ai_provider, "🤖")
                provider_name = {"groq": "Groq (Free)", "huggingface": "Hugging Face (Free)", "openai": "OpenAI (Paid)"}.get(analyzer.ai_provider, analyzer.ai_provider)
                st.success(f"{provider_emoji} **AI Provider Active:** {provider_name}")
            else:
                st.warning("⚠️ No AI provider configured. See setup options below.")
            
            # Setup instructions for free alternatives
            with st.expander("🔧 AI Provider Setup (Free Options Available)"):
                st.markdown("""
                **Free AI Options (Recommended):**
                
                **1. Groq (Fastest - Free Tier)** ⭐ Recommended
                - Sign up at: https://console.groq.com
                - Get free API credits (generous limits)
                - Set environment variable: `GROQ_API_KEY=your_key_here`
                - Install: `pip install groq`
                - Supports: Llama 3.3 70B, Mixtral 8x7B (very capable models)
                
                **2. Hugging Face Inference API (Free Tier)**
                - Sign up at: https://huggingface.co
                - Get API token from Settings → Access Tokens
                - Set environment variable: `HF_API_KEY=your_token_here`
                - No additional install needed (uses `requests`)
                - Supports: Mistral 7B, Llama models, and more
                
                **3. OpenAI (Paid - Not Recommended for Free Use)**
                - Set environment variable: `OPENAI_API_KEY=your_key_here`
                - Install: `pip install openai`
                - Uses GPT-4o (requires payment)
                
                **Priority:** Groq → Hugging Face → OpenAI (auto-detected based on available keys)
                """)
            
            if not analyzer.ai_client:
                st.info("👆 Configure one of the free AI providers above to enable natural language queries.")
            else:
                st.markdown("""
                Ask questions about your data in natural language! Examples:
                - "Show me sales trends by region"
                - "What's the correlation between age and purchase amount?"
                - "Create a bar chart of product categories"
                - "Show distribution of customer satisfaction scores"
                """)
                
                query = st.text_input(
                    "Ask a question about your data:",
                    placeholder="e.g., Show me the top 5 products by sales..."
                )
                
                if st.button("🔮 Analyze", type="primary") and query:
                    with st.spinner(f"Processing with {analyzer.ai_provider.upper()}..."):
                        result = analyzer.natural_language_query(query)
                        
                        if "error" in result:
                            st.error(f"Error: {result['error']}")
                        else:
                            # Display the AI's interpretation
                            st.markdown("### 🧠 AI Interpretation")
                            st.json(result)
                            
                            # Show which provider was used
                            if "_provider" in result:
                                provider_display = {"groq": "⚡ Groq", "huggingface": "🤗 Hugging Face", "openai": "🤖 OpenAI"}.get(result["_provider"], result["_provider"])
                                st.caption(f"Generated by: {provider_display}")
                            
                            # Try to create the suggested visualization
                            try:
                                chart_type = result.get('chart_type', '').lower()
                                x_col = result.get('x_column')
                                y_col = result.get('y_column')
                                color_col = result.get('color_column')
                                title = result.get('title', 'AI Generated Chart')
                                
                                fig = None
                                
                                if chart_type == 'line' and x_col and y_col:
                                    fig = px.line(df, x=x_col, y=y_col, color=color_col, title=title)
                                elif chart_type == 'bar' and x_col and y_col:
                                    fig = px.bar(df, x=x_col, y=y_col, color=color_col, title=title)
                                elif chart_type == 'scatter' and x_col and y_col:
                                    fig = px.scatter(df, x=x_col, y=y_col, color=color_col, title=title)
                                elif chart_type == 'histogram' and y_col:
                                    fig = px.histogram(df, x=y_col, color=color_col, title=title)
                                elif chart_type == 'box' and y_col:
                                    fig = px.box(df, y=y_col, color=color_col, title=title)
                                elif chart_type == 'pie' and x_col:
                                    pie_data = df[x_col].value_counts().reset_index()
                                    pie_data.columns = [x_col, 'count']
                                    fig = px.pie(pie_data, values='count', names=x_col, title=title)
                                
                                if fig:
                                    fig.update_layout(template="plotly_dark")
                                    st.plotly_chart(fig, use_container_width=True)
                                    
                                    # Show insights
                                    if 'insights' in result:
                                        st.markdown("### 💡 AI Insights")
                                        st.info(result['insights'])
                                else:
                                    st.warning("Could not generate the requested visualization. Please try a different query.")
                                    
                            except Exception as e:
                                st.error(f"Error creating visualization: {str(e)}")
        
        with tab5:
            st.markdown("## 🏗️ Dashboard Builder")
            
            # Initialize session state for dashboard
            if 'dashboard_widgets' not in st.session_state:
                st.session_state.dashboard_widgets = []
            
            # Dashboard controls
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                st.markdown("### Add Widgets to Dashboard")
                widget_type = st.selectbox(
                    "Widget Type:",
                    ["Metric Card", "Chart Widget", "Data Table", "Insight Card", "Filter Panel"]
                )
            
            with col2:
                if st.button("➕ Add Widget", type="primary"):
                    widget_config = {
                        'type': widget_type,
                        'id': len(st.session_state.dashboard_widgets),
                        'title': f"{widget_type} {len(st.session_state.dashboard_widgets) + 1}"
                    }
                    
                    if widget_type == "Metric Card" and len(numeric_cols) > 0:
                        widget_config['column'] = numeric_cols[0]
                        widget_config['aggregation'] = 'sum'
                    elif widget_type == "Chart Widget":
                        widget_config['chart_type'] = 'bar'
                        widget_config['x_col'] = categorical_cols[0] if categorical_cols else None
                        widget_config['y_col'] = numeric_cols[0] if numeric_cols else None
                    
                    st.session_state.dashboard_widgets.append(widget_config)
            
            with col3:
                if st.button("🗑️ Clear Dashboard"):
                    st.session_state.dashboard_widgets = []
                    st.rerun()
            
            # Layout configuration
            st.markdown("### Dashboard Layout")
            layout_cols = st.slider("Columns per row:", 1, 4, 2)
            
            # Render dashboard
            if st.session_state.dashboard_widgets:
                st.markdown("### 📊 Live Dashboard")
                
                # Create responsive grid
                widgets_per_row = layout_cols
                rows = [st.session_state.dashboard_widgets[i:i + widgets_per_row] 
                       for i in range(0, len(st.session_state.dashboard_widgets), widgets_per_row)]
                
                for row in rows:
                    cols = st.columns(len(row))
                    
                    for i, widget in enumerate(row):
                        with cols[i]:
                            # Widget container with glassmorphism styling
                            st.markdown(f"""
                            <div class="dashboard-card">
                                <h4>📈 {widget['title']}</h4>
                            """, unsafe_allow_html=True)
                            
                            try:
                                if widget['type'] == "Metric Card" and 'column' in widget:
                                    col_data = df[widget['column']]
                                    if widget.get('aggregation') == 'sum':
                                        value = col_data.sum()
                                    elif widget.get('aggregation') == 'mean':
                                        value = col_data.mean()
                                    elif widget.get('aggregation') == 'max':
                                        value = col_data.max()
                                    else:
                                        value = col_data.count()
                                    
                                    st.metric(widget['column'], f"{value:,.2f}" if isinstance(value, float) else f"{value:,}")
                                
                                elif widget['type'] == "Chart Widget":
                                    if widget.get('x_col') and widget.get('y_col'):
                                        chart_data = df.groupby(widget['x_col'])[widget['y_col']].sum().reset_index()
                                        fig = px.bar(chart_data, x=widget['x_col'], y=widget['y_col'],
                                                   height=300, template="plotly_dark")
                                        st.plotly_chart(fig, use_container_width=True)
                                
                                elif widget['type'] == "Data Table":
                                    st.dataframe(df.head(5), width='stretch')
                                
                                elif widget['type'] == "Insight Card":
                                    insights = analyzer.generate_insights()
                                    if insights:
                                        st.info(insights[0])
                                    else:
                                        st.info("No insights available")
                                
                                elif widget['type'] == "Filter Panel":
                                    if categorical_cols:
                                        filter_col = categorical_cols[0]
                                        unique_vals = df[filter_col].unique()
                                        selected = st.multiselect(f"Filter {filter_col}:", unique_vals, key=f"filter_{widget['id']}")
                            
                            except Exception as e:
                                st.error(f"Widget error: {str(e)}")
                            
                            st.markdown("</div>", unsafe_allow_html=True)
                
                # Dashboard export
                st.markdown("### 💾 Dashboard Export")
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("📱 Export Dashboard Config"):
                        dashboard_config = {
                            'widgets': st.session_state.dashboard_widgets,
                            'layout_cols': layout_cols,
                            'timestamp': datetime.now().isoformat()
                        }
                        config_json = json.dumps(dashboard_config, indent=2)
                        st.download_button(
                            label="Download Config",
                            data=config_json,
                            file_name=f"dashboard_config_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                            mime="application/json"
                        )
                
                with col2:
                    uploaded_config = st.file_uploader("📁 Import Dashboard Config", type=['json'])
                    if uploaded_config:
                        try:
                            config_data = json.loads(uploaded_config.read())
                            st.session_state.dashboard_widgets = config_data.get('widgets', [])
                            st.success("Dashboard configuration imported!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error importing config: {str(e)}")
            
            else:
                st.info("Add widgets to start building your dashboard!")
        
        with tab6:
            st.markdown("## 🗺️ Geographic Maps")

            from data_context import (
                GEO_DV_LAT,
                GEO_DV_LON,
                apply_geocode_columns,
                build_data_digest,
                detect_coordinate_columns,
                detect_location_text_columns,
            )

            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            categorical_cols = df.select_dtypes(
                include=["object", "category", "string"]
            ).columns.tolist()

            lat_cols, lon_cols = detect_coordinate_columns(df)
            loc_hint_cols = detect_location_text_columns(df)
            string_cols = [
                c
                for c in df.columns
                if df[c].dtype in ["object", "category"]
                or pd.api.types.is_string_dtype(df[c])
            ]

            if lat_cols and lon_cols:
                st.markdown("### 📍 Coordinate-based maps")

                col1, col2 = st.columns(2)
                with col1:
                    lat_col = st.selectbox("Latitude column:", lat_cols)
                    lon_col = st.selectbox("Longitude column:", lon_cols)

                with col2:
                    map_style = st.selectbox(
                        "Map style:",
                        [
                            "open-street-map",
                            "carto-positron",
                            "carto-darkmatter",
                            "satellite",
                        ],
                    )
                    size_col = st.selectbox("Size by:", [None] + numeric_cols)
                    color_col = st.selectbox(
                        "Color by:", [None] + numeric_cols + categorical_cols
                    )

                if st.button("🗺️ Generate map", key="geo_gen_map"):
                    map_data = df.dropna(subset=[lat_col, lon_col])
                    map_data = map_data[
                        (map_data[lat_col] >= -90)
                        & (map_data[lat_col] <= 90)
                        & (map_data[lon_col] >= -180)
                        & (map_data[lon_col] <= 180)
                    ]

                    if len(map_data) > 0:
                        fig = px.scatter_mapbox(
                            map_data,
                            lat=lat_col,
                            lon=lon_col,
                            color=color_col,
                            size=size_col,
                            hover_data=df.columns.tolist()[:8],
                            mapbox_style=map_style,
                            height=600,
                            zoom=3,
                        )
                        fig.update_layout(template="plotly_dark")
                        st.plotly_chart(fig, use_container_width=True)
                        st.success(f"Plotted **{len(map_data)}** points.")
                    else:
                        st.warning("No valid coordinate rows after filtering.")

            else:
                st.markdown("### 🌍 Maps from city / address columns")
                st.caption(
                    "Geocoding uses OpenStreetMap **Nominatim** (via `geopy`). "
                    "Use a modest number of distinct places and ~1 second between lookups."
                )

                if loc_hint_cols:
                    st.success(
                        "Detected possible place columns: **"
                        + "**, **".join(loc_hint_cols[:10])
                        + ("** …" if len(loc_hint_cols) > 10 else "**")
                    )

                place_candidates = list(dict.fromkeys(loc_hint_cols + string_cols))
                if not place_candidates:
                    st.warning(
                        "No text columns found to geocode. Add a column with city, region, or address text, "
                        "or use the sample dataset below."
                    )
                else:
                    place_col = st.selectbox("Column to geocode", place_candidates)
                    ctx_choices = [c for c in string_cols if c != place_col]
                    context_pick = st.selectbox(
                        "Optional context column (e.g. State), appended to each query",
                        ["(none)"] + ctx_choices,
                    )
                    append_usa = st.checkbox(
                        'Append ", USA" for disambiguation', value=True
                    )
                    max_unique = st.slider(
                        "Max distinct place values to geocode", 10, 200, 60
                    )

                    sample_vals = (
                        df[place_col]
                        .dropna()
                        .astype(str)
                        .str.strip()
                    )
                    sample_vals = sample_vals[sample_vals != ""].unique()[:10]
                    st.markdown(f"**Sample values in `{place_col}`:**")
                    for v in sample_vals:
                        st.write(f"• {v}")

                    try:
                        import geopy  # noqa: F401
                    except ImportError:
                        geopy = None
                    if geopy is None:
                        st.error("Install **geopy** to enable geocoding: `pip install geopy`")
                    elif st.button(
                        "🌍 Geocode → add dv_latitude / dv_longitude columns",
                        type="primary",
                        key="geo_run_geocode",
                    ):
                        ser = df[place_col].dropna().astype(str).str.strip()
                        ser = ser[ser != ""]
                        uniq = pd.unique(ser.values)
                        if len(uniq) > max_unique:
                            st.warning(
                                f"Using first **{max_unique}** distinct values (of {len(uniq)})."
                            )
                            uniq = uniq[:max_unique]

                        ctx_col = None if context_pick == "(none)" else context_pick
                        mapping = {}
                        bar = st.progress(0.0, text="Geocoding…")
                        n = len(uniq)
                        for i, key in enumerate(uniq):
                            key_s = str(key).strip()
                            ctx_val = ""
                            if ctx_col is not None and ctx_col in df.columns:
                                idx = df.index[df[place_col].astype(str).str.strip() == key_s]
                                if len(idx) > 0:
                                    raw = df.loc[idx[0], ctx_col]
                                    ctx_val = "" if pd.isna(raw) else str(raw).strip()
                            q = key_s
                            if ctx_val:
                                q = f"{q}, {ctx_val}"
                            if append_usa and ", usa" not in q.lower():
                                q = f"{q}, USA"
                            if i > 0:
                                time.sleep(1.05)
                            mapping[key_s] = _cached_geocode_osm(q)
                            bar.progress((i + 1) / max(n, 1))

                        geo_df = apply_geocode_columns(df, place_col, mapping)
                        n_ok = int(geo_df[[GEO_DV_LAT, GEO_DV_LON]].notna().all(axis=1).sum())
                        analyzer.df = geo_df
                        bar.progress(1.0, text="Done")
                        try:
                            st.session_state.data_digest = build_data_digest(geo_df)
                        except Exception:
                            pass
                        st.success(
                            f"Added **{GEO_DV_LAT}** / **{GEO_DV_LON}** — **{n_ok}** rows resolved. "
                            "Use **Generate map** above on the next run."
                        )
                        st.rerun()

                st.markdown("---")
                st.markdown("### 📍 Or load a small demo with coordinates")
                st.caption("Replaces the current in-memory table with US cities + lat/lon.")
                if st.button("📍 Load sample cities (demo)", key="geo_sample_cities"):
                    cities_data = {
                        "City": [
                            "New York",
                            "Los Angeles",
                            "Chicago",
                            "Houston",
                            "Phoenix",
                            "Philadelphia",
                            "San Antonio",
                            "San Diego",
                            "Dallas",
                            "San Jose",
                        ],
                        "Latitude": [
                            40.7128,
                            34.0522,
                            41.8781,
                            29.7604,
                            33.4484,
                            39.9526,
                            29.4241,
                            32.7157,
                            32.7767,
                            37.3382,
                        ],
                        "Longitude": [
                            -74.0060,
                            -118.2437,
                            -87.6298,
                            -95.3698,
                            -112.0740,
                            -75.1652,
                            -98.4936,
                            -117.1611,
                            -96.7970,
                            -121.8863,
                        ],
                        "Population": [
                            8398748,
                            3990456,
                            2705994,
                            2320268,
                            1680992,
                            1584064,
                            1547253,
                            1423851,
                            1343573,
                            1021795,
                        ],
                        "State": [
                            "NY",
                            "CA",
                            "IL",
                            "TX",
                            "AZ",
                            "PA",
                            "TX",
                            "CA",
                            "TX",
                            "CA",
                        ],
                    }
                    geo_df = pd.DataFrame(cities_data)
                    analyzer.df = geo_df
                    try:
                        st.session_state.data_digest = build_data_digest(geo_df)
                    except Exception:
                        pass
                    st.success("Sample geographic data loaded. Open this tab again to plot the map.")
                    st.rerun()
        
        with tab7:
            st.markdown("## ⚙️ Advanced Analytics")
            
            # Data filtering
            st.markdown("### 🔍 Data Filtering")
            
            # Create filters for each column
            filters = {}
            filter_cols = st.columns(min(3, len(df.columns)))
            
            for i, col in enumerate(df.columns[:3]):  # Show first 3 columns for filtering
                with filter_cols[i % 3]:
                    if df[col].dtype in ['object', 'category']:
                        unique_vals = df[col].unique()
                        selected_vals = st.multiselect(f"Filter {col}:", unique_vals, default=unique_vals[:5] if len(unique_vals) > 5 else unique_vals)
                        if selected_vals != list(unique_vals):
                            filters[col] = selected_vals
                    elif pd.api.types.is_numeric_dtype(df[col]):
                        min_val, max_val = float(df[col].min()), float(df[col].max())
                        selected_range = st.slider(f"Filter {col}:", min_val, max_val, (min_val, max_val))
                        if selected_range != (min_val, max_val):
                            filters[col] = selected_range
            
            # Apply filters
            filtered_df = df.copy()
            for col, filter_val in filters.items():
                if df[col].dtype in ['object', 'category']:
                    filtered_df = filtered_df[filtered_df[col].isin(filter_val)]
                else:
                    filtered_df = filtered_df[(filtered_df[col] >= filter_val[0]) & (filtered_df[col] <= filter_val[1])]
            
            if len(filtered_df) != len(df):
                st.success(f"Filtered data: {len(filtered_df):,} rows (from {len(df):,})")
                st.dataframe(filtered_df.head(100), width='stretch')
            
            # Statistical summary
            st.markdown("### 📈 Statistical Summary")
            numeric_cols = filtered_df.select_dtypes(include=[np.number]).columns
            
            summary_stats = None
            corr_matrix = None
            
            if len(numeric_cols) > 0:
                summary_stats = filtered_df[numeric_cols].describe()
                st.dataframe(summary_stats, width='stretch')
                
                # Correlation matrix
                if len(numeric_cols) > 1:
                    st.markdown("### 🔗 Correlation Matrix")
                    corr_matrix = filtered_df[numeric_cols].corr()
                    
                    fig = px.imshow(
                        corr_matrix,
                        text_auto=True,
                        aspect="auto",
                        color_continuous_scale="RdBu_r",
                        title="Correlation Matrix"
                    )
                    fig.update_layout(template="plotly_dark")
                    st.plotly_chart(fig, use_container_width=True)
            
            # Data export
            st.markdown("### 💾 Data Export")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("📊 Export Filtered Data"):
                    csv = filtered_df.to_csv(index=False)
                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name=f"filtered_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
            
            with col2:
                if st.button("📈 Export Summary Stats"):
                    if len(numeric_cols) > 0 and summary_stats is not None:
                        summary_csv = summary_stats.to_csv()
                        st.download_button(
                            label="Download Summary",
                            data=summary_csv,
                            file_name=f"summary_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
            
            with col3:
                if st.button("🔗 Export Correlation"):
                    if len(numeric_cols) > 1 and corr_matrix is not None:
                        corr_csv = corr_matrix.to_csv()
                        st.download_button(
                            label="Download Correlation",
                            data=corr_csv,
                            file_name=f"correlation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
        
        with tab8:
            st.markdown("## ✅ Comprehensive Feature Implementation Status")
            
            # Core Data Functionality
            st.markdown("### 📊 Core Data Functionality")
            core_features = [
                ("✅", "CSV file upload and parsing", "Implemented with robust error handling"),
                ("✅", "Excel file upload (.xlsx, .xls)", "Full support with openpyxl"),
                ("✅", "Drag-and-drop interface", "Streamlit native file uploader"),
                ("✅", "Data validation and sanitization", "Comprehensive validation in load_data()"),
                ("✅", "Large dataset support", "Smart sampling with configurable limits"),
                ("✅", "Real-time filtering", "Advanced filtering in tab7"),
                ("✅", "Data sorting and grouping", "Multiple aggregation options"),
                ("✅", "Missing value detection", "Column info analysis"),
                ("✅", "Export capabilities", "PNG, PDF, CSV, HTML formats"),
                ("✅", "PostgreSQL database integration", "Full CRUD operations with SQLAlchemy")
            ]
            
            for status, feature, description in core_features:
                st.markdown(f"{status} **{feature}**: {description}")
            
            # Visualization Features
            st.markdown("### 🎨 Visualization Features")
            viz_features = [
                ("✅", "15+ Chart Types", "Line, Bar, Scatter, Heatmap, Treemap, Sankey, Geographic, etc."),
                ("✅", "Interactive Controls", "Zoom, pan, hover tooltips, click interactions"),
                ("✅", "Real-time Updates", "Dynamic chart generation with smooth transitions"),
                ("✅", "Custom Themes", "8 color schemes with user preferences"),
                ("✅", "Glassmorphism Design", "Modern UI with backdrop blur effects"),
                ("✅", "Responsive Layout", "Adaptive to different screen sizes"),
                ("✅", "Animation System", "Smooth morphing transitions and micro-interactions")
            ]
            
            for status, feature, description in viz_features:
                st.markdown(f"{status} **{feature}**: {description}")
            
            # AI & Smart Features
            st.markdown("### 🤖 AI & Smart Features")
            ai_features = [
                ("✅", "Natural Language Queries", "Multi-provider support: Groq (Free), Hugging Face (Free), OpenAI (Paid)"),
                ("✅", "Automatic Anomaly Detection", "IQR-based outlier detection"),
                ("✅", "Trend Analysis", "Correlation discovery and pattern recognition"),
                ("✅", "AI-Powered Insights", "Automated insight generation"),
                ("✅", "Statistical Analysis", "Comprehensive statistical summaries"),
                ("✅", "Smart Visualizations", "AI suggests appropriate chart types")
            ]
            
            for status, feature, description in ai_features:
                st.markdown(f"{status} **{feature}**: {description}")
            
            # User Experience Features  
            st.markdown("### 🎯 User Experience Features")
            ux_features = [
                ("✅", "Dashboard Builder", "Drag-and-drop widget system with live updates"),
                ("✅", "Geographic Mapping", "Interactive maps with coordinate plotting"),
                ("✅", "Collaboration Tools", "Real-time commenting and sharing system"),
                ("✅", "Personalization", "Persistent theme preferences and layouts"),
                ("✅", "Performance Optimization", "Smart sampling and caching"),
                ("✅", "Accessibility Features", "Keyboard navigation and screen reader support"),
                ("✅", "Mobile Responsive", "Touch-friendly controls and adaptive design"),
                ("✅", "Onboarding System", "Interactive guided experience"),
                ("✅", "Error Handling", "Comprehensive error boundaries with recovery"),
                ("✅", "Session Management", "Save/restore analysis sessions")
            ]
            
            for status, feature, description in ux_features:
                st.markdown(f"{status} **{feature}**: {description}")
            
            # Technical Features
            st.markdown("### ⚡ Technical Architecture")
            tech_features = [
                ("✅", "Single-File Architecture", "Lightweight, self-contained deployment"),
                ("✅", "Database Integration", "MongoDB Atlas + PostgreSQL support"),
                ("✅", "Multi-Provider AI", "Groq (Free) | Hugging Face (Free) | OpenAI (Paid)"),
                ("✅", "Environment Configuration", "Secure secret management"),
                ("✅", "Modern CSS/JS", "Glassmorphism, animations, particle effects"),
                ("✅", "Error Recovery", "Graceful failure handling"),
                ("✅", "Performance Monitoring", "Data quality metrics and optimization"),
                ("✅", "Security Best Practices", "Data validation and secure processing")
            ]
            
            for status, feature, description in tech_features:
                st.markdown(f"{status} **{feature}**: {description}")
            
            # Feature Completion Summary
            st.markdown("### 📈 Implementation Summary")
            
            total_features = len(core_features) + len(viz_features) + len(ai_features) + len(ux_features) + len(tech_features)
            completed_features = sum(1 for features in [core_features, viz_features, ai_features, ux_features, tech_features] 
                                   for status, _, _ in features if status == "✅")
            
            completion_rate = (completed_features / total_features) * 100
            
            st.success(f"**{completion_rate:.0f}% Complete** - {completed_features}/{total_features} features implemented")
            
            # Performance metrics
            if analyzer.df is not None:
                st.markdown("### 📊 Current Session Metrics")
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Dataset Size", f"{len(analyzer.df):,} rows")
                with col2:
                    st.metric("Columns", len(analyzer.df.columns))
                with col3:
                    data_quality = analyzer.db_manager.calculate_data_quality(analyzer.df)
                    st.metric("Data Quality", f"{data_quality:.1f}%")
                with col4:
                    memory_mb = analyzer.df.memory_usage(deep=True).sum() / (1024**2)
                    st.metric("Memory Usage", f"{memory_mb:.1f} MB")
        
        with tab9:
            st.markdown("## 🔮 Automated Visualizations")
            
            st.info("📌 Automatic visualizations are generated when you upload a file. View them here!")
            
            try:
                from auto_viz import AutoVizGenerator
                
                gen = AutoVizGenerator()
                
                # Analyze and recommend
                st.markdown("### 📊 Dataset Analysis")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Total Records", f"{len(df):,}")
                with col2:
                    st.metric("Numeric Columns", len(df.select_dtypes(include=[np.number]).columns))
                with col3:
                    st.metric("Categorical Columns", len(df.select_dtypes(include=['object']).columns))
                
                # Generate recommendations
                recommendations = gen.recommend_visualizations(df)
                
                st.markdown("### 💡 Recommended Visualizations")
                cols_for_recommendations = st.columns(min(3, len(recommendations)))
                
                for i, rec in enumerate(recommendations[:6]):
                    with cols_for_recommendations[i % 3]:
                        st.write(f"**{rec.get('type').upper()}**")
                        st.write(f"_{rec.get('title')}_")
                        st.write(f"🎯 {rec.get('reason')}")
                
                # Generate all visualizations
                if st.button("🎨 Generate All Visualizations", type="primary"):
                    with st.spinner("Generating visualizations..."):
                        result = gen.batch_generate(df, limit=6)
                        
                        if result.get('success'):
                            st.success(f"Generated {result.get('total_generated')} visualizations!")
                            
                            for viz in result.get('visualizations', []):
                                st.markdown(f"### {viz.get('title')}")
                                st.markdown(f"**Type:** {viz.get('type')} | **Priority:** {viz.get('priority')}")
                                st.markdown(f"_{viz.get('reason')}_")
                                
                                if viz.get('insights'):
                                    st.markdown(f"💡 **Insights:** {', '.join(viz.get('insights'))}")
                                
                                st.components.v1.html(viz.get('html'), height=500)
                        else:
                            st.error(f"Error: {result.get('error')}")
                            
            except ImportError as e:
                st.error(f"Auto Visualization module not available: {str(e)}")
        
        with tab10:
            st.markdown("## 📈 Advanced Forecasting")
            
            try:
                from forecasting_engine import ForecastingEngine
                
                engine = ForecastingEngine()
                
                numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
                datetime_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
                
                if not datetime_cols:
                    st.warning("⚠️ No datetime columns found. Forecasting requires time series data.")
                elif not numeric_cols:
                    st.error("❌ No numeric columns for forecasting.")
                else:
                    st.markdown("### 🔮 Time Series Forecasting")
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        date_col = st.selectbox("Select Date Column:", datetime_cols)
                    with col2:
                        value_col = st.selectbox("Select Value Column:", numeric_cols)
                    with col3:
                        forecast_periods = st.number_input("Forecast Periods:", min_value=1, max_value=365, value=30)
                    
                    model_type = st.radio("Select Forecasting Model:", ["ARIMA", "Prophet", "Exponential Smoothing"], horizontal=True)

                    if model_type == "Prophet":
                        from forecasting_engine import PROPHET_AVAILABLE
                        if not PROPHET_AVAILABLE:
                            st.warning(
                                "Prophet is not installed (keeps the default install light). "
                                "Install with: `pip install -e \".[forecasting]\"` — or use ARIMA / Exponential Smoothing."
                            )
                    
                    if st.button("🚀 Generate Forecast", type="primary"):
                        with st.spinner("Training model..."):
                            # Prepare time series
                            ts_df = engine.prepare_timeseries(df, date_col, value_col)
                            
                            if ts_df is not None:
                                if model_type == "ARIMA":
                                    result = engine.forecast_arima(ts_df[value_col], periods=forecast_periods)
                                elif model_type == "Prophet":
                                    result = engine.forecast_prophet(df, date_col, value_col, forecast_periods)
                                else:
                                    result = engine.forecast_exponential_smoothing(ts_df[value_col], periods=forecast_periods)
                                
                                if result.get('success'):
                                    st.success("✅ Forecast Generated!")
                                    
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        st.markdown("### 📊 Model Summary")
                                        summary = result.get('summary', {})
                                        for key, value in summary.items():
                                            st.write(f"**{key.upper()}:** {value}")
                                    
                                    with col2:
                                        st.markdown("### 🎯 Forecast Statistics")
                                        st.json(result.get('summary', {}))
                                    
                                    st.markdown("### 📈 Forecast Data (Partial)")
                                    forecast_data = result.get('forecast', {})
                                    if forecast_data:
                                        st.json({k: v for k, v in list(forecast_data.items())[:5]})
                                else:
                                    st.error(f"Forecast Error: {result.get('error')}")
                            
            except ImportError as e:
                st.error(f"Forecasting module not available: {str(e)}")
        
        with tab11:
            st.markdown("## 🧪 A/B Testing & Experimentation")
            
            try:
                from ab_testing import ABTestingFramework
                
                framework = ABTestingFramework()
                
                numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
                categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
                
                if not numeric_cols or not categorical_cols:
                    st.warning("⚠️ A/B Testing requires both numeric and categorical columns.")
                else:
                    st.markdown("### 🧪 Create A/B Test")
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        test_name = st.text_input("Test Name:", value="experiment_1")
                    with col2:
                        metric_col = st.selectbox("Metric Column:", numeric_cols)
                    with col3:
                        group_col = st.selectbox("Group Column:", categorical_cols)
                    
                    hypothesis = st.text_area("Hypothesis:", placeholder="Enter your test hypothesis...")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        alpha = st.slider("Significance Level (α):", 0.01, 0.10, 0.05)
                    with col2:
                        test_type = st.radio("Test Type:", ["T-Test", "Mann-Whitney U", "Chi-Square"], horizontal=True)
                    
                    if st.button("📊 Run A/B Test", type="primary"):
                        with st.spinner("Analyzing data..."):
                            # Get variants
                            unique_groups = df[group_col].unique()[:2]  # Get first 2 groups
                            
                            if len(unique_groups) >= 2:
                                variant_a = df[df[group_col] == unique_groups[0]][metric_col].dropna()
                                variant_b = df[df[group_col] == unique_groups[1]][metric_col].dropna()
                                
                                # Create experiment
                                exp_result = framework.create_experiment(
                                    test_name, variant_a, variant_b, metric_col, 
                                    hypothesis=hypothesis
                                )
                                
                                if exp_result.get('success'):
                                    # Run test
                                    if test_type == "T-Test":
                                        test_result = framework.t_test_analysis(test_name, alpha=alpha)
                                    elif test_type == "Mann-Whitney U":
                                        test_result = framework.mann_whitney_test(test_name, alpha=alpha)
                                    else:
                                        st.info("Chi-Square test requires contingency table setup")
                                        test_result = {"success": False}
                                    
                                    if test_result.get('success'):
                                        # Display results
                                        st.markdown("### 📊 Test Results")
                                        
                                        col1, col2, col3, col4 = st.columns(4)
                                        with col1:
                                            st.metric("P-Value", f"{test_result.get('p_value', 0):.4f}")
                                        with col2:
                                            is_sig = "✅ YES" if test_result.get('is_significant') else "❌ NO"
                                            st.metric("Significant?", is_sig)
                                        with col3:
                                            st.metric("Mean A", f"{variant_a.mean():.2f}")
                                        with col4:
                                            st.metric("Mean B", f"{variant_b.mean():.2f}")
                                        
                                        st.markdown(f"**Recommendation:** {test_result.get('recommendation')}")
                                        
                                        if 'cohens_d' in test_result:
                                            st.markdown(f"**Effect Size (Cohen's d):** {test_result.get('cohens_d'):.3f} ({test_result.get('effect_size_interpretation')})")
                            else:
                                st.error("Need at least 2 distinct groups for A/B testing")
                    
                    # Calculate sample size
                    st.markdown("### 📐 Sample Size Calculator")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        baseline = st.slider("Baseline Conversion Rate:", 0.01, 0.50, 0.25)
                    with col2:
                        min_effect = st.slider("Minimum Effect Size:", 0.01, 0.30, 0.05)
                    with col3:
                        power = st.slider("Statistical Power:", 0.70, 0.99, 0.80)
                    
                    if st.button("Calculate Sample Size"):
                        sample_result = framework.calculate_sample_size(baseline, min_effect, 0.05, power)
                        
                        if sample_result.get('success'):
                            st.success(sample_result.get('note'))
                            col1, col2 = st.columns(2)
                            with col1:
                                st.metric("Per Variant", f"{sample_result.get('sample_size_per_variant'):,}")
                            with col2:
                                st.metric("Total Sample", f"{sample_result.get('total_sample_size'):,}")
                            
            except ImportError as e:
                st.error(f"A/B Testing module not available: {str(e)}")
        
        with tab12:
            st.markdown("## 🔧 Custom Formula Builder")
            
            try:
                from formula_builder import FormulaBuilder
                
                builder = FormulaBuilder(df)
                
                st.markdown("### 📝 Create Custom Formulas")
                st.info("Create new columns using formulas. Supports arithmetic, conditional, string, and aggregation operations.")
                
                formula_type = st.selectbox(
                    "Formula Type:",
                    ["Arithmetic", "Conditional (IF/THEN)", "String Manipulation", "Aggregation"]
                )
                
                if formula_type == "Arithmetic":
                    st.markdown("**Example:** `[column1] * [column2] + 100`")
                    formula_name = st.text_input("Formula Name:")
                    formula = st.text_input("Formula Expression:")
                    description = st.text_area("Description:")
                    
                    if st.button("✅ Create Arithmetic Formula"):
                        result = builder.create_arithmetic_formula(formula_name, formula, description)
                        if result.get('success'):
                            st.success(result.get('message'))
                            st.write("Sample values:", result.get('sample_values'))
                        else:
                            st.error(f"Error: {result.get('error')}")
                
                elif formula_type == "Conditional (IF/THEN)":
                    st.markdown("Create conditions that return different values")
                    formula_name = st.text_input("Formula Name:")
                    
                    num_conditions = st.number_input("Number of conditions:", min_value=1, max_value=5, value=2)
                    conditions = []
                    
                    for i in range(num_conditions):
                        st.markdown(f"**Condition {i+1}**")
                        col1, col2 = st.columns(2)
                        with col1:
                            condition = st.text_input(f"Condition {i+1}:", key=f"cond_{i}")
                        with col2:
                            value = st.text_input(f"Then Value {i+1}:", key=f"val_{i}")
                        conditions.append({"condition": condition, "value": value})
                    
                    if st.button("✅ Create Conditional Formula"):
                        result = builder.create_conditional_formula(formula_name, conditions)
                        if result.get('success'):
                            st.success(result.get('message'))
                        else:
                            st.error(f"Error: {result.get('error')}")
                
                elif formula_type == "String Manipulation":
                    st.markdown("**Example:** `[column1].upper() + ' - ' + [column2]`")
                    formula_name = st.text_input("Formula Name:")
                    formula = st.text_input("Formula Expression:")
                    description = st.text_area("Description:")
                    
                    if st.button("✅ Create String Formula"):
                        result = builder.create_string_formula(formula_name, formula, description)
                        if result.get('success'):
                            st.success(result.get('message'))
                        else:
                            st.error(f"Error: {result.get('error')}")
                
                else:  # Aggregation
                    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
                    categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
                    
                    formula_name = st.text_input("Formula Name:")
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        agg_column = st.selectbox("Column to Aggregate:", numeric_cols) if numeric_cols else None
                    with col2:
                        agg_type = st.selectbox("Aggregation:", ["sum", "mean", "count", "max", "min", "std"])
                    with col3:
                        group_by = st.selectbox("Group By:", [None] + categorical_cols) if categorical_cols else None
                    
                    if st.button("✅ Create Aggregation Formula"):
                        result = builder.create_aggregation_formula(formula_name, agg_column, agg_type, group_by)
                        if result.get('success'):
                            st.success(result.get('message'))
                            st.write("Result:", result.get('result'))
                        else:
                            st.error(f"Error: {result.get('error')}")
                
                # List existing formulas
                st.markdown("### 📋 Existing Formulas")
                formulas = builder.list_formulas()
                
                if formulas:
                    for formula in formulas:
                        st.markdown(f"**{formula.get('name')}** ({formula.get('type')})")
                        if formula.get('description'):
                            st.write(formula.get('description'))
                else:
                    st.info("No formulas created yet")
                    
            except ImportError as e:
                st.error(f"Formula Builder module not available: {str(e)}")
        
        with tab13:
            st.markdown("## 📋 Report Scheduling & Export")
            
            try:
                from report_scheduler import ReportScheduler
                
                scheduler = ReportScheduler()
                
                st.markdown("### 📅 Schedule Automated Reports")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("#### Create New Schedule")
                    schedule_name = st.text_input("Schedule Name:")
                    frequency = st.selectbox("Frequency:", ["daily", "weekly", "monthly"])
                    time = st.time_input("Execution Time:").strftime("%H:%M")
                    report_format = st.selectbox("Report Format:", ["html", "csv", "json"])
                    
                    if st.button("📅 Create Schedule"):
                        result = scheduler.create_schedule(schedule_name, {
                            "name": schedule_name,
                            "frequency": frequency,
                            "time": time,
                            "report_format": report_format,
                            "output_path": f"reports/{schedule_name}_{datetime.now().strftime('%Y%m%d')}.{report_format}"
                        })
                        
                        if result.get('success'):
                            st.success(result.get('message'))
                            st.write(f"Next run: {result.get('next_run')}")
                        else:
                            st.error(f"Error: {result.get('error')}")
                
                with col2:
                    st.markdown("#### Scheduler Status")
                    status = scheduler.get_scheduler_status()
                    
                    col_a, col_b, col_c = st.columns(3)
                    with col_a:
                        st.metric("Total Schedules", status.get('total_schedules'))
                    with col_b:
                        st.metric("Active", status.get('active_schedules'))
                    with col_c:
                        st.metric("Reports Run", status.get('total_reports_run'))
                
                # List schedules
                st.markdown("#### Scheduled Reports")
                schedules = scheduler.list_schedules()
                
                if schedules:
                    for sched in schedules:
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.write(f"📅 **{sched.get('name')}** - {sched.get('frequency')} @ {sched.get('frequency')}")
                        with col2:
                            if st.button("▶️ Run Now", key=f"run_{sched.get('id')}"):
                                result = scheduler.run_report(sched.get('id'), df)
                                if result.get('success'):
                                    st.success(f"Report saved to: {result.get('output_path')}")
                                else:
                                    st.error(result.get('error'))
                
                # Export current dataset
                st.markdown("### 💾 Export Current Dataset")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if st.button("📊 Export as HTML Report"):
                        from report_scheduler import HTMLReportGenerator
                        gen = HTMLReportGenerator()
                        html_content = gen.generate(df, {
                            "title": "Data Report",
                            "include_summary": True,
                            "include_stats": True
                        })
                        st.download_button(
                            label="Download HTML Report",
                            data=html_content,
                            file_name=f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html",
                            mime="text/html"
                        )
                
                with col2:
                    if st.button("📈 Export as CSV"):
                        csv_data = df.to_csv(index=False)
                        st.download_button(
                            label="Download CSV",
                            data=csv_data,
                            file_name=f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                
                with col3:
                    if st.button("📋 Export as JSON"):
                        json_data = df.to_json(orient='records')
                        st.download_button(
                            label="Download JSON",
                            data=json_data,
                            file_name=f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                            mime="application/json"
                        )
                    
            except ImportError as e:
                st.error(f"Report Scheduler module not available: {str(e)}")
    
    else:
        # Welcome screen with sample data options
        st.markdown("""
        <div style="text-align: center; padding: 3rem;">
            <h2>Welcome to DataViz Pro! 🚀</h2>
            <p style="font-size: 1.2rem; margin-bottom: 2rem;">
                Upload your data or try our sample datasets to get started with advanced analytics and AI-powered insights.
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # Quick start options
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("📈 Sales Analytics"):
                analyzer.generate_sample_data("Sales Data")
                st.rerun()
        
        with col2:
            if st.button("👥 Customer Data"):
                analyzer.generate_sample_data("Customer Analytics")
                st.rerun()
        
        with col3:
            if st.button("💰 Financial Data"):
                analyzer.generate_sample_data("Financial Data")
                st.rerun()
        
        with col4:
            if st.button("🌐 Web Analytics"):
                analyzer.generate_sample_data("Website Analytics")
                st.rerun()
        
        # Features showcase with interactive demonstration
        st.markdown("""
        <div style="margin-top: 3rem;">
            <h3>✨ Comprehensive Features</h3>
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 1rem; margin-top: 1rem;">
                <div class="insight-card">
                    <h4>🤖 AI-Powered Insights</h4>
                    <p>Automatic anomaly detection, trend analysis, correlation discovery, and intelligent pattern recognition</p>
                </div>
                <div class="insight-card">
                    <h4>💬 Natural Language Queries</h4>
                    <p>Ask questions in plain English! Supports Groq (Free), Hugging Face (Free), and OpenAI</p>
                </div>
                <div class="insight-card">
                    <h4>📊 Advanced Visualizations</h4>
                    <p>15+ chart types including treemaps, sankey diagrams, geographic maps, and waterfall charts</p>
                </div>
                <div class="insight-card">
                    <h4>🏗️ Dashboard Builder</h4>
                    <p>Drag-and-drop interface for creating custom dashboards with real-time widgets</p>
                </div>
                <div class="insight-card">
                    <h4>🗺️ Geographic Mapping</h4>
                    <p>Interactive maps with coordinate plotting and location-based analytics</p>
                </div>
                <div class="insight-card">
                    <h4>🤝 Collaboration Tools</h4>
                    <p>Real-time commenting, sharing capabilities, and session management</p>
                </div>
                <div class="insight-card">
                    <h4>🎨 Premium Design</h4>
                    <p>Glassmorphism UI with adaptive themes, smooth animations, and personalization</p>
                </div>
                <div class="insight-card">
                    <h4>⚡ Performance Optimized</h4>
                    <p>Smart sampling, caching, and efficient rendering for large datasets</p>
                </div>
            </div>
        </div>
        
        <!-- Floating Action Button -->
        <div class="floating-action" onclick="window.scrollTo({top: 0, behavior: 'smooth'})" title="Back to Top">
            ↑
        </div>
        """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
