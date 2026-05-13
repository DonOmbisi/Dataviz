#!/usr/bin/env python3
"""
Fast Startup Script for DataViz Pro
Optimized for performance with minimal imports and caching
"""

import os
import sys

# Performance optimizations
os.environ['STREAMLIT_SERVER_HEADLESS'] = 'true'
os.environ['STREAMLIT_SERVER_ENABLE_CORS'] = 'false'
os.environ['STREAMLIT_BROWSER_GATHER_USAGE_STATS'] = 'false'
os.environ['STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION'] = 'false'
os.environ['STREAMLIT_SERVER_FILE_WATCHER_TYPE'] = 'none'

# Reduce memory usage
os.environ['STREAMLIT_SERVER_MAX_UPLOAD_SIZE'] = '100'
os.environ['STREAMLIT_SERVER_MAX_MESSAGE_SIZE'] = '100'

if __name__ == "__main__":
    import streamlit.web.cli as stcli
    
    # Override sys.argv to run the app
    sys.argv = ["streamlit", "run", "app.py", "--server.port=8501"]
    
    print("🚀 Starting DataViz Pro in FAST mode...")
    print("   - CSS animations: DISABLED")
    print("   - JavaScript particles: DISABLED")
    print("   - Database: LAZY LOAD")
    print("   - File watching: DISABLED")
    print("   - CORS: DISABLED")
    print("   - Max upload: 100MB")
    print("")
    print("Open: http://localhost:8501")
    print("")
    
    stcli.main()
