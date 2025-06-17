
# DataViz Pro - Advanced Analytics Dashboard

A powerful data visualization and analytics platform with AI-powered insights, built with Streamlit and modern web technologies.

![DataViz Pro](https://img.shields.io/badge/DataViz-Pro-blue?style=for-the-badge&logo=chart.js)
![Python](https://img.shields.io/badge/Python-3.11+-green?style=for-the-badge&logo=python)
![Streamlit](https://img.shields.io/badge/Streamlit-1.45+-red?style=for-the-badge&logo=streamlit)

## 🚀 Features

- **15+ Chart Types**: Line, Bar, Scatter, Heatmap, Treemap, Sankey, Geographic Maps, and more
- **AI-Powered Insights**: OpenAI GPT-4o integration for natural language queries
- **Dashboard Builder**: Drag-and-drop widget system with real-time updates
- **Geographic Mapping**: Interactive maps with coordinate plotting
- **Collaboration Tools**: Real-time commenting and sharing capabilities
- **Advanced Analytics**: Anomaly detection, trend analysis, correlation discovery
- **Modern UI**: Glassmorphism design with smooth animations
- **Database Support**: MongoDB Atlas and PostgreSQL integration
- **Performance Optimized**: Smart sampling and caching for large datasets

## 📋 Prerequisites

- Python 3.11 or higher
- pip package manager
- (Optional) MongoDB Atlas account for database features
- (Optional) OpenAI API key for AI features
- (Optional) PostgreSQL database for alternative storage

## 🛠️ Installation & Setup

### 1. Clone/Download the Project

```bash
# If using git
git clone <your-repository-url>
cd dataviz-pro

# Or download and extract the ZIP file
```

### 2. Install Dependencies

```bash
# Install all required packages
pip install streamlit pandas numpy plotly pymongo dnspython openpyxl scikit-learn scipy openai psycopg2-binary
```

**Or using the project file:**
```bash
pip install -e .
```

### 3. Environment Configuration

Create a `.env` file in the project root directory:

```bash
cp .env.example .env
```

Edit the `.env` file with your actual credentials (see Environment Variables section below).

### 4. Run the Application

```bash
# Start the Streamlit server
streamlit run app.py --server.port 5000
```

The application will be available at: `http://localhost:5000`

## 🔧 Environment Variables

All environment variables are optional. The application will work with basic functionality even without them.

### Database Configuration (Optional)

**MongoDB Atlas (Recommended):**
```bash
MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/dataviz_pro?retryWrites=true&w=majority
MONGODB_DB_NAME=dataviz_pro
```

**PostgreSQL (Alternative):**
```bash
DATABASE_URL=postgresql://username:password@hostname:port/database_name
```

### AI Features (Optional)

**OpenAI Integration:**
```bash
OPENAI_API_KEY=sk-your-openai-api-key-here
```

### Server Configuration (Optional)

```bash
# Custom port (default: 8501)
STREAMLIT_SERVER_PORT=5000

# Server address (use 0.0.0.0 for external access)
STREAMLIT_SERVER_ADDRESS=0.0.0.0
```

## 🗄️ Database Setup

### MongoDB Atlas (Recommended)

1. Create a free account at [MongoDB Atlas](https://www.mongodb.com/atlas)
2. Create a new cluster
3. Set up database access (username/password)
4. Configure network access (allow your IP or 0.0.0.0/0 for testing)
5. Get your connection string and add it to `.env`

### PostgreSQL (Alternative)

1. Install PostgreSQL locally or use a cloud service
2. Create a new database
3. Add the connection URL to `.env`
4. Tables will be created automatically on first run

## 🚀 Deployment Options

### Deploy on Replit (Recommended)

1. Fork this Repl or import your code
2. Set environment variables in Replit Secrets:
   - `MONGODB_URI`
   - `OPENAI_API_KEY`
3. Click the "Run" button
4. Your app will be live at your Replit URL

### Local Development

```bash
# Development mode with auto-reload
streamlit run app.py --server.port 5000 --server.runOnSave true
```

### Production Deployment

```bash
# Production mode
streamlit run app.py --server.port 5000 --server.headless true
```

## 📁 Project Structure

```
dataviz-pro/
├── app.py                 # Main application file
├── pyproject.toml         # Python dependencies
├── .replit               # Replit configuration
├── .env.example          # Environment variables template
├── README.md             # This file
└── .streamlit/
    └── config.toml       # Streamlit configuration
```

## 🎯 Usage Guide

### 1. Data Import
- **Upload Files**: Drag and drop CSV or Excel files
- **Sample Data**: Choose from 4 pre-built datasets
- **Database**: Load previously saved datasets

### 2. Visualization
- Select from 15+ chart types
- Customize colors, themes, and layouts
- Export charts as PNG, HTML, or PDF

### 3. AI Features
- **Natural Language**: Ask questions like "Show sales trends by region"
- **Auto Insights**: Get AI-generated insights about your data
- **Anomaly Detection**: Automatically find outliers and anomalies

### 4. Dashboard Building
- Drag and drop widgets
- Create custom layouts
- Export and import dashboard configurations

### 5. Collaboration
- Add comments and notes
- Share analysis sessions
- Export complete reports

## 🔧 Customization

### Themes and Styling
The app uses a modern glassmorphism design. You can customize:
- Color schemes (8 built-in options)
- Chart heights and layouts
- Animation settings
- Grid visibility

### Adding New Chart Types
To add new visualizations, modify the chart generation logic in the "Visualizations" tab section of `app.py`.

### Database Schema
The app uses flexible document/table structures:
- **datasets**: Store dataset metadata
- **analyses**: Save chart configurations
- **comments**: User collaboration data

## 🐛 Troubleshooting

### Common Issues

**Port Already in Use:**
```bash
# Change port in command
streamlit run app.py --server.port 8502
```

**Missing Dependencies:**
```bash
# Reinstall all packages
pip install --force-reinstall -r requirements.txt
```

**Database Connection Failed:**
- Check your connection string format
- Verify network access and firewall settings
- Ensure database credentials are correct

**OpenAI API Errors:**
- Verify your API key is valid and has credits
- Check rate limits and usage quotas

### Performance Optimization

For large datasets (>50k rows):
- Use the sampling feature in visualizations
- Enable data caching in performance settings
- Consider filtering data before analysis

## 📊 Feature Status

- ✅ **Core Data Functionality**: Complete
- ✅ **15+ Visualization Types**: Complete  
- ✅ **AI-Powered Insights**: Complete
- ✅ **Dashboard Builder**: Complete
- ✅ **Geographic Mapping**: Complete
- ✅ **Collaboration Tools**: Complete
- ✅ **Database Integration**: Complete
- ✅ **Modern UI/UX**: Complete

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

- **Documentation**: This README and inline help tooltips
- **Sample Data**: Built-in datasets for testing
- **Error Handling**: Comprehensive error messages and recovery
- **Community**: Share your dashboards and get help

## 🔮 Roadmap

- [ ] Real-time data streaming
- [ ] Advanced ML model integration
- [ ] Custom plugin system
- [ ] Team workspace features
- [ ] Advanced security controls

---

**Built with ❤️ using Streamlit, Plotly, and modern web technologies**
