import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
from plotly.subplots import make_subplots
import json
import os
import io
import base64
from datetime import datetime, timedelta
import re
from typing import Dict, List, Any, Optional
import warnings
warnings.filterwarnings('ignore')

# OpenAI integration
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

# Page configuration
st.set_page_config(
    page_title="DataViz Pro - Advanced Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for glassmorphism and modern design
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 15px;
        margin-bottom: 2rem;
        color: white;
        text-align: center;
        backdrop-filter: blur(10px);
        box-shadow: 0 8px 32px 0 rgba(31, 38, 135, 0.37);
    }
    
    .metric-card {
        background: rgba(255, 255, 255, 0.1);
        backdrop-filter: blur(10px);
        border-radius: 15px;
        padding: 1.5rem;
        margin: 1rem 0;
        border: 1px solid rgba(255, 255, 255, 0.2);
        box-shadow: 0 8px 32px 0 rgba(31, 38, 135, 0.37);
    }
    
    .insight-card {
        background: linear-gradient(135deg, rgba(0, 212, 255, 0.1), rgba(118, 75, 162, 0.1));
        backdrop-filter: blur(10px);
        border-radius: 15px;
        padding: 1.5rem;
        margin: 1rem 0;
        border: 1px solid rgba(0, 212, 255, 0.3);
        box-shadow: 0 8px 32px 0 rgba(0, 212, 255, 0.2);
    }
    
    .stSelectbox > div > div {
        background: rgba(255, 255, 255, 0.1);
        backdrop-filter: blur(10px);
        border-radius: 10px;
    }
    
    .uploadedFile {
        background: rgba(0, 212, 255, 0.1);
        border-radius: 10px;
        padding: 1rem;
    }
</style>
""", unsafe_allow_html=True)

class DataAnalyzer:
    def __init__(self):
        self.df = None
        self.openai_client = None
        if OPENAI_AVAILABLE:
            api_key = os.getenv("OPENAI_API_KEY")
            if api_key:
                self.openai_client = OpenAI(api_key=api_key)
    
    def load_data(self, uploaded_file) -> pd.DataFrame:
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
            
            # Convert date columns
            for col in df.columns:
                if df[col].dtype == 'object':
                    # Try to convert to datetime
                    try:
                        df[col] = pd.to_datetime(df[col], errors='ignore')
                    except:
                        pass
            
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
        """Process natural language queries using OpenAI"""
        if not self.openai_client or self.df is None:
            return {"error": "OpenAI not available or no data loaded"}
        
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

            # the newest OpenAI model is "gpt-4o" which was released May 13, 2024.
            # do not change this unless explicitly requested by the user
            response = self.openai_client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": query}
                ],
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            return result
            
        except Exception as e:
            return {"error": f"Failed to process query: {str(e)}"}

# Initialize the analyzer
@st.cache_resource
def get_analyzer():
    return DataAnalyzer()

analyzer = get_analyzer()

# Main application
def main():
    # Header
    st.markdown("""
    <div class="main-header">
        <h1>📊 DataViz Pro</h1>
        <p>Advanced Analytics Dashboard with AI-Powered Insights</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.markdown("### 🎛️ Control Panel")
        
        # Data source selection
        data_source = st.radio(
            "Choose Data Source:",
            ["Upload File", "Sample Dataset"],
            help="Upload your own data or use sample datasets"
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
        else:
            sample_type = st.selectbox(
                "Select Sample Dataset:",
                ["Sales Data", "Customer Analytics", "Financial Data", "Website Analytics"]
            )
            
            if st.button("Load Sample Data"):
                with st.spinner("Generating sample data..."):
                    df = analyzer.generate_sample_data(sample_type)
                    st.success(f"✅ Generated {len(df)} rows, {len(df.columns)} columns")
        
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
    
    # Main content area
    if analyzer.df is not None:
        df = analyzer.df
        
        # Create tabs for different views
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "🔍 Overview", "📊 Visualizations", "🤖 AI Insights", "💬 Natural Language", "⚙️ Advanced"
        ])
        
        with tab1:
            st.markdown("## 📈 Data Overview")
            
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
            st.dataframe(df.head(100), use_container_width=True)
            
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
            st.dataframe(stats_df, use_container_width=True)
        
        with tab2:
            st.markdown("## 📊 Interactive Visualizations")
            
            # Chart type selection
            chart_type = st.selectbox(
                "Select Chart Type:",
                ["Line Chart", "Bar Chart", "Scatter Plot", "Heatmap", "Histogram", "Box Plot", "Pie Chart"]
            )
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Column selection based on chart type
                numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
                categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
                date_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
                all_cols = df.columns.tolist()
                
                if chart_type in ["Line Chart", "Bar Chart", "Scatter Plot"]:
                    x_col = st.selectbox("X-axis:", all_cols)
                    y_col = st.selectbox("Y-axis:", numeric_cols)
                    color_col = st.selectbox("Color by:", [None] + categorical_cols)
                elif chart_type == "Heatmap":
                    if len(numeric_cols) >= 2:
                        selected_cols = st.multiselect("Select columns for heatmap:", numeric_cols, default=numeric_cols[:5])
                    else:
                        st.warning("Need at least 2 numeric columns for heatmap")
                        selected_cols = []
                elif chart_type in ["Histogram", "Box Plot"]:
                    y_col = st.selectbox("Column to analyze:", numeric_cols)
                    color_col = st.selectbox("Group by:", [None] + categorical_cols)
                elif chart_type == "Pie Chart":
                    category_col = st.selectbox("Category column:", categorical_cols)
                    value_col = st.selectbox("Value column (optional):", [None] + numeric_cols)
            
            with col2:
                # Chart customization
                st.markdown("### 🎨 Customization")
                
                # Color scheme
                color_scheme = st.selectbox(
                    "Color Scheme:",
                    ["plotly", "viridis", "plasma", "inferno", "magma", "cividis"]
                )
                
                # Chart size
                chart_height = st.slider("Chart Height:", 300, 800, 500)
                
                # Aggregation for grouped data
                if chart_type in ["Bar Chart", "Line Chart"]:
                    agg_func = st.selectbox("Aggregation:", ["sum", "mean", "count", "max", "min"])
            
            # Generate visualization
            try:
                fig = None
                
                if chart_type == "Line Chart" and x_col and y_col:
                    if color_col:
                        fig = px.line(df, x=x_col, y=y_col, color=color_col, 
                                    color_discrete_sequence=px.colors.qualitative.Set3)
                    else:
                        fig = px.line(df, x=x_col, y=y_col)
                
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
                
                elif chart_type == "Pie Chart" and category_col:
                    if value_col:
                        pie_data = df.groupby(category_col)[value_col].sum().reset_index()
                        fig = px.pie(pie_data, values=value_col, names=category_col,
                                   color_discrete_sequence=px.colors.qualitative.Set3)
                    else:
                        pie_data = df[category_col].value_counts().reset_index()
                        pie_data.columns = [category_col, 'count']
                        fig = px.pie(pie_data, values='count', names=category_col,
                                   color_discrete_sequence=px.colors.qualitative.Set3)
                
                if fig:
                    fig.update_layout(
                        height=chart_height,
                        template="plotly_dark",
                        showlegend=True,
                        margin=dict(l=0, r=0, t=50, b=0)
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    
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
                        st.dataframe(df.loc[anomalies], use_container_width=True)
                    else:
                        st.success("No anomalies detected in the selected column.")
            else:
                st.info("No numeric columns available for anomaly detection.")
        
        with tab4:
            st.markdown("## 💬 Natural Language Queries")
            
            if not OPENAI_AVAILABLE:
                st.warning("⚠️ OpenAI integration not available. Install openai package to use this feature.")
            elif not analyzer.openai_client:
                st.warning("⚠️ OpenAI API key not found. Set OPENAI_API_KEY environment variable to use this feature.")
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
                    with st.spinner("Processing your query..."):
                        result = analyzer.natural_language_query(query)
                        
                        if "error" in result:
                            st.error(f"Error: {result['error']}")
                        else:
                            # Display the AI's interpretation
                            st.markdown("### 🧠 AI Interpretation")
                            st.json(result)
                            
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
                st.dataframe(filtered_df.head(100), use_container_width=True)
            
            # Statistical summary
            st.markdown("### 📈 Statistical Summary")
            numeric_cols = filtered_df.select_dtypes(include=[np.number]).columns
            
            if len(numeric_cols) > 0:
                summary_stats = filtered_df[numeric_cols].describe()
                st.dataframe(summary_stats, use_container_width=True)
                
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
                    if len(numeric_cols) > 0:
                        summary_csv = summary_stats.to_csv()
                        st.download_button(
                            label="Download Summary",
                            data=summary_csv,
                            file_name=f"summary_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
            
            with col3:
                if st.button("🔗 Export Correlation"):
                    if len(numeric_cols) > 1:
                        corr_csv = corr_matrix.to_csv()
                        st.download_button(
                            label="Download Correlation",
                            data=corr_csv,
                            file_name=f"correlation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
    
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
            if st.button("📈 Sales Analytics", use_container_width=True):
                analyzer.generate_sample_data("Sales Data")
                st.rerun()
        
        with col2:
            if st.button("👥 Customer Data", use_container_width=True):
                analyzer.generate_sample_data("Customer Analytics")
                st.rerun()
        
        with col3:
            if st.button("💰 Financial Data", use_container_width=True):
                analyzer.generate_sample_data("Financial Data")
                st.rerun()
        
        with col4:
            if st.button("🌐 Web Analytics", use_container_width=True):
                analyzer.generate_sample_data("Website Analytics")
                st.rerun()
        
        # Features showcase
        st.markdown("""
        <div style="margin-top: 3rem;">
            <h3>✨ Features</h3>
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 1rem; margin-top: 1rem;">
                <div class="insight-card">
                    <h4>🤖 AI-Powered Insights</h4>
                    <p>Automatic anomaly detection, trend analysis, and correlation discovery</p>
                </div>
                <div class="insight-card">
                    <h4>💬 Natural Language Queries</h4>
                    <p>Ask questions in plain English and get instant visualizations</p>
                </div>
                <div class="insight-card">
                    <h4>📊 Interactive Charts</h4>
                    <p>Multiple chart types with real-time filtering and customization</p>
                </div>
                <div class="insight-card">
                    <h4>🎨 Modern Design</h4>
                    <p>Glassmorphism UI with adaptive color schemes and smooth animations</p>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
