"""
Automated Visualization Generator
Automatically creates optimal visualizations based on data characteristics
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple, Optional
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings

warnings.filterwarnings('ignore')


class AutoVizGenerator:
    """Automatically generate visualizations from data"""
    
    def __init__(self):
        self.analysis_results = {}
        self.generated_charts = {}
    
    def analyze_dataset(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze dataset structure and characteristics"""
        
        try:
            analysis = {
                "total_rows": len(df),
                "total_columns": len(df.columns),
                "numeric_columns": list(df.select_dtypes(include=[np.number]).columns),
                "categorical_columns": list(df.select_dtypes(include=['object', 'category']).columns),
                "datetime_columns": list(df.select_dtypes(include=['datetime64']).columns),
                "columns_with_nulls": list(df.columns[df.isnull().any()]),
                "null_percentages": (df.isnull().sum() / len(df) * 100).to_dict(),
                "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024**2
            }
            
            # Detailed column analysis
            analysis["column_details"] = {}
            for col in df.columns:
                col_dtype = str(df[col].dtype)
                analysis["column_details"][col] = {
                    "dtype": col_dtype,
                    "unique_count": df[col].nunique(),
                    "null_count": df[col].isnull().sum(),
                    "sample_values": df[col].dropna().head(3).tolist()
                }
                
                # Add numeric statistics
                if col in analysis["numeric_columns"]:
                    analysis["column_details"][col].update({
                        "min": float(df[col].min()),
                        "max": float(df[col].max()),
                        "mean": float(df[col].mean()),
                        "median": float(df[col].median()),
                        "std": float(df[col].std())
                    })
            
            return analysis
            
        except Exception as e:
            return {"error": str(e)}
    
    def recommend_visualizations(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Recommend visualizations based on data characteristics"""
        
        recommendations = []
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
        datetime_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
        
        # 1. Time series chart if datetime column exists
        if datetime_cols and numeric_cols:
            recommendations.append({
                "type": "line",
                "title": f"Time Series: {numeric_cols[0]} over time",
                "x_column": datetime_cols[0],
                "y_column": numeric_cols[0],
                "priority": "high",
                "reason": "Datetime and numeric columns detected - ideal for time series"
            })
        
        # 2. Distribution analysis
        if len(numeric_cols) > 0:
            recommendations.append({
                "type": "histogram",
                "title": f"Distribution: {numeric_cols[0]}",
                "x_column": numeric_cols[0],
                "priority": "high",
                "reason": "Showing distribution of numeric column"
            })
        
        # 3. Correlation heatmap
        if len(numeric_cols) >= 2:
            recommendations.append({
                "type": "heatmap",
                "title": "Correlation Matrix",
                "columns": numeric_cols[:10],  # Limit to 10 columns
                "priority": "high",
                "reason": f"Multiple numeric columns ({len(numeric_cols)}) - correlation analysis"
            })
        
        # 4. Categorical analysis
        if len(categorical_cols) > 0 and len(numeric_cols) > 0:
            recommendations.append({
                "type": "bar",
                "title": f"{categorical_cols[0]} vs {numeric_cols[0]}",
                "x_column": categorical_cols[0],
                "y_column": numeric_cols[0],
                "priority": "high",
                "reason": "Categorical vs numeric - bar chart for comparison"
            })
        
        # 5. Box plot for outlier detection
        if len(numeric_cols) > 0 and len(categorical_cols) > 0:
            recommendations.append({
                "type": "box",
                "title": f"Outlier Detection: {numeric_cols[0]} by {categorical_cols[0]}",
                "y_column": numeric_cols[0],
                "x_column": categorical_cols[0],
                "priority": "medium",
                "reason": "Box plot for outlier and distribution analysis"
            })
        
        # 6. Scatter plot
        if len(numeric_cols) >= 2:
            recommendations.append({
                "type": "scatter",
                "title": f"{numeric_cols[0]} vs {numeric_cols[1]}",
                "x_column": numeric_cols[0],
                "y_column": numeric_cols[1],
                "priority": "medium",
                "reason": "Relationship between two numeric variables"
            })
        
        # 7. Pie chart for categorical breakdown
        if len(categorical_cols) > 0:
            recommendations.append({
                "type": "pie",
                "title": f"Distribution of {categorical_cols[0]}",
                "category_column": categorical_cols[0],
                "priority": "medium",
                "reason": "Categorical distribution visualization"
            })
        
        # 8. Multi-dimensional analysis
        if len(numeric_cols) >= 3 and len(categorical_cols) > 0:
            recommendations.append({
                "type": "scatter_3d",
                "title": "3D Scatter Plot Analysis",
                "x_column": numeric_cols[0],
                "y_column": numeric_cols[1],
                "z_column": numeric_cols[2],
                "color_column": categorical_cols[0] if categorical_cols else None,
                "priority": "low",
                "reason": "Multi-dimensional analysis with 3+ numeric columns"
            })
        
        return sorted(recommendations, key=lambda x: {"high": 0, "medium": 1, "low": 2}.get(x.get("priority", "low")))
    
    def generate_automatic_dashboard(self, df: pd.DataFrame, dashboard_name: str = "Auto Dashboard") -> Dict[str, Any]:
        """Generate a complete dashboard with multiple visualizations"""
        
        try:
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
            
            figures = []
            
            # 1. Summary metrics
            if numeric_cols:
                metrics_fig = go.Figure()
                for col in numeric_cols[:5]:  # Limit to 5 columns
                    metrics_fig.add_trace(go.Indicator(
                        mode="number+delta",
                        value=df[col].mean(),
                        title={"text": f"Mean {col}"},
                        domain={'x': [0, 1], 'y': [0, 1]}
                    ))
                figures.append({"name": "Summary Metrics", "figure": metrics_fig})
            
            # 2. Main distribution chart
            if numeric_cols:
                dist_fig = px.histogram(
                    df,
                    x=numeric_cols[0],
                    title=f"Distribution of {numeric_cols[0]}",
                    nbins=30,
                    template="plotly_dark"
                )
                figures.append({"name": "Distribution", "figure": dist_fig})
            
            # 3. Correlation heatmap
            if len(numeric_cols) >= 2:
                corr_matrix = df[numeric_cols].corr()
                corr_fig = px.imshow(
                    corr_matrix,
                    title="Correlation Matrix",
                    color_continuous_scale="RdBu",
                    template="plotly_dark"
                )
                figures.append({"name": "Correlations", "figure": corr_fig})
            
            # 4. Categorical analysis
            if categorical_cols and numeric_cols:
                cat_fig = px.bar(
                    df.groupby(categorical_cols[0])[numeric_cols[0]].sum().reset_index(),
                    x=categorical_cols[0],
                    y=numeric_cols[0],
                    title=f"{categorical_cols[0]} Analysis",
                    template="plotly_dark"
                )
                figures.append({"name": "Category Analysis", "figure": cat_fig})
            
            # 5. Time series if available
            datetime_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
            if datetime_cols and numeric_cols:
                ts_fig = px.line(
                    df,
                    x=datetime_cols[0],
                    y=numeric_cols[0],
                    title="Time Series Analysis",
                    template="plotly_dark"
                )
                figures.append({"name": "Time Series", "figure": ts_fig})
            
            return {
                "success": True,
                "dashboard_name": dashboard_name,
                "total_visualizations": len(figures),
                "visualizations": [{"name": f["name"], "html": f["figure"].to_html()} for f in figures],
                "analysis": self.analyze_dataset(df)
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def generate_chart(self, df: pd.DataFrame, chart_config: Dict[str, Any]) -> Optional[go.Figure]:
        """Generate a single chart based on configuration"""
        
        try:
            chart_type = chart_config.get("type")
            title = chart_config.get("title", "Chart")
            
            if chart_type == "line":
                fig = px.line(
                    df,
                    x=chart_config.get("x_column"),
                    y=chart_config.get("y_column"),
                    color=chart_config.get("color_column"),
                    title=title,
                    template="plotly_dark"
                )
            
            elif chart_type == "bar":
                fig = px.bar(
                    df,
                    x=chart_config.get("x_column"),
                    y=chart_config.get("y_column"),
                    color=chart_config.get("color_column"),
                    title=title,
                    template="plotly_dark"
                )
            
            elif chart_type == "scatter":
                fig = px.scatter(
                    df,
                    x=chart_config.get("x_column"),
                    y=chart_config.get("y_column"),
                    color=chart_config.get("color_column"),
                    size=chart_config.get("size_column"),
                    title=title,
                    template="plotly_dark"
                )
            
            elif chart_type == "histogram":
                fig = px.histogram(
                    df,
                    x=chart_config.get("x_column"),
                    title=title,
                    template="plotly_dark"
                )
            
            elif chart_type == "box":
                fig = px.box(
                    df,
                    y=chart_config.get("y_column"),
                    x=chart_config.get("x_column"),
                    title=title,
                    template="plotly_dark"
                )
            
            elif chart_type == "heatmap":
                columns = chart_config.get("columns", df.select_dtypes(include=[np.number]).columns.tolist())
                corr_matrix = df[columns].corr()
                fig = px.imshow(
                    corr_matrix,
                    title=title,
                    template="plotly_dark",
                    color_continuous_scale="RdBu"
                )
            
            elif chart_type == "pie":
                fig = px.pie(
                    df,
                    names=chart_config.get("category_column"),
                    values=chart_config.get("value_column"),
                    title=title,
                    template="plotly_dark"
                )
            
            elif chart_type == "scatter_3d":
                fig = px.scatter_3d(
                    df,
                    x=chart_config.get("x_column"),
                    y=chart_config.get("y_column"),
                    z=chart_config.get("z_column"),
                    color=chart_config.get("color_column"),
                    title=title,
                    template="plotly_dark"
                )
            
            else:
                return None
            
            # Apply styling
            fig.update_layout(
                height=chart_config.get("height", 500),
                template="plotly_dark",
                hovermode="x unified"
            )
            
            return fig
            
        except Exception as e:
            return None
    
    def generate_insights_from_viz(self, df: pd.DataFrame, chart_config: Dict) -> List[str]:
        """Generate text insights from visualization configuration"""
        
        insights = []
        
        try:
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            
            if chart_config.get("type") == "histogram":
                col = chart_config.get("x_column")
                if col in numeric_cols:
                    insights.append(f"Mean: {df[col].mean():.2f}, Median: {df[col].median():.2f}, Std Dev: {df[col].std():.2f}")
            
            elif chart_config.get("type") == "bar":
                x_col = chart_config.get("x_column")
                y_col = chart_config.get("y_column")
                if y_col in numeric_cols:
                    top_group = df.groupby(x_col)[y_col].sum().idxmax()
                    insights.append(f"Top group: {top_group}")
            
            elif chart_config.get("type") == "scatter":
                x_col = chart_config.get("x_column")
                y_col = chart_config.get("y_column")
                if x_col in numeric_cols and y_col in numeric_cols:
                    corr = df[x_col].corr(df[y_col])
                    insights.append(f"Correlation: {corr:.3f}")
            
        except Exception:
            pass
        
        return insights
    
    def batch_generate(self, df: pd.DataFrame, limit: int = 10) -> Dict[str, Any]:
        """Generate multiple visualizations at once"""
        
        try:
            recommendations = self.recommend_visualizations(df)[:limit]
            generated = []
            
            for rec in recommendations:
                chart_config = {
                    "type": rec.get("type"),
                    "title": rec.get("title"),
                    "x_column": rec.get("x_column"),
                    "y_column": rec.get("y_column"),
                    "color_column": rec.get("color_column"),
                    "category_column": rec.get("category_column"),
                    "value_column": rec.get("value_column"),
                    "columns": rec.get("columns")
                }
                
                fig = self.generate_chart(df, chart_config)
                
                if fig:
                    generated.append({
                        "title": rec.get("title"),
                        "type": rec.get("type"),
                        "priority": rec.get("priority"),
                        "reason": rec.get("reason"),
                        "html": fig.to_html(),
                        "insights": self.generate_insights_from_viz(df, chart_config)
                    })
            
            return {
                "success": True,
                "total_generated": len(generated),
                "visualizations": generated
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
