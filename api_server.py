"""
FastAPI Layer for Programmatic Access
RESTful API for all DataViz Pro features
Run separately: uvicorn api_server:app --host 0.0.0.0 --port 8000
"""

from fastapi import FastAPI, HTTPException, File, UploadFile, Query
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
import pandas as pd
import json
from datetime import datetime
import io
import os

# Create FastAPI app
app = FastAPI(
    title="DataViz Pro API",
    description="RESTful API for advanced data visualization and analytics",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== Pydantic Models ====================

class DataUploadRequest(BaseModel):
    """Request model for data upload"""
    name: str = Field(..., description="Dataset name")
    description: Optional[str] = Field(None, description="Dataset description")

class ColumnInfo(BaseModel):
    """Column information"""
    name: str
    dtype: str
    null_count: int
    unique_values: int

class DatasetInfo(BaseModel):
    """Dataset information"""
    id: str
    name: str
    rows: int
    columns: int
    created_at: str
    columns_info: Optional[List[ColumnInfo]] = None

class ChartRequest(BaseModel):
    """Request model for chart generation"""
    chart_type: str = Field(..., description="Type of chart (line, bar, scatter, etc.)")
    x_column: str = Field(..., description="X-axis column")
    y_column: str = Field(..., description="Y-axis column")
    color_column: Optional[str] = Field(None, description="Column for color encoding")
    title: Optional[str] = Field(None, description="Chart title")
    height: Optional[int] = Field(500, description="Chart height in pixels")

class ForecastRequest(BaseModel):
    """Request model for forecasting"""
    date_column: str = Field(..., description="Date/timestamp column")
    value_column: str = Field(..., description="Value column to forecast")
    model_type: str = Field("arima", description="Model type: arima, prophet, exponential_smoothing")
    periods: int = Field(30, description="Number of periods to forecast")
    order: Optional[tuple] = Field(None, description="ARIMA order (p, d, q)")

class ABTestRequest(BaseModel):
    """Request model for A/B testing"""
    test_name: str = Field(..., description="Name of the A/B test")
    metric_column: str = Field(..., description="Column containing metric values")
    group_column: str = Field(..., description="Column containing group assignments (A/B)")
    hypothesis: Optional[str] = Field(None, description="Test hypothesis")

class FormulaRequest(BaseModel):
    """Request model for formula creation"""
    name: str = Field(..., description="Formula name")
    formula: str = Field(..., description="Formula expression")
    description: Optional[str] = Field(None, description="Formula description")

class ReportScheduleRequest(BaseModel):
    """Request model for scheduling reports"""
    name: str = Field(..., description="Schedule name")
    frequency: str = Field("daily", description="Frequency: daily, weekly, monthly")
    time: str = Field("09:00", description="Time in HH:MM format")
    format: str = Field("html", description="Report format: html, csv, json")

# ==================== In-Memory Data Storage ====================

datasets = {}  # Store uploaded datasets
dataset_counter = 0

# Streaming state must persist across requests; FastAPI endpoints create/execute per-request,
# so we keep a single in-process manager.
from streaming_engine import StreamingDataManager

_streaming_manager = StreamingDataManager()

# =============== WebSocket collaboration (MVP) ===============
from fastapi import WebSocket
import asyncio

# Connected WS clients (process-local, in-memory)
_ws_clients: set[WebSocket] = set()

async def _broadcast_ws_event(message: Dict[str, Any]) -> None:
    dead: List[WebSocket] = []
    for ws in list(_ws_clients):
        try:
            await ws.send_json(message)
        except Exception:
            dead.append(ws)
    for ws in dead:
        _ws_clients.discard(ws)

@app.websocket("/api/ws/collab")
async def collab_ws(websocket: WebSocket):
    """
    MVP real-time collaboration channel.
    Client sends: {"type":"chat"|"state_update", ...}
    Server broadcasts every received message to all clients.
    """
    await websocket.accept()
    _ws_clients.add(websocket)
    try:
        # Send a hello so UI can confirm connectivity.
        await websocket.send_json({"type": "hello", "connected": True})
        while True:
            payload = await websocket.receive_json()
            # Normalize and tag with server-side metadata.
            payload = dict(payload)
            payload.setdefault("server_ts", datetime.now().isoformat())
            await _broadcast_ws_event(payload)
    except Exception:
        # Client disconnected.
        pass
    finally:
        _ws_clients.discard(websocket)

# ==================== Health & Info Endpoints ====================

@app.get("/api/health", tags=["Health"])
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }

@app.get("/api/info", tags=["Info"])
async def get_api_info():
    """Get API information and capabilities"""
    return {
        "api_name": "DataViz Pro API",
        "version": "1.0.0",
        "features": [
            "Data Upload and Management",
            "Visualization Generation",
            "Advanced Forecasting (ARIMA, Prophet)",
            "A/B Testing",
            "Formula Builder",
            "Report Scheduling",
            "Real-time Streaming"
        ],
        "supported_formats": ["CSV", "Excel", "JSON", "Parquet"],
        "authentication": "Optional API key header"
    }

# ==================== Data Management Endpoints ====================

@app.post("/api/data/upload", response_model=DatasetInfo, tags=["Data Management"])
async def upload_data(file: UploadFile = File(...), name: str = Query(...)):
    """Upload and store a dataset"""
    global dataset_counter
    
    try:
        # Read file
        content = await file.read()
        
        if file.filename.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(content))
        elif file.filename.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(io.BytesIO(content))
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format")
        
        # Store dataset
        dataset_id = f"dataset_{dataset_counter}"
        dataset_counter += 1
        
        datasets[dataset_id] = {
            "id": dataset_id,
            "name": name,
            "df": df,
            "created_at": datetime.now().isoformat(),
            "rows": len(df),
            "columns": len(df.columns)
        }
        
        return DatasetInfo(
            id=dataset_id,
            name=name,
            rows=len(df),
            columns=len(df.columns),
            created_at=datasets[dataset_id]["created_at"]
        )
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/data/list", tags=["Data Management"])
async def list_datasets():
    """List all available datasets"""
    return {
        "datasets": [
            {
                "id": ds["id"],
                "name": ds["name"],
                "rows": ds["rows"],
                "columns": ds["columns"],
                "created_at": ds["created_at"]
            }
            for ds in datasets.values()
        ],
        "total": len(datasets)
    }

@app.get("/api/data/{dataset_id}", tags=["Data Management"])
async def get_dataset_info(dataset_id: str):
    """Get information about a specific dataset"""
    
    if dataset_id not in datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    ds = datasets[dataset_id]
    df = ds["df"]
    
    columns_info = []
    for col in df.columns:
        columns_info.append(ColumnInfo(
            name=col,
            dtype=str(df[col].dtype),
            null_count=int(df[col].isnull().sum()),
            unique_values=int(df[col].nunique())
        ))
    
    return DatasetInfo(
        id=dataset_id,
        name=ds["name"],
        rows=ds["rows"],
        columns=ds["columns"],
        created_at=ds["created_at"],
        columns_info=columns_info
    )

@app.get("/api/data/{dataset_id}/preview", tags=["Data Management"])
async def preview_dataset(dataset_id: str, rows: int = Query(10, ge=1, le=1000)):
    """Preview dataset rows"""
    
    if dataset_id not in datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    df = datasets[dataset_id]["df"]
    preview_data = df.head(rows).to_dict(orient='records')
    
    return {
        "dataset_id": dataset_id,
        "rows_shown": len(preview_data),
        "data": preview_data
    }

@app.delete("/api/data/{dataset_id}", tags=["Data Management"])
async def delete_dataset(dataset_id: str):
    """Delete a dataset"""
    
    if dataset_id not in datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    del datasets[dataset_id]
    return {"message": f"Dataset {dataset_id} deleted"}

# ==================== Analysis Endpoints ====================

@app.post("/api/analysis/summary", tags=["Analysis"])
async def get_summary_statistics(dataset_id: str = Query(...)):
    """Get summary statistics for a dataset"""
    
    if dataset_id not in datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    df = datasets[dataset_id]["df"]
    
    numeric_cols = df.select_dtypes(include=['number']).columns
    summary = df[numeric_cols].describe().to_dict()
    
    return {
        "dataset_id": dataset_id,
        "summary": summary
    }

@app.post("/api/analysis/correlation", tags=["Analysis"])
async def get_correlation_matrix(dataset_id: str = Query(...)):
    """Get correlation matrix for numeric columns"""
    
    if dataset_id not in datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    df = datasets[dataset_id]["df"]
    numeric_cols = df.select_dtypes(include=['number']).columns
    correlation = df[numeric_cols].corr().to_dict()
    
    return {
        "dataset_id": dataset_id,
        "correlation_matrix": correlation
    }

@app.post("/api/analysis/filter", tags=["Analysis"])
async def filter_dataset(dataset_id: str = Query(...), 
                        column: str = Query(...),
                        value: str = Query(...)):
    """Filter dataset by column value"""
    
    if dataset_id not in datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    df = datasets[dataset_id]["df"]
    
    if column not in df.columns:
        raise HTTPException(status_code=400, detail="Column not found")
    
    filtered_df = df[df[column] == value]
    
    return {
        "dataset_id": dataset_id,
        "filter": {f"{column}": value},
        "rows_matched": len(filtered_df),
        "data": filtered_df.head(100).to_dict(orient='records')
    }

# ==================== Visualization Endpoints ====================

@app.post("/api/visualization/chart", tags=["Visualization"])
async def generate_chart(dataset_id: str = Query(...), request: ChartRequest = None):
    """Generate a chart"""
    
    if dataset_id not in datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    df = datasets[dataset_id]["df"]
    
    try:
        import plotly.express as px
        
        kwargs = {
            "data_frame": df,
            "x": request.x_column,
            "y": request.y_column,
            "title": request.title or f"{request.chart_type} Chart"
        }
        
        if request.color_column:
            kwargs["color"] = request.color_column
        
        if request.chart_type == "line":
            fig = px.line(**kwargs)
        elif request.chart_type == "bar":
            fig = px.bar(**kwargs)
        elif request.chart_type == "scatter":
            fig = px.scatter(**kwargs)
        elif request.chart_type == "histogram":
            fig = px.histogram(df, x=request.x_column, color=request.color_column) if request.color_column else px.histogram(df, x=request.x_column)
        elif request.chart_type == "box":
            fig = px.box(df, x=request.x_column, y=request.y_column, color=request.color_column) if request.color_column else px.box(df, x=request.x_column, y=request.y_column)
        elif request.chart_type == "heatmap":
            # Expect x_column=y axis variable (values) isn't sufficient; treat heatmap as correlation by default.
            # If y_column is provided and exists, build a pivot heatmap from x/y/value columns.
            if request.y_column and request.x_column and request.y_column in df.columns and request.x_column in df.columns:
                # If the user provided a numeric y_column, try as a correlation heatmap.
                if pd.api.types.is_numeric_dtype(df[request.y_column]) and pd.api.types.is_numeric_dtype(df[request.x_column]):
                    corr = df[[request.x_column, request.y_column]].corr()
                    fig = px.imshow(corr, text_auto=True)
                else:
                    # Fallback pivot: x_column -> columns, y_column -> rows, and use first numeric col as values
                    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
                    if not numeric_cols:
                        raise HTTPException(status_code=400, detail="Heatmap requires at least one numeric column")
                    value_col = numeric_cols[0]
                    pivot = df.pivot_table(index=request.y_column, columns=request.x_column, values=value_col, aggfunc="mean")
                    fig = px.imshow(pivot, aspect="auto", text_auto=False)
            else:
                # Correlation heatmap for all numeric columns
                numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
                if len(numeric_cols) < 2:
                    raise HTTPException(status_code=400, detail="Heatmap requires at least two numeric columns")
                corr = df[numeric_cols].corr()
                fig = px.imshow(corr, text_auto=True, aspect="auto")
        elif request.chart_type == "pie":
            # For pie: x_column is category, y_column (optional) is value
            if not request.y_column:
                counts = df[request.x_column].value_counts().reset_index()
                counts.columns = [request.x_column, "count"]
                fig = px.pie(counts, names=request.x_column, values="count")
            else:
                agg = df.groupby(request.x_column)[request.y_column].sum().reset_index()
                fig = px.pie(agg, names=request.x_column, values=request.y_column)
        else:
            raise HTTPException(status_code=400, detail="Unsupported chart type")
        
        fig.update_layout(height=request.height)
        html = fig.to_html()
        
        return {
            "success": True,
            "chart_type": request.chart_type,
            "html": html
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==================== Forecasting Endpoints ====================

@app.post("/api/forecast/arima", tags=["Forecasting"])
async def forecast_arima(dataset_id: str = Query(...), request: ForecastRequest = None):
    """Generate ARIMA forecast"""
    
    if dataset_id not in datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    try:
        from forecasting_engine import ForecastingEngine
        
        df = datasets[dataset_id]["df"]
        engine = ForecastingEngine()
        
        ts_df = engine.prepare_timeseries(df, request.date_column, request.value_column)
        result = engine.forecast_arima(ts_df[request.value_column], periods=request.periods)
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/forecast/prophet", tags=["Forecasting"])
async def forecast_prophet(dataset_id: str = Query(...), request: ForecastRequest = None):
    """Generate Prophet forecast"""
    
    if dataset_id not in datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    try:
        from forecasting_engine import ForecastingEngine
        
        df = datasets[dataset_id]["df"]
        engine = ForecastingEngine()
        
        result = engine.forecast_prophet(df, request.date_column, request.value_column, request.periods)
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==================== A/B Testing Endpoints ====================

@app.post("/api/experiment/create", tags=["A/B Testing"])
async def create_experiment(dataset_id: str = Query(...), request: ABTestRequest = None):
    """Create and run A/B test"""
    
    if dataset_id not in datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    try:
        from ab_testing import ABTestingFramework
        
        df = datasets[dataset_id]["df"]
        framework = ABTestingFramework()
        
        variant_a = df[df[request.group_column] == 'A'][request.metric_column].dropna()
        variant_b = df[df[request.group_column] == 'B'][request.metric_column].dropna()
        
        exp_result = framework.create_experiment(
            request.test_name,
            variant_a,
            variant_b,
            request.metric_column,
            hypothesis=request.hypothesis
        )
        
        # Run t-test
        test_result = framework.t_test_analysis(request.test_name)
        
        return {
            "experiment": exp_result,
            "analysis": test_result
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==================== Formula Builder Endpoints ====================

@app.post("/api/formula/create", tags=["Formula Builder"])
async def create_formula(dataset_id: str = Query(...), request: FormulaRequest = None):
    """Create a custom formula"""
    
    if dataset_id not in datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    try:
        from formula_builder import FormulaBuilder
        
        df = datasets[dataset_id]["df"]
        builder = FormulaBuilder(df)
        
        result = builder.create_arithmetic_formula(request.name, request.formula, request.description)
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==================== Report Scheduling Endpoints ====================

@app.post("/api/schedule/create", tags=["Report Scheduling"])
async def create_schedule(request: ReportScheduleRequest = None):
    """Create a scheduled report"""
    
    try:
        from report_scheduler import ReportScheduler
        
        scheduler = ReportScheduler()
        
        result = scheduler.create_schedule(request.name, {
            "name": request.name,
            "frequency": request.frequency,
            "time": request.time,
            "report_format": request.format
        })
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/schedule/list", tags=["Report Scheduling"])
async def list_schedules():
    """List all scheduled reports"""
    
    try:
        from report_scheduler import ReportScheduler
        
        scheduler = ReportScheduler()
        schedules = scheduler.list_schedules()
        
        return {
            "schedules": schedules,
            "total": len(schedules)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==================== Streaming Endpoints ====================

@app.post("/api/stream/create", tags=["Real-time Streaming"])
async def create_stream(stream_id: str = Query(...)):
    """Create a data stream"""
    try:
        result = _streaming_manager.create_stream(stream_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/stream/ingest", tags=["Real-time Streaming"])
async def ingest_data(stream_id: str = Query(...), data: List[Dict] = None):
    """Ingest data into stream and broadcast over WebSockets (MVP)."""
    try:
        result = _streaming_manager.ingest_data(stream_id, data or [])

        # Broadcast success events to all WS clients
        if isinstance(result, dict) and result.get("success") is not False:
            await _broadcast_ws_event({
                "type": "stream_ingest",
                "stream_id": stream_id,
                "ingestion_stats": result.get("ingestion_stats", {}),
                "server_ts": datetime.now().isoformat()
            })

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stream/{stream_id}", tags=["Real-time Streaming"])
async def get_stream_data(stream_id: str, format: str = Query("dataframe")):
    """Get data from stream"""
    try:
        data = _streaming_manager.get_stream_data(stream_id, format)
        return {"stream_id": stream_id, "format": format, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==================== Export Endpoints ====================

@app.get("/api/export/{dataset_id}", tags=["Export"])
async def export_dataset(dataset_id: str, format: str = Query("csv")):
    """Export dataset in specified format"""
    
    if dataset_id not in datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    df = datasets[dataset_id]["df"]
    ds_name = datasets[dataset_id]["name"]
    
    try:
        if format == "csv":
            csv_data = df.to_csv(index=False)
            return JSONResponse(
                content={"data": csv_data},
                headers={"Content-Disposition": f"attachment; filename={ds_name}.csv"}
            )
        elif format == "json":
            return df.to_dict(orient='records')
        elif format == "excel":
            excel_buffer = io.BytesIO()
            df.to_excel(excel_buffer, index=False)
            excel_buffer.seek(0)
            return FileResponse(excel_buffer, filename=f"{ds_name}.xlsx")
        else:
            raise HTTPException(status_code=400, detail="Unsupported format")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==================== Root & Documentation ====================

@app.get("/", tags=["Root"])
async def root():
    """Root endpoint with API documentation link"""
    return {
        "message": "DataViz Pro API",
        "documentation": "/api/docs",
        "health_check": "/api/health"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
