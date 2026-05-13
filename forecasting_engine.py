"""
Advanced Forecasting Engine
Supports ARIMA, Prophet, and exponential smoothing for time series forecasting
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta
import warnings
import json

warnings.filterwarnings('ignore')

# Conditional imports for forecasting libraries
try:
    from statsmodels.tsa.arima.model import ARIMA
    from statsmodels.tsa.seasonal import seasonal_decompose
    from statsmodels.tsa.stattools import adfuller
    from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

try:
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False


class ForecastingEngine:
    """Advanced forecasting engine supporting multiple models"""
    
    def __init__(self):
        self.models = {}
        self.forecasts = {}
        self.diagnostics = {}
    
    def prepare_timeseries(self, df: pd.DataFrame, date_col: str, value_col: str) -> pd.DataFrame:
        """Prepare and validate time series data"""
        try:
            # Create a copy
            ts_df = df[[date_col, value_col]].copy()
            
            # Convert date column to datetime
            ts_df[date_col] = pd.to_datetime(ts_df[date_col])
            ts_df = ts_df.sort_values(date_col)
            
            # Remove duplicates (keep first)
            ts_df = ts_df.drop_duplicates(subset=[date_col], keep='first')
            
            # Remove NaN values
            ts_df = ts_df.dropna()
            
            # Set date as index
            ts_df = ts_df.set_index(date_col)
            
            return ts_df
            
        except Exception as e:
            return None
    
    def check_stationarity(self, timeseries: pd.Series) -> Dict[str, Any]:
        """Check if time series is stationary using ADF test"""
        if not STATSMODELS_AVAILABLE:
            return {"stationary": None, "error": "statsmodels not available"}
        
        try:
            result = adfuller(timeseries.dropna())
            
            return {
                "stationary": result[1] < 0.05,
                "adf_statistic": result[0],
                "p_value": result[1],
                "critical_values": result[4],
                "interpretation": "Series is stationary" if result[1] < 0.05 else "Series is non-stationary (differencing recommended)"
            }
        except Exception as e:
            return {"error": str(e)}
    
    def forecast_arima(self, timeseries: pd.Series, periods: int = 30, 
                      order: Tuple[int, int, int] = None) -> Dict[str, Any]:
        """Forecast using ARIMA model"""
        if not STATSMODELS_AVAILABLE:
            return {"error": "statsmodels not available for ARIMA"}
        
        try:
            # Auto-detect order if not provided
            if order is None:
                # Simple heuristic: p=1, d=1, q=1
                order = (1, 1, 1)
            
            # Fit ARIMA model
            model = ARIMA(timeseries, order=order)
            fitted_model = model.fit()
            
            # Generate forecast
            forecast_result = fitted_model.get_forecast(steps=periods)
            forecast_df = forecast_result.conf_int()
            forecast_df['forecast'] = forecast_result.predicted_mean
            
            # Calculate diagnostics
            residuals = fitted_model.resid
            mae = np.mean(np.abs(residuals))
            rmse = np.sqrt(np.mean(residuals**2))
            
            return {
                "model_type": "ARIMA",
                "order": order,
                "forecast": forecast_df.to_dict(),
                "summary": {
                    "mae": mae,
                    "rmse": rmse,
                    "aic": fitted_model.aic,
                    "bic": fitted_model.bic
                },
                "residuals": residuals.to_dict(),
                "success": True
            }
            
        except Exception as e:
            return {"error": str(e), "success": False}
    
    def forecast_prophet(self, df: pd.DataFrame, date_col: str, value_col: str,
                        periods: int = 30, interval_width: float = 0.95) -> Dict[str, Any]:
        """Forecast using Facebook's Prophet"""
        if not PROPHET_AVAILABLE:
            return {"error": "Prophet not available"}
        
        try:
            # Prepare data for Prophet (requires 'ds' and 'y' columns)
            prophet_df = df[[date_col, value_col]].copy()
            prophet_df.columns = ['ds', 'y']
            prophet_df['ds'] = pd.to_datetime(prophet_df['ds'])
            prophet_df = prophet_df.sort_values('ds')
            
            # Remove NaN values
            prophet_df = prophet_df.dropna()
            
            # Fit Prophet model
            model = Prophet(
                yearly_seasonality=True,
                weekly_seasonality=True,
                daily_seasonality=False,
                interval_width=interval_width,
                changepoint_prior_scale=0.05
            )
            model.fit(prophet_df)
            
            # Create future dataframe
            future = model.make_future_dataframe(periods=periods, freq='D')
            
            # Make forecast
            forecast = model.predict(future)
            
            # Extract relevant columns
            forecast_clean = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(periods)
            
            return {
                "model_type": "Prophet",
                "forecast": forecast_clean.to_dict(),
                "seasonality_components": {
                    "yearly": "included",
                    "weekly": "included",
                    "monthly": "auto-detected"
                },
                "success": True
            }
            
        except Exception as e:
            return {"error": str(e), "success": False}
    
    def forecast_exponential_smoothing(self, timeseries: pd.Series, periods: int = 30,
                                      seasonal: Optional[str] = None) -> Dict[str, Any]:
        """Forecast using exponential smoothing"""
        if not STATSMODELS_AVAILABLE:
            return {"error": "statsmodels not available"}
        
        try:
            from statsmodels.tsa.holtwinters import ExponentialSmoothing
            
            # Fit exponential smoothing
            if seasonal and len(timeseries) > 24:
                model = ExponentialSmoothing(
                    timeseries,
                    seasonal_periods=12,
                    trend='add',
                    seasonal='add',
                    initialization_method='estimated'
                )
            else:
                model = ExponentialSmoothing(
                    timeseries,
                    trend='add',
                    initialization_method='estimated'
                )
            
            fitted_model = model.fit(optimized=True)
            
            # Forecast
            forecast_values = fitted_model.forecast(steps=periods)
            
            return {
                "model_type": "Exponential Smoothing",
                "forecast": forecast_values.to_dict(),
                "smoothing_level": fitted_model.params.get('smoothing_level'),
                "success": True
            }
            
        except Exception as e:
            return {"error": str(e), "success": False}
    
    def decompose_timeseries(self, timeseries: pd.Series, period: int = 12) -> Dict[str, Any]:
        """Decompose time series into trend, seasonality, and residuals"""
        if not STATSMODELS_AVAILABLE:
            return {"error": "statsmodels not available"}
        
        try:
            decomposition = seasonal_decompose(timeseries, model='additive', period=period)
            
            return {
                "trend": decomposition.trend.to_dict(),
                "seasonal": decomposition.seasonal.to_dict(),
                "residual": decomposition.resid.to_dict(),
                "observed": decomposition.observed.to_dict()
            }
            
        except Exception as e:
            return {"error": str(e)}
    
    def compare_models(self, timeseries: pd.Series, test_size: float = 0.2,
                      periods: int = 30) -> Dict[str, Any]:
        """Compare multiple forecasting models"""
        if not SKLEARN_AVAILABLE:
            return {"error": "scikit-learn not available"}
        
        try:
            # Split data
            train_size = int(len(timeseries) * (1 - test_size))
            train, test = timeseries[:train_size], timeseries[train_size:]
            
            results = {}
            
            # ARIMA
            if STATSMODELS_AVAILABLE:
                arima_result = self.forecast_arima(train, periods=len(test), order=(1, 1, 1))
                if arima_result.get('success'):
                    forecast_values = list(arima_result['forecast']['forecast'].values())
                    mae = mean_absolute_error(test, forecast_values[:len(test)])
                    rmse = np.sqrt(mean_squared_error(test, forecast_values[:len(test)]))
                    results['ARIMA'] = {'MAE': mae, 'RMSE': rmse}
            
            # Exponential Smoothing
            if STATSMODELS_AVAILABLE:
                es_result = self.forecast_exponential_smoothing(train, periods=len(test))
                if es_result.get('success'):
                    forecast_values = list(es_result['forecast'].values())
                    mae = mean_absolute_error(test, forecast_values)
                    rmse = np.sqrt(mean_squared_error(test, forecast_values))
                    results['Exponential Smoothing'] = {'MAE': mae, 'RMSE': rmse}
            
            return {
                "model_comparison": results,
                "best_model": min(results.items(), key=lambda x: x[1]['MAE'])[0] if results else None
            }
            
        except Exception as e:
            return {"error": str(e)}
    
    def get_forecast_confidence_intervals(self, forecast_dict: Dict, confidence: float = 0.95) -> Dict:
        """Calculate confidence intervals for forecasts"""
        try:
            return {
                "confidence_level": confidence,
                "lower_bound": "Included in model output",
                "upper_bound": "Included in model output",
                "interpretation": f"There is a {confidence*100:.0f}% probability the actual value will fall within this range"
            }
        except Exception as e:
            return {"error": str(e)}
    
    def save_forecast_config(self, config: Dict, filename: str) -> bool:
        """Save forecast configuration for later use"""
        try:
            with open(filename, 'w') as f:
                json.dump(config, f, indent=2, default=str)
            return True
        except Exception as e:
            return False
    
    def load_forecast_config(self, filename: str) -> Optional[Dict]:
        """Load forecast configuration"""
        try:
            with open(filename, 'r') as f:
                return json.load(f)
        except Exception as e:
            return None
