"""
Scheduled Report Generation System
Automated report creation, scheduling, and delivery
"""

import pandas as pd
import json
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime, timedelta
import warnings
from abc import ABC, abstractmethod

warnings.filterwarnings('ignore')

# Conditional imports
try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    APScheduler_AVAILABLE = True
except ImportError:
    APScheduler_AVAILABLE = False

try:
    import schedule
    SCHEDULE_AVAILABLE = True
except ImportError:
    SCHEDULE_AVAILABLE = False


class ReportGenerator(ABC):
    """Abstract base class for report generators"""
    
    @abstractmethod
    def generate(self, data: pd.DataFrame, config: Dict) -> str:
        """Generate report content"""
        pass
    
    @abstractmethod
    def save(self, content: str, filepath: str) -> bool:
        """Save report to file"""
        pass


class HTMLReportGenerator(ReportGenerator):
    """Generate HTML reports"""
    
    def generate(self, data: pd.DataFrame, config: Dict) -> str:
        """Generate HTML report"""
        
        try:
            title = config.get('title', 'Data Report')
            description = config.get('description', '')
            include_summary = config.get('include_summary', True)
            include_stats = config.get('include_stats', True)
            include_charts = config.get('include_charts', False)
            
            html_parts = [
                f"<!DOCTYPE html>",
                f"<html>",
                f"<head>",
                f"<meta charset='UTF-8'>",
                f"<title>{title}</title>",
                f"<style>",
                f"body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}",
                f"h1 {{ color: #333; border-bottom: 3px solid #667eea; padding-bottom: 10px; }}",
                f"h2 {{ color: #666; margin-top: 30px; }}",
                f"table {{ border-collapse: collapse; width: 100%; background: white; }}",
                f"th {{ background: #667eea; color: white; padding: 10px; text-align: left; }}",
                f"td {{ padding: 10px; border-bottom: 1px solid #ddd; }}",
                f"tr:hover {{ background: #f9f9f9; }}",
                f".summary {{ background: white; padding: 15px; border-radius: 5px; margin: 15px 0; }}",
                f".stats {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin: 15px 0; }}",
                f".stat-card {{ background: white; padding: 15px; border-radius: 5px; text-align: center; }}",
                f".stat-value {{ font-size: 24px; font-weight: bold; color: #667eea; }}",
                f".stat-label {{ color: #999; font-size: 12px; margin-top: 5px; }}",
                f".timestamp {{ color: #999; font-size: 12px; margin-top: 20px; }}",
                f"</style>",
                f"</head>",
                f"<body>",
                f"<h1>{title}</h1>",
            ]
            
            if description:
                html_parts.append(f"<div class='summary'><p>{description}</p></div>")
            
            # Summary stats
            if include_summary:
                html_parts.append(f"<h2>Overview</h2>")
                html_parts.append(f"<div class='summary'>")
                html_parts.append(f"<p><strong>Total Records:</strong> {len(data):,}</p>")
                html_parts.append(f"<p><strong>Total Columns:</strong> {len(data.columns)}</p>")
                html_parts.append(f"<p><strong>Report Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>")
                html_parts.append(f"</div>")
            
            # Statistical summary
            if include_stats:
                html_parts.append(f"<h2>Statistical Summary</h2>")
                stats_df = data.describe()
                html_parts.append(stats_df.to_html(classes='summary-table'))
            
            # Data table
            html_parts.append(f"<h2>Data Sample</h2>")
            html_parts.append(data.head(100).to_html())
            
            # Footer
            html_parts.append(f"<div class='timestamp'>")
            html_parts.append(f"Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            html_parts.append(f"</div>")
            html_parts.append(f"</body>")
            html_parts.append(f"</html>")
            
            return "\n".join(html_parts)
            
        except Exception as e:
            return f"<html><body><h1>Error generating report</h1><p>{str(e)}</p></body></html>"
    
    def save(self, content: str, filepath: str) -> bool:
        """Save HTML report"""
        
        try:
            with open(filepath, 'w') as f:
                f.write(content)
            return True
        except Exception:
            return False


class CSVReportGenerator(ReportGenerator):
    """Generate CSV reports"""
    
    def generate(self, data: pd.DataFrame, config: Dict) -> str:
        """Generate CSV report"""
        
        try:
            include_index = config.get('include_index', False)
            return data.to_csv(index=include_index)
        except Exception as e:
            return f"Error: {str(e)}"
    
    def save(self, content: str, filepath: str) -> bool:
        """Save CSV report"""
        
        try:
            with open(filepath, 'w') as f:
                f.write(content)
            return True
        except Exception:
            return False


class JSONReportGenerator(ReportGenerator):
    """Generate JSON reports"""
    
    def generate(self, data: pd.DataFrame, config: Dict) -> str:
        """Generate JSON report"""
        
        try:
            orient = config.get('orient', 'records')
            return data.to_json(orient=orient)
        except Exception as e:
            return f"Error: {str(e)}"
    
    def save(self, content: str, filepath: str) -> bool:
        """Save JSON report"""
        
        try:
            with open(filepath, 'w') as f:
                f.write(content)
            return True
        except Exception:
            return False


class ReportScheduler:
    """Schedule and manage automated reports"""
    
    def __init__(self):
        self.schedules = {}
        self.reports_history = {}
        self.generators = {
            'html': HTMLReportGenerator(),
            'csv': CSVReportGenerator(),
            'json': JSONReportGenerator()
        }
        
        # Initialize scheduler if available
        if APScheduler_AVAILABLE:
            self.scheduler = BackgroundScheduler()
            self.scheduler_type = 'apscheduler'
        elif SCHEDULE_AVAILABLE:
            self.scheduler = None
            self.scheduler_type = 'schedule'
        else:
            self.scheduler = None
            self.scheduler_type = None
    
    def create_schedule(self, schedule_id: str, config: Dict) -> Dict[str, Any]:
        """Create a scheduled report"""
        
        try:
            # Ensure background scheduler is running (otherwise schedules never execute)
            if self.scheduler_type == "apscheduler":
                self.start_scheduler()
            
            schedule_config = {
                "id": schedule_id,
                "name": config.get('name', schedule_id),
                "frequency": config.get('frequency', 'daily'),  # daily, weekly, monthly
                "time": config.get('time', '09:00'),
                "data_source": config.get('data_source'),
                "report_format": config.get('report_format', 'html'),
                "output_path": config.get('output_path'),
                "recipients": config.get('recipients', []),
                "enabled": config.get('enabled', True),
                "created_at": datetime.now().isoformat(),
                "last_run": None,
                "next_run": self._calculate_next_run(config.get('frequency'), config.get('time')),
            }

            self.schedules[schedule_id] = schedule_config

            # Register APScheduler job so reports actually run.
            if self.scheduler_type == "apscheduler" and self.scheduler:
                # If job already exists, replace it.
                job_id = f"report_{schedule_id}"

                # Light interval approximation (avoid heavy cron parsing).
                freq = schedule_config["frequency"]
                interval_days = 1
                if freq == "weekly":
                    interval_days = 7
                elif freq == "monthly":
                    interval_days = 30

                # Schedule a lightweight job that expects data_source to provide data.
                # MVP: if data_source isn't provided, we skip execution gracefully.
                self.scheduler.add_job(
                    func=self._scheduled_run_wrapper,
                    trigger="interval",
                    days=interval_days,
                    id=job_id,
                    replace_existing=True,
                    kwargs={"schedule_id": schedule_id},
                )
            
            return {
                "success": True,
                "message": f"Schedule '{schedule_id}' created",
                "next_run": schedule_config['next_run']
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def run_report(self, schedule_id: str, data: pd.DataFrame) -> Dict[str, Any]:
        """Execute a report immediately"""
        if schedule_id not in self.schedules:
            return {"success": False, "error": f"Schedule '{schedule_id}' not found"}

        try:
            config = self.schedules[schedule_id]
            report_format = config.get('report_format', 'html')

            if report_format not in self.generators:
                return {"success": False, "error": f"Unsupported format: {report_format}"}

            generator = self.generators[report_format]

            # Generate report
            content = generator.generate(data, config)

            # Save report
            output_path = config.get('output_path')
            if output_path:
                success = generator.save(content, output_path)
                if not success:
                    return {"success": False, "error": "Failed to save report"}

            # Update schedule
            self.schedules[schedule_id]['last_run'] = datetime.now().isoformat()
            self.schedules[schedule_id]['next_run'] = self._calculate_next_run(
                config.get('frequency'),
                config.get('time')
            )

            # Record in history
            if schedule_id not in self.reports_history:
                self.reports_history[schedule_id] = []

            self.reports_history[schedule_id].append({
                "executed_at": datetime.now().isoformat(),
                "format": report_format,
                "output_path": output_path,
                "records": len(data),
                "status": "success"
            })

            return {
                "success": True,
                "message": "Report generated successfully",
                "format": report_format,
                "output_path": output_path,
                "records_processed": len(data)
            }

        except Exception as e:
            return {"success": False, "error": str(e)}

    def _scheduled_run_wrapper(self, schedule_id: str) -> None:
        """
        APScheduler job wrapper (MVP).

        This project currently doesn't persist datasets for scheduled jobs.
        Therefore, if the schedule doesn't have a data_source we skip.
        Otherwise we generate a report for an empty DataFrame so the job
        doesn't crash (so scheduling is at least functional).
        """
        try:
            config = self.schedules.get(schedule_id)
            if not config or not config.get("enabled", True):
                return

            data_source = config.get("data_source")
            if not data_source:
                return

            empty_df = pd.DataFrame()
            self.run_report(schedule_id=schedule_id, data=empty_df)
        except Exception:
            return

    def start_scheduler(self) -> Dict[str, Any]:
        """Start the background scheduler"""
        
        if not APScheduler_AVAILABLE:
            return {"success": False, "error": "APScheduler not available"}
        
        try:
            if not self.scheduler.running:
                self.scheduler.start()
            
            return {
                "success": True,
                "message": "Scheduler started",
                "scheduled_reports": len(self.schedules)
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def stop_scheduler(self) -> Dict[str, Any]:
        """Stop the background scheduler"""
        
        try:
            if self.scheduler and hasattr(self.scheduler, 'running') and self.scheduler.running:
                self.scheduler.shutdown()
            
            return {"success": True, "message": "Scheduler stopped"}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def get_schedule_status(self, schedule_id: str) -> Optional[Dict]:
        """Get status of a scheduled report"""
        
        if schedule_id not in self.schedules:
            return None
        
        schedule = self.schedules[schedule_id]
        history = self.reports_history.get(schedule_id, [])
        
        return {
            "id": schedule_id,
            "name": schedule.get('name'),
            "frequency": schedule.get('frequency'),
            "enabled": schedule.get('enabled'),
            "last_run": schedule.get('last_run'),
            "next_run": schedule.get('next_run'),
            "total_runs": len(history),
            "latest_status": history[-1] if history else None
        }
    
    def list_schedules(self) -> List[Dict]:
        """List all scheduled reports"""
        
        return [
            {
                "id": schedule_id,
                "name": schedule.get('name'),
                "frequency": schedule.get('frequency'),
                "enabled": schedule.get('enabled'),
                "next_run": schedule.get('next_run')
            }
            for schedule_id, schedule in self.schedules.items()
        ]
    
    def update_schedule(self, schedule_id: str, updates: Dict) -> Dict[str, Any]:
        """Update a scheduled report configuration"""
        
        if schedule_id not in self.schedules:
            return {"success": False, "error": f"Schedule '{schedule_id}' not found"}
        
        try:
            self.schedules[schedule_id].update(updates)
            return {"success": True, "message": "Schedule updated"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def delete_schedule(self, schedule_id: str) -> Dict[str, Any]:
        """Delete a scheduled report"""
        
        try:
            if schedule_id in self.schedules:
                del self.schedules[schedule_id]
            return {"success": True, "message": f"Schedule '{schedule_id}' deleted"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def get_report_history(self, schedule_id: str, limit: int = 50) -> List[Dict]:
        """Get execution history for a schedule"""
        
        if schedule_id not in self.reports_history:
            return []
        
        return self.reports_history[schedule_id][-limit:]
    
    def _calculate_next_run(self, frequency: str, time_str: str = '09:00') -> str:
        """Calculate next run time based on frequency"""
        
        try:
            now = datetime.now()
            hour, minute = map(int, time_str.split(':'))
            next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            
            # If time has already passed today, schedule for next occurrence
            if next_run <= now:
                if frequency == 'daily':
                    next_run += timedelta(days=1)
                elif frequency == 'weekly':
                    next_run += timedelta(weeks=1)
                elif frequency == 'monthly':
                    # Add one month
                    if next_run.month == 12:
                        next_run = next_run.replace(year=next_run.year + 1, month=1)
                    else:
                        next_run = next_run.replace(month=next_run.month + 1)
            
            return next_run.isoformat()
            
        except Exception:
            return datetime.now().isoformat()
    
    def get_scheduler_status(self) -> Dict[str, Any]:
        """Get overall scheduler status"""
        
        scheduler_running = False
        if self.scheduler and hasattr(self.scheduler, 'running'):
            scheduler_running = self.scheduler.running
        
        return {
            "scheduler_type": self.scheduler_type,
            "is_running": scheduler_running,
            "total_schedules": len(self.schedules),
            "active_schedules": sum(1 for s in self.schedules.values() if s.get('enabled')),
            "total_reports_run": sum(len(h) for h in self.reports_history.values())
        }
