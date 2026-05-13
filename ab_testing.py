"""
A/B Testing Framework
Statistical analysis for A/B tests, multivariate tests, and experiment tracking
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from scipy import stats
from datetime import datetime
import json
import warnings

warnings.filterwarnings('ignore')


class ABTestingFramework:
    """Comprehensive A/B testing and experimentation framework"""
    
    def __init__(self):
        self.experiments = {}
        self.results = {}
        self.baseline_data = {}
    
    def create_experiment(self, experiment_id: str, variant_a: pd.Series,
                         variant_b: pd.Series, metric_name: str,
                         description: str = "",
                         hypothesis: str = "") -> Dict[str, Any]:
        """Create and initialize an A/B test experiment"""
        
        try:
            # Basic validation
            if len(variant_a) < 2 or len(variant_b) < 2:
                return {"success": False, "error": "Both variants need at least 2 observations"}
            
            self.experiments[experiment_id] = {
                "id": experiment_id,
                "metric": metric_name,
                "description": description,
                "hypothesis": hypothesis,
                "variant_a": {
                    "data": variant_a,
                    "n": len(variant_a),
                    "mean": variant_a.mean(),
                    "std": variant_a.std(),
                    "min": variant_a.min(),
                    "max": variant_a.max()
                },
                "variant_b": {
                    "data": variant_b,
                    "n": len(variant_b),
                    "mean": variant_b.mean(),
                    "std": variant_b.std(),
                    "min": variant_b.min(),
                    "max": variant_b.max()
                },
                "created_at": datetime.now().isoformat()
            }
            
            # Store baseline for comparison
            self.baseline_data[experiment_id] = {
                "variant_a_mean": variant_a.mean(),
                "variant_b_mean": variant_b.mean()
            }
            
            return {
                "success": True,
                "message": f"Experiment '{experiment_id}' created successfully",
                "experiment_id": experiment_id,
                "variant_a_n": len(variant_a),
                "variant_b_n": len(variant_b)
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def t_test_analysis(self, experiment_id: str, 
                       equal_var: bool = True,
                       alpha: float = 0.05) -> Dict[str, Any]:
        """Perform independent samples t-test"""
        
        if experiment_id not in self.experiments:
            return {"success": False, "error": "Experiment not found"}
        
        try:
            exp = self.experiments[experiment_id]
            variant_a = exp['variant_a']['data']
            variant_b = exp['variant_b']['data']
            
            # Perform t-test
            t_statistic, p_value = stats.ttest_ind(variant_a, variant_b, equal_var=equal_var)
            
            # Calculate effect size (Cohen's d)
            n_a, n_b = len(variant_a), len(variant_b)
            mean_a, mean_b = variant_a.mean(), variant_b.mean()
            std_a, std_b = variant_a.std(), variant_b.std()
            
            pooled_std = np.sqrt(((n_a - 1) * std_a**2 + (n_b - 1) * std_b**2) / (n_a + n_b - 2))
            cohens_d = (mean_b - mean_a) / pooled_std if pooled_std > 0 else 0
            
            # Determine significance
            is_significant = p_value < alpha
            
            result = {
                "success": True,
                "test_type": "Independent Samples t-test",
                "hypothesis": exp.get('hypothesis'),
                "t_statistic": t_statistic,
                "p_value": p_value,
                "alpha": alpha,
                "is_significant": is_significant,
                "cohens_d": cohens_d,
                "effect_size_interpretation": self._interpret_cohens_d(abs(cohens_d)),
                "mean_difference": mean_b - mean_a,
                "variant_a_mean": mean_a,
                "variant_b_mean": mean_b,
                "confidence_level": 1 - alpha,
                "recommendation": self._get_recommendation(is_significant, mean_b > mean_a)
            }
            
            self.results[experiment_id] = result
            return result
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def mann_whitney_test(self, experiment_id: str,
                         alpha: float = 0.05) -> Dict[str, Any]:
        """Perform Mann-Whitney U test (non-parametric alternative)"""
        
        if experiment_id not in self.experiments:
            return {"success": False, "error": "Experiment not found"}
        
        try:
            exp = self.experiments[experiment_id]
            variant_a = exp['variant_a']['data']
            variant_b = exp['variant_b']['data']
            
            # Perform Mann-Whitney U test
            u_statistic, p_value = stats.mannwhitneyu(variant_a, variant_b)
            
            is_significant = p_value < alpha
            
            result = {
                "success": True,
                "test_type": "Mann-Whitney U Test (Non-parametric)",
                "u_statistic": u_statistic,
                "p_value": p_value,
                "alpha": alpha,
                "is_significant": is_significant,
                "median_a": np.median(variant_a),
                "median_b": np.median(variant_b),
                "recommendation": self._get_recommendation(is_significant, np.median(variant_b) > np.median(variant_a))
            }
            
            self.results[experiment_id] = result
            return result
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def chi_square_test(self, experiment_id: str, contingency_table: np.ndarray,
                       alpha: float = 0.05) -> Dict[str, Any]:
        """Perform chi-square test for categorical data"""
        
        try:
            chi2, p_value, dof, expected = stats.chi2_contingency(contingency_table)
            
            is_significant = p_value < alpha
            
            # Cramér's V effect size
            n = contingency_table.sum()
            min_dim = min(contingency_table.shape) - 1
            cramers_v = np.sqrt(chi2 / (n * min_dim)) if min_dim > 0 else 0
            
            result = {
                "success": True,
                "test_type": "Chi-Square Test",
                "chi_square_statistic": chi2,
                "p_value": p_value,
                "degrees_of_freedom": dof,
                "alpha": alpha,
                "is_significant": is_significant,
                "cramers_v": cramers_v,
                "effect_size_interpretation": self._interpret_cramers_v(cramers_v),
                "recommendation": "Significant difference detected" if is_significant else "No significant difference"
            }
            
            return result
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def calculate_sample_size(self, baseline_conversion: float, 
                             min_effect_size: float,
                             alpha: float = 0.05,
                             power: float = 0.8) -> Dict[str, Any]:
        """Calculate required sample size for A/B test"""
        
        try:
            # Using the formula for two-proportion z-test
            z_alpha = stats.norm.ppf(1 - alpha / 2)
            z_beta = stats.norm.ppf(power)
            
            p1 = baseline_conversion
            p2 = baseline_conversion + min_effect_size
            p_bar = (p1 + p2) / 2
            
            n = (z_alpha + z_beta)**2 * (p1 * (1 - p1) + p2 * (1 - p2)) / (p2 - p1)**2
            n = int(np.ceil(n))
            
            return {
                "success": True,
                "sample_size_per_variant": n,
                "total_sample_size": n * 2,
                "baseline_conversion": baseline_conversion,
                "min_effect_size": min_effect_size,
                "alpha": alpha,
                "power": power,
                "note": f"Each variant needs {n:,} observations for {power*100:.0f}% power"
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def calculate_confidence_interval(self, experiment_id: str,
                                    confidence: float = 0.95) -> Dict[str, Any]:
        """Calculate confidence intervals for the difference in means"""
        
        if experiment_id not in self.experiments:
            return {"success": False, "error": "Experiment not found"}
        
        try:
            exp = self.experiments[experiment_id]
            variant_a = exp['variant_a']['data']
            variant_b = exp['variant_b']['data']
            
            mean_diff = variant_b.mean() - variant_a.mean()
            n_a, n_b = len(variant_a), len(variant_b)
            
            # Standard error of the difference
            se_diff = np.sqrt((variant_a.std()**2 / n_a) + (variant_b.std()**2 / n_b))
            
            # Critical value
            z_critical = stats.norm.ppf((1 + confidence) / 2)
            
            # Confidence interval
            margin_of_error = z_critical * se_diff
            ci_lower = mean_diff - margin_of_error
            ci_upper = mean_diff + margin_of_error
            
            return {
                "success": True,
                "confidence_level": confidence,
                "mean_difference": mean_diff,
                "confidence_interval_lower": ci_lower,
                "confidence_interval_upper": ci_upper,
                "margin_of_error": margin_of_error,
                "interpretation": f"We are {confidence*100:.0f}% confident the true difference is between {ci_lower:.4f} and {ci_upper:.4f}"
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def sequential_testing(self, experiment_id: str, 
                          alpha: float = 0.05,
                          max_observations: int = 10000) -> Dict[str, Any]:
        """Perform sequential testing (optional stopping)"""
        
        if experiment_id not in self.experiments:
            return {"success": False, "error": "Experiment not found"}
        
        try:
            exp = self.experiments[experiment_id]
            variant_a = exp['variant_a']['data']
            variant_b = exp['variant_b']['data']
            
            # Sequential probability ratio test (SPRT)
            results = []
            
            for n in range(2, min(len(variant_a), len(variant_b), max_observations)):
                t_stat, p_val = stats.ttest_ind(
                    variant_a.iloc[:n],
                    variant_b.iloc[:n]
                )
                
                results.append({
                    "n": n,
                    "t_statistic": t_stat,
                    "p_value": p_val,
                    "significant": p_val < alpha
                })
                
                if p_val < alpha:
                    # Stop early if significant
                    return {
                        "success": True,
                        "test_type": "Sequential Testing (SPRT)",
                        "stopped_at_n": n,
                        "conclusion": "Significant difference detected",
                        "p_value": p_val,
                        "final_t_statistic": t_stat
                    }
            
            return {
                "success": True,
                "test_type": "Sequential Testing (SPRT)",
                "stopped_at_n": len(results),
                "conclusion": "No significant difference at current sample size",
                "recommendation": "Continue collecting data or accept null hypothesis"
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def multivariate_test(self, variants: Dict[str, pd.Series],
                         alpha: float = 0.05) -> Dict[str, Any]:
        """Perform ANOVA for multiple variants (A/B/C testing)"""
        
        try:
            # Prepare data for ANOVA
            groups = [variant_data for variant_data in variants.values()]
            
            # Perform ANOVA
            f_statistic, p_value = stats.f_oneway(*groups)
            
            is_significant = p_value < alpha
            
            result = {
                "success": True,
                "test_type": "One-Way ANOVA (Multivariate)",
                "num_variants": len(variants),
                "variant_names": list(variants.keys()),
                "f_statistic": f_statistic,
                "p_value": p_value,
                "alpha": alpha,
                "is_significant": is_significant,
                "means": {name: data.mean() for name, data in variants.items()},
                "recommendation": "At least one variant differs significantly" if is_significant else "No significant differences detected"
            }
            
            return result
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def get_experiment_summary(self, experiment_id: str) -> Dict[str, Any]:
        """Get comprehensive summary of an experiment"""
        
        if experiment_id not in self.experiments:
            return {"success": False, "error": "Experiment not found"}
        
        exp = self.experiments[experiment_id]
        result = self.results.get(experiment_id, {})
        
        return {
            "success": True,
            "experiment_id": experiment_id,
            "metric": exp['metric'],
            "hypothesis": exp.get('hypothesis', ''),
            "variant_a": {
                "n": exp['variant_a']['n'],
                "mean": exp['variant_a']['mean'],
                "std": exp['variant_a']['std']
            },
            "variant_b": {
                "n": exp['variant_b']['n'],
                "mean": exp['variant_b']['mean'],
                "std": exp['variant_b']['std']
            },
            "analysis_result": result,
            "created_at": exp.get('created_at')
        }
    
    def _interpret_cohens_d(self, cohens_d: float) -> str:
        """Interpret Cohen's d effect size"""
        
        if cohens_d < 0.2:
            return "Negligible"
        elif cohens_d < 0.5:
            return "Small"
        elif cohens_d < 0.8:
            return "Medium"
        else:
            return "Large"
    
    def _interpret_cramers_v(self, cramers_v: float) -> str:
        """Interpret Cramér's V effect size"""
        
        if cramers_v < 0.1:
            return "Negligible"
        elif cramers_v < 0.3:
            return "Small"
        elif cramers_v < 0.5:
            return "Medium"
        else:
            return "Large"
    
    def _get_recommendation(self, is_significant: bool, b_better: bool) -> str:
        """Generate recommendation based on test results"""
        
        if not is_significant:
            return "No statistically significant difference. More data or continue with current variant."
        elif b_better:
            return "✅ Variant B is significantly better. Consider implementing."
        else:
            return "✅ Variant A is significantly better. Continue with current variant."
    
    def export_results(self, experiment_id: str) -> Dict[str, Any]:
        """Export experiment results as JSON"""
        
        if experiment_id not in self.experiments:
            return {"success": False, "error": "Experiment not found"}
        
        exp = self.experiments[experiment_id]
        
        export = {
            "experiment_id": experiment_id,
            "metric": exp['metric'],
            "description": exp.get('description', ''),
            "created_at": exp.get('created_at'),
            "variant_a_summary": {
                "n": exp['variant_a']['n'],
                "mean": float(exp['variant_a']['mean']),
                "std": float(exp['variant_a']['std']),
                "min": float(exp['variant_a']['min']),
                "max": float(exp['variant_a']['max'])
            },
            "variant_b_summary": {
                "n": exp['variant_b']['n'],
                "mean": float(exp['variant_b']['mean']),
                "std": float(exp['variant_b']['std']),
                "min": float(exp['variant_b']['min']),
                "max": float(exp['variant_b']['max'])
            },
            "analysis_results": self.results.get(experiment_id, {})
        }
        
        return {"success": True, "export": export}
    
    def list_experiments(self) -> List[Dict]:
        """List all experiments"""
        
        return [
            {
                "id": exp_id,
                "metric": exp['metric'],
                "description": exp.get('description', ''),
                "variant_a_n": exp['variant_a']['n'],
                "variant_b_n": exp['variant_b']['n'],
                "has_results": exp_id in self.results
            }
            for exp_id, exp in self.experiments.items()
        ]
