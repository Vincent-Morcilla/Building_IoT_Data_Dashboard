import pandas as pd
import numpy as np
from typing import Dict, Any
import json

class Analytics:
    def __init__(self, db):
        self.db = db

    def run(self) -> Dict[str, Any]:
        """Run all analyses and return the results."""
        print("Running Data Quality Analysis...")
        
        quality_metrics = {}
        for stream_id in self.db:
            df = self.db.get_stream(stream_id)
            metrics = self.analyze_stream(df, stream_id)
            quality_metrics[stream_id] = metrics

        # Create summary plots and tables
        missing_data_plot = self.create_missing_data_plot(quality_metrics)
        outlier_plot = self.create_outlier_plot(quality_metrics)
        gap_analysis_plot = self.create_gap_analysis_plot(quality_metrics)

        return {
            "data_quality_missing": missing_data_plot,
            "data_quality_outliers": outlier_plot,
            "data_quality_gaps": gap_analysis_plot,
        }

    def analyze_stream(self, df: pd.DataFrame, stream_id: str) -> Dict[str, Any]:
        """Analyze a single stream for data quality metrics."""
        # Remove outliers
        df, outlier_info = self.remove_outlier(df)
        
        # Detect missing and zeros
        quality_info = self.detect_missing_and_zeros(df)
        
        # Deduce granularity
        granularity = self.deduce_granularity(df.index)
        
        # Analyze gaps
        gap_analysis = self.analyse_sensor_gaps(df, granularity)
        
        return {
            "outliers": outlier_info,
            "quality_info": quality_info,
            "granularity": granularity,
            "gap_analysis": gap_analysis,
        }

    def remove_outlier(self, df):
        """Removes outliers from the data based on 3 standard deviations from the mean."""
        mean = df['value'].mean()
        std = df['value'].std()
        lower_limit = mean - 3 * std
        upper_limit = mean + 3 * std

        outliers = df[(df['value'] < lower_limit) | (df['value'] > upper_limit)]
        outlier_info = (len(outliers), list(zip(outliers.index, outliers['value'])))

        df['value'] = df['value'].apply(
            lambda x: mean if x < lower_limit or x > upper_limit else x
        )
        return df, outlier_info

    def detect_missing_and_zeros(self, df):
        """Detects missing values and zero values for the stream."""
        missing_count = df['value'].isna().sum()
        zero_count = (df['value'] == 0).sum()

        return {
            'Missing': missing_count,
            'Zeros': zero_count
        }

    def deduce_granularity(self, timestamps):
        """Deduces the granularity of the time intervals in the time series data."""
        time_diffs = np.diff(timestamps).astype('timedelta64[s]').astype(int)
        if len(time_diffs) == 0:
            return None
        
        granularity = pd.Series(time_diffs).mode()[0]

        if granularity % 86400 == 0:
            granularity = f'{granularity // 86400} days'
        elif granularity % 3600 == 0:
            granularity = f'{granularity // 3600} hours'
        elif 0 <= granularity % 60 < 5:
            granularity = f'{granularity // 60} minutes'        
        elif granularity % 60 > 55:
            granularity = f'{1 + granularity // 60} minutes'        
        else:
            granularity = f'{granularity} seconds'

        return granularity

    def analyse_sensor_gaps(self, df, granularity):
        """Analyzes gaps in sensor data."""
        granularity_in_seconds = self.granularity_to_seconds(granularity)
        timestamps = df.index
        time_diffs = np.diff(timestamps).astype('timedelta64[s]').astype(float) / granularity_in_seconds
        
        gap_analysis = self.categorise_gaps(time_diffs, len(timestamps), granularity_in_seconds)
        
        return gap_analysis

    def granularity_to_seconds(self, granularity):
        """Convert granularity string to seconds."""
        parts = granularity.split()
        value = int(parts[0])
        unit = parts[1]
        if unit == 'days':
            return value * 86400
        elif unit == 'hours':
            return value * 3600
        elif unit == 'minutes':
            return value * 60
        else:
            return value

    def categorise_gaps(self, normalised_diffs, total_counts, granularity_in_seconds):
        """Categorizes gaps in the time series data."""
        small_gap = sum(1.5 <= diff < 3 for diff in normalised_diffs)
        medium_gap = sum(3 <= diff < 6 for diff in normalised_diffs)
        large_gap = sum(diff >= 6 for diff in normalised_diffs)
        
        total_gaps = small_gap + medium_gap + large_gap
        gap_percentage = (total_gaps / total_counts) * 100 if total_counts > 0 else 0
        
        return {
            "small_gap": small_gap,
            "medium_gap": medium_gap,
            "large_gap": large_gap,
            "total_gaps": total_gaps,
            "gap_percentage": gap_percentage,
        }

    def create_missing_data_plot(self, quality_metrics):
        """Create a bar plot of missing data ratios."""
        stream_ids = list(quality_metrics.keys())
        missing_ratios = [metrics['quality_info']['Missing'] / sum(metrics['quality_info'].values()) 
                          for metrics in quality_metrics.values()]

        return {
            "data": [{
                "x": stream_ids,
                "y": missing_ratios,
                "type": "bar",
                "name": "Missing Data Ratio"
            }],
            "layout": {
                "title": "Missing Data Ratio by Stream",
                "xaxis": {"title": "Stream ID"},
                "yaxis": {"title": "Missing Data Ratio"}
            }
        }

    def create_outlier_plot(self, quality_metrics):
        """Create a bar plot of outlier ratios."""
        stream_ids = list(quality_metrics.keys())
        outlier_ratios = [metrics['outliers'][0] / sum(metrics['quality_info'].values()) 
                          for metrics in quality_metrics.values()]

        return {
            "data": [{
                "x": stream_ids,
                "y": outlier_ratios,
                "type": "bar",
                "name": "Outlier Ratio"
            }],
            "layout": {
                "title": "Outlier Ratio by Stream",
                "xaxis": {"title": "Stream ID"},
                "yaxis": {"title": "Outlier Ratio"}
            }
        }

    def create_gap_analysis_plot(self, quality_metrics):
        """Create a stacked bar plot of gap analysis."""
        stream_ids = list(quality_metrics.keys())
        small_gaps = [metrics['gap_analysis']['small_gap'] for metrics in quality_metrics.values()]
        medium_gaps = [metrics['gap_analysis']['medium_gap'] for metrics in quality_metrics.values()]
        large_gaps = [metrics['gap_analysis']['large_gap'] for metrics in quality_metrics.values()]

        return {
            "data": [
                {
                    "x": stream_ids,
                    "y": small_gaps,
                    "type": "bar",
                    "name": "Small Gaps"
                },
                {
                    "x": stream_ids,
                    "y": medium_gaps,
                    "type": "bar",
                    "name": "Medium Gaps"
                },
                {
                    "x": stream_ids,
                    "y": large_gaps,
                    "type": "bar",
                    "name": "Large Gaps"
                }
            ],
            "layout": {
                "title": "Gap Analysis by Stream",
                "xaxis": {"title": "Stream ID"},
                "yaxis": {"title": "Number of Gaps"},
                "barmode": "stack"
            }
        }