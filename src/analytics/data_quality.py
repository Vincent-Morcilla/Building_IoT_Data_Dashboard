import os
import pandas as pd
import numpy as np
from scipy import stats
from typing import Dict, Any

class Analytics:
    def __init__(self, db):
        self.db = db
        self._df = None
        self.output_dir = os.path.join(os.getcwd(), 'output')  # Define output directory

    def run(self) -> Dict[str, Any]:
        """Run all analyses and return the results."""
        print("Running Data Quality Analysis...")
        self._df = self.preprocess_to_sensor_rows()
        self._df = self.profile_groups(self._df)
        gap_analysis_results = self.analyse_sensor_gaps(self._df)
        self._df = pd.concat([self._df, gap_analysis_results], axis=1)
        
        data_quality_df = self.prepare_data_quality_df(self._df)
        summary_table_df = self.create_summary_table(data_quality_df)
        
        # Save data quality table to CSV
        self.save_data_quality_to_csv(data_quality_df)
        
        # Save summary table to CSV
        self.save_summary_table_to_csv(summary_table_df)
        
        return {
            "DataQualityTable": self.create_data_quality_plot(data_quality_df),
            "SummaryTable": self.create_summary_plot(summary_table_df),
            "DataQualityOverview": self.get_data_quality_overview(data_quality_df),
            "SummaryAnalysis": self.get_summary_analysis(summary_table_df)
        }

    def preprocess_to_sensor_rows(self):
        sensor_data = []
        all_streams = self.db.get_all_streams()
        
        for stream_id, df in all_streams.items():
            try:
                label = self.db.get_label(stream_id)
                
                if df.empty:
                    print(f"Warning: Empty DataFrame for stream {stream_id}")
                    continue
                
                row = {
                    'StreamID': stream_id,
                    'Label': label,
                    'Timestamps': df['time'],
                    'Values': df['value'].values,
                    'Deduced_Granularity': self.deduce_granularity(df['time']),
                    'Value_Count': len(df),
                    'Outliers': self.detect_outliers(df['value'].values),
                    'Missing': df['value'].isna().sum(),
                    'Zeros': (df['value'] == 0).sum(),
                    'Start_Timestamp': df['time'].min(),
                    'End_Timestamp': df['time'].max(),
                    'Sensor_Mean': df['value'].mean(),
                    'Sensor_Max': df['value'].max(),
                    'Sensor_Min': df['value'].min()
                }
                sensor_data.append(row)
            except Exception as e:
                print(f"Error processing stream {stream_id}: {str(e)}")
        
        result_df = pd.DataFrame(sensor_data)
        print(f"Preprocessed data shape: {result_df.shape}")
        print(f"Columns: {result_df.columns}")
        print(result_df.head())
        return result_df

    def profile_groups(self, df):
        grouped = df.groupby('Label')
        
        def calculate_group_stats(group):
            return pd.Series({
                'Group_Mean': group['Sensor_Mean'].mean(),
                'Group_Std': group['Sensor_Mean'].std(),
                'Group_Min': group['Sensor_Min'].min(),
                'Group_Max': group['Sensor_Max'].max(),
            })
        
        group_stats = grouped.apply(calculate_group_stats)
        return df.merge(group_stats, left_on='Label', right_index=True)

    def analyse_sensor_gaps(self, df):
        def process_row(row):
            timestamps = row['Timestamps']
            granularity = row['Deduced_Granularity']
            
            if len(timestamps) < 2:
                return pd.Series({
                    'Small_Gap_Count': 0,
                    'Medium_Gap_Count': 0,
                    'Large_Gap_Count': 0,
                    'Total_Gaps': 0,
                    'Gap_Percentage': 0,
                    'Total_Gap_Size_Seconds': 0
                })
            
            granularity_parts = str(granularity).split()
            time_granularity = int(granularity_parts[0])
            time_unit = granularity_parts[1]
            granularity_in_seconds = time_granularity * {'minutes': 60, 'hours': 3600}.get(time_unit, 86400)
            
            time_diffs = np.diff(timestamps.astype(int)) / 1e9  # Convert to seconds
            expected_diff = granularity_in_seconds
            normalised_diffs = time_diffs / expected_diff
            
            small_gap = np.sum((normalised_diffs > 1.5) & (normalised_diffs <= 3))
            medium_gap = np.sum((normalised_diffs > 3) & (normalised_diffs <= 6))
            large_gap = np.sum(normalised_diffs > 6)
            total_gaps = small_gap + medium_gap + large_gap
            
            time_delta_seconds = (timestamps.iloc[-1] - timestamps.iloc[0]).total_seconds()
            total_gap_intervals = sum(diff - 1 for diff in normalised_diffs if diff > 1.5)
            total_gap_size_seconds = total_gap_intervals * granularity_in_seconds
            gap_percentage = total_gap_size_seconds / time_delta_seconds if time_delta_seconds > 0 else 0.0
            
            return pd.Series({
                'Small_Gap_Count': small_gap,
                'Medium_Gap_Count': medium_gap,
                'Large_Gap_Count': large_gap,
                'Total_Gaps': total_gaps,
                'Gap_Percentage': gap_percentage,
                'Total_Gap_Size_Seconds': total_gap_size_seconds
            })
        
        return df.apply(process_row, axis=1)

    def prepare_data_quality_df(self, sensor_df):
        data_quality_df = sensor_df[[
            'StreamID', 'Label', 'Deduced_Granularity', 'Value_Count', 'Outliers',
            'Missing', 'Zeros', 'Small_Gap_Count', 'Medium_Gap_Count', 'Large_Gap_Count',
            'Total_Gaps', 'Gap_Percentage', 'Total_Gap_Size_Seconds', 'Group_Mean', 'Group_Std', 
            'Sensor_Mean', 'Sensor_Max', 'Sensor_Min', 'Start_Timestamp', 'End_Timestamp'
        ]].copy()
        
        data_quality_df['Outlier_Count'] = data_quality_df['Outliers'].apply(lambda x: x[0])
        data_quality_df['Time_Delta'] = data_quality_df['End_Timestamp'] - data_quality_df['Start_Timestamp']
        data_quality_df['Total_Time_Delta_Seconds'] = data_quality_df['Time_Delta'].dt.total_seconds().round().astype(int)
        
        # Calculate FlaggedForRemoval based on group statistics
        data_quality_df['FlaggedForRemoval'] = data_quality_df.groupby('Label').apply(
            lambda group: (
                (group['Sensor_Mean'] < (group['Group_Mean'] - 3 * group['Group_Std'])) | 
                (group['Sensor_Mean'] > (group['Group_Mean'] + 3 * group['Group_Std']))
            ).astype(int)
        ).reset_index(level=0, drop=True)
        
        # Skip flagging for groups with std = 0
        data_quality_df.loc[data_quality_df['Group_Std'] == 0, 'FlaggedForRemoval'] = 0
        
        for col in ['Group_Mean', 'Group_Std', 'Sensor_Mean', 'Sensor_Max', 'Sensor_Min']:
            data_quality_df[col] = data_quality_df[col].astype(float)
        
        return data_quality_df

    def create_summary_table(self, data_quality_df):
        def sum_timedelta(x):
            if pd.api.types.is_timedelta64_dtype(x):
                return x.sum()
            else:
                return pd.to_timedelta(x).sum()

        summary_table = data_quality_df.groupby('Label').agg({
            'StreamID': 'count',
            'Deduced_Granularity': lambda x: stats.mode(x)[0][0],
            'Start_Timestamp': 'min',
            'End_Timestamp': 'max',
            'Value_Count': 'sum',
            'Outlier_Count': 'sum',
            'Missing': 'sum',
            'Zeros': 'sum',
            'Small_Gap_Count': 'sum',
            'Medium_Gap_Count': 'sum',
            'Large_Gap_Count': 'sum',
            'Total_Gaps': 'sum',
            'Group_Mean': 'first',
            'Group_Std': 'first',
            'FlaggedForRemoval': 'sum',  # Add this line to sum FlaggedForRemoval
            'Time_Delta': sum_timedelta,
            'Total_Gap_Size_Seconds': 'sum',
            'Total_Time_Delta_Seconds': 'sum'
        }).reset_index()
        
        # Calculate Total_Gap_Percent
        summary_table['Total_Gap_Percent'] = (summary_table['Total_Gap_Size_Seconds'] / summary_table['Total_Time_Delta_Seconds']).fillna(0)
        
        # Format Total_Gap_Percent as percentage string
        summary_table['Total_Gap_Percent'] = summary_table['Total_Gap_Percent'].apply(lambda x: f"{x:.2%}")
        
        # Format Time_Delta as string
        summary_table['Total_Time_Delta'] = summary_table['Time_Delta'].apply(lambda x: str(x))
        
        # Rename columns for clarity
        summary_table = summary_table.rename(columns={
            'StreamID': 'Stream_Count',
            'Deduced_Granularity': 'Common_Granularity',
            'Start_Timestamp': 'Earliest_Timestamp',
            'End_Timestamp': 'Latest_Timestamp',
            'Value_Count': 'Total_Value_Count',
            'Outlier_Count': 'Total_Outliers',
            'Missing': 'Total_Missing',
            'Zeros': 'Total_Zeros',
            'Small_Gap_Count': 'Total_Small_Gaps',
            'Medium_Gap_Count': 'Total_Medium_Gaps',
            'Large_Gap_Count': 'Total_Large_Gaps',
            'FlaggedForRemoval': 'Total_Flagged_For_Removal',
            'Time_Delta': 'Total_Time_Delta'
        })
        
        # Reorder columns
        column_order = [
            'Label', 'Stream_Count', 'Common_Granularity', 'Earliest_Timestamp', 'Latest_Timestamp',
            'Total_Value_Count', 'Total_Outliers', 'Total_Missing', 'Total_Zeros',
            'Group_Mean', 'Group_Std',
            'Total_Small_Gaps', 'Total_Medium_Gaps', 'Total_Large_Gaps', 'Total_Gaps',
            'Total_Time_Delta_Seconds', 'Total_Gap_Size_Seconds', 'Total_Gap_Percent', 
            'Total_Flagged_For_Removal'
        ]
        summary_table = summary_table[column_order]
        
        # Sort the summary table by Total_Gap_Percent in descending order
        summary_table_sorted = summary_table.sort_values('Total_Gap_Percent', ascending=False)
        
        return summary_table_sorted

    def get_data_quality_overview(self, data_quality_df):
        """Generate overview analysis of data quality."""
        return {
            "PieChartAndTable": {
                "title": "Data Quality Overview",
                "pie_charts": [
                    {
                        "title": "Proportion of Streams with Outliers",
                        "labels": "has_outliers",
                        "textinfo": "percent+label",
                        "dataframe": data_quality_df['Outlier_Count'].apply(lambda x: 'Has Outliers' if x > 0 else 'No Outliers')
                    },
                    {
                        "title": "Proportion of Streams with Missing Data",
                        "labels": "has_missing",
                        "textinfo": "percent+label",
                        "dataframe": data_quality_df['Missing'].apply(lambda x: 'Has Missing' if x > 0 else 'No Missing')
                    }
                ],
                "tables": [
                    {
                        "title": "Data Quality Summary",
                        "columns": ["Metric", "Value"],
                        "rows": [
                            ["Total Streams", len(data_quality_df)],
                            ["Streams with Outliers", (data_quality_df['Outlier_Count'] > 0).sum()],
                            ["Streams with Missing Data", (data_quality_df['Missing'] > 0).sum()],
                            ["Streams with Zeros", (data_quality_df['Zeros'] > 0).sum()]
                        ]
                    }
                ]
            }
        }

    def get_summary_analysis(self, summary_table_df):
        """Generate summary analysis."""
        return {
            "Table": {
                "title": "Data Quality Summary by Label",
                "columns": summary_table_df.columns.tolist(),
                "dataframe": summary_table_df
            }
        }

    @staticmethod
    def deduce_granularity(timestamps):
        time_diffs = np.diff(timestamps).astype('timedelta64[s]').astype(int)  # in seconds
        if len(time_diffs) == 0:
            return None
        
        granularity = pd.Series(time_diffs).mode()[0]  # Most common time interval in seconds

        # Upgrade granularity
        if granularity % 86400 == 0:  # 86400 seconds = 1 day
            return f'{granularity // 86400} days'
        elif granularity % 3600 == 0:  # 3600 seconds = 1 hour
            return f'{granularity // 3600} hours'
        elif 0 <= granularity % 60 < 5:  # 60 seconds = 1 minute
            return f'{granularity // 60} minutes'        
        elif granularity % 60 > 55:
            return f'{1 + granularity // 60} minutes'        
        else:
            return f'{granularity} seconds'

    @staticmethod
    def detect_outliers(values):
        q1 = np.percentile(values, 25)
        q3 = np.percentile(values, 75)
        iqr = q3 - q1
        lower_bound = q1 - (1.5 * iqr)
        upper_bound = q3 + (1.5 * iqr)
        outliers = np.sum((values < lower_bound) | (values > upper_bound))
        return outliers, (lower_bound, upper_bound)

    def create_data_quality_plot(self, data_quality_df):
        return {
            "type": "table",
            "header": {
                "values": list(data_quality_df.columns),
                "align": "center",
                "line": {"width": 1, "color": "black"},
                "fill": {"color": "grey"},
                "font": {"family": "Arial", "size": 12, "color": "white"}
            },
            "cells": {
                "values": data_quality_df.values.T,
                "align": "center",
                "line": {"color": "black", "width": 1},
                "font": {"family": "Arial", "size": 11, "color": ["black"]}
            }
        }

    def create_summary_plot(self, summary_table_df):
        return {
            "type": "table",
            "header": {
                "values": list(summary_table_df.columns),
                "align": "center",
                "line": {"width": 1, "color": "black"},
                "fill": {"color": "grey"},
                "font": {"family": "Arial", "size": 12, "color": "white"}
            },
            "cells": {
                "values": summary_table_df.values.T,
                "align": "center",
                "line": {"color": "black", "width": 1},
                "font": {"family": "Arial", "size": 11, "color": ["black"]}
            }
        }

    def save_data_quality_to_csv(self, data_quality_df):
        """Save the data quality DataFrame to a CSV file."""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        
        output_file_path = os.path.join(self.output_dir, 'data_quality_table.csv')
        data_quality_df.to_csv(output_file_path, index=False)
        print(f"Data quality table saved to {output_file_path}")

    def save_summary_table_to_csv(self, summary_table_df):
        """Save the summary table DataFrame to a CSV file."""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        
        output_file_path = os.path.join(self.output_dir, 'summary_table.csv')
        summary_table_df.to_csv(output_file_path, index=False)
        print(f"Summary table saved to {output_file_path}")

if __name__ == "__main__":
    # Example usage
    from your_database_module import Database
    db = Database()  # Initialize your database
    analytics = Analytics(db)
    results = analytics.run()
    print(results)
