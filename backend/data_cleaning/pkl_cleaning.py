import os
import numpy as np
import pandas as pd
import pickle
import json

def remove_outlier(data):
    """
    Removes outliers from the data based on 3 standard deviations from the mean.
    Replaces outliers with the mean value.
    
    Args:
    data (dict): Dictionary of DataFrames, each representing a sensor's data.
    
    Returns:
    tuple: (cleaned data, outlier information)
    """    
    outlier_info = {}
    for stream_id, df in data.items():
        mean = df['Value'].mean()
        std = df['Value'].std()
        lower_limit = mean - 3 * std
        upper_limit = mean + 3 * std

        # Find outliers and store their timestamps and values
        outliers = df[(df['Value'] < lower_limit) | (df['Value'] > upper_limit)]
        outlier_info[stream_id] = (len(outliers), list(zip(outliers.index, outliers['Value'])))

        # Replace outliers with the mean
        data[stream_id]['Value'] = data[stream_id]['Value'].apply(
            lambda x: mean if x < lower_limit or x > upper_limit else x
        )
    return data, outlier_info

def detect_missing_and_zeros(data):
    """
    Detects missing values and zero values for each stream.
    """
    quality_info = {}
    for stream_id, df in data.items():
        missing_count = df['Value'].isna().sum()
        zero_count = (df['Value'] == 0).sum()

        # Store missing and zero information for this stream
        quality_info[stream_id] = {
            'Missing': missing_count,
            'Zeros': zero_count
        }
    return quality_info

def deduce_granularity(timestamps):
    """
    Deduces the granularity of the time intervals in the time series data.
    Returns the most frequent time difference in an upgraded unit of time.
    """
    time_diffs = np.diff(timestamps).astype('timedelta64[s]').astype(int)  # in seconds
    if len(time_diffs) == 0:
        return None
    
    granularity = pd.Series(time_diffs).mode()[0]  # Most common time interval in seconds

    # Upgrade granularity
    if granularity % 86400 == 0:  # 86400 seconds = 1 day
        granularity = f'{granularity // 86400} days'
    elif granularity % 3600 == 0:  # 3600 seconds = 1 hour
        granularity = f'{granularity // 3600} hours'
    elif 0 <= granularity % 60 < 5:  # 60 seconds = 1 minute
        granularity = f'{granularity // 60} minutes'        
    elif granularity % 60 >55:
        granularity = f'{1 + granularity // 60} minutes'        
    else:
        granularity = f'{granularity} seconds'

    return granularity

def preprocess_to_sensor_rows(data, labels):
    """
    Preprocesses data and returns a DataFrame where each row represents a sensor with
    its stream ID, timestamp array, value array, deduced granularity, and data quality information.
    Also includes the 'y' label from the original data.
    """
    # Remove outliers and get outlier information
    data, outlier_info = remove_outlier(data)
    
    # Detect missing values and zeros
    quality_info = detect_missing_and_zeros(data)

    sensor_data = []
    
    # For each sensor (stream), process the data
    for stream_id, df in data.items():
        timestamps = df.index.to_numpy()  # Timestamp array
        values = df['Value'].to_numpy()  # Value array
        granularity = deduce_granularity(timestamps)  # Deduced granularity
        
        # Prepare the row for this sensor
        outlier_count, outliers = outlier_info[stream_id]  # Number of outliers and the list of outliers
        missing_count = quality_info[stream_id]['Missing']
        zero_count = quality_info[stream_id]['Zeros']
        label = labels[stream_id]  # Get the label for this stream

        # Add start and end timestamp
        start_timestamp = timestamps[0] if len(timestamps) > 0 else None
        end_timestamp = timestamps[-1] if len(timestamps) > 0 else None

        row = {
            'StreamID': stream_id,
            'Label': label,  # Add the 'y' label
            'Timestamps': timestamps,
            'Values': values,
            'Deduced_Granularity': granularity,
            'Outliers': [outlier_count, outliers],  # Number of outliers and the list of (timestamp, outlier)
            'Missing': missing_count,  # Number of missing values
            'Zeros': zero_count,  # Number of zero values
            'Start_Timestamp': start_timestamp,
            'End_Timestamp': end_timestamp,
            'Sensor_Mean': np.mean(values)  # Add the cleaned sensor mean
        }
        sensor_data.append(row)
    
    # Convert the sensor data to a DataFrame
    return pd.DataFrame(sensor_data)

def remove_outliers_from_group(group):
    """
    Removes outliers from a group based on its mean and standard deviation.
    Returns the cleaned group data.
    """
    group_mean = group['Sensor_Mean'].mean()
    group_std = group['Sensor_Mean'].std()

    # Define the bounds for outlier removal (within 3 standard deviations)
    lower_limit = group_mean - 3 * group_std
    upper_limit = group_mean + 3 * group_std

    # Filter out outliers from the group
    cleaned_group = group[(group['Sensor_Mean'] >= lower_limit) & (group['Sensor_Mean'] <= upper_limit)]

    return cleaned_group
    
def prepare_data_for_preprocessing(data):
    """
    Prepares raw data for preprocessing by creating DataFrames for each sensor
    and extracting labels.
    
    Args:
    data (list): List of dictionaries containing raw sensor data.
    
    Returns:
    tuple: (prepared data dictionary, labels dictionary)
    """    
    prepared_data = {}
    labels = {}  # To store the 'y' labels (Brick labels)
    
    for entry in data:
        # Access the 'data' field which holds the actual time-series data
        timestamps = pd.to_datetime(entry['data']['t'])  # Time-series timestamps
        values = entry['data']['v']  # Corresponding values
        stream_id = entry['StreamID']  # Use StreamID as the key
        
        # Create a DataFrame and set the index as Timestamp
        prepared_data[stream_id] = pd.DataFrame({'Timestamp': timestamps, 'Value': values}).set_index('Timestamp')
        
        # Use the Brick label (strBrickLabel) as the 'y' label for this stream
        labels[stream_id] = entry['strBrickLabel']
    
    return prepared_data, labels

def profile_groups(df):
    """
    Profiles each group of sensors based on their cleaned values.
    Flags sensors whose cleaned average values deviate significantly from the group mean.
    """
    # Add columns to store profiling results
    df['Group_Mean'] = np.nan
    df['Group_Std'] = np.nan
    df['FlaggedForRemoval'] = 0  # Default to 0 (not flagged)

    grouped = df.groupby('Label')  # Group by label (strBrickLabel)

    for label, group in grouped:
        # Calculate group-level statistics
        cleaned_values = np.concatenate(group['Values'].to_numpy())  # Aggregate cleaned values across streams
        group_mean = cleaned_values.mean()  # Mean of cleaned values
        group_std = cleaned_values.std()  # Standard deviation of cleaned values

        # Skip flagging if the group std is 0 - usually setpoints
        if group_std == 0:
            continue

        lower_limit = group_mean - 3 * group_std
        upper_limit = group_mean + 3 * group_std

        # Adorn each row in the group with the group statistics
        df.loc[group.index, 'Group_Mean'] = group_mean
        df.loc[group.index, 'Group_Std'] = group_std

        # Flag sensors whose cleaned means are outliers within the group
        df.loc[group.index, 'FlaggedForRemoval'] = df.loc[group.index, 'Sensor_Mean'].apply(
            lambda x: 1 if x < lower_limit or x > upper_limit else 0
        )

    return df

def categorise_gaps(normalised_diffs, total_counts, granularity_in_seconds):
    """
    Categorizes gaps in the time series data into small, medium, and large gaps.
    
    Args:
    normalised_diffs (array-like): normalised time differences.
    total_counts (int): Total number of timestamps.
    granularity_in_seconds (int): Granularity of the time series in seconds.
    
    Returns:
    tuple: Various gap statistics and indices.
    """
    small_gap_indices = [i for i, diff in enumerate(normalised_diffs) if 1.5 <= diff < 3 and i + 1 < total_counts]
    medium_gap_indices = [i for i, diff in enumerate(normalised_diffs) if 3 <= diff < 6 and i + 1 < total_counts]
    large_gap_indices = [i for i, diff in enumerate(normalised_diffs) if diff >= 6 and i + 1 < total_counts]

    all_gap_indices = sorted(small_gap_indices + medium_gap_indices + large_gap_indices)

    small_gap = len(small_gap_indices)
    medium_gap = len(medium_gap_indices)
    large_gap = len(large_gap_indices)
    normal = total_counts - (small_gap + medium_gap + large_gap)
    total_gaps = small_gap + medium_gap + large_gap
    gap_percentage = (total_gaps / total_counts) * 100 if total_counts > 0 else 0
    
    small_gap_interp_count = sum([int(np.floor(diff)) for diff in normalised_diffs if 1.5 <= diff < 3])
    medium_gap_interp_count = sum([int(np.floor(diff)) for diff in normalised_diffs if 3 <= diff < 6])
    large_gap_interp_count = sum([int(np.floor(diff)) for diff in normalised_diffs if diff >= 6])

    return (small_gap, medium_gap, large_gap, normal, total_gaps, gap_percentage, 
            all_gap_indices, small_gap_indices, medium_gap_indices, large_gap_indices, 
            small_gap_interp_count, medium_gap_interp_count, large_gap_interp_count)


def insert_new_timestamps_and_interpolate(timestamps, values, granularity_in_seconds, gap_indices, normalized_diffs):
    timestamps_series = pd.to_datetime(timestamps)
    values_series = pd.Series(values, index=timestamps_series)

    print("Debugging information:")
    print(f"Length of timestamps_series: {len(timestamps_series)}")
    print(f"gap_indices: {gap_indices}")
    print(f"Type of gap_indices: {type(gap_indices)}")
    print(f"Content of gap_indices: {list(gap_indices)}")

    # Get the pairs of timestamps where gaps start and end
    gap_start_times = timestamps_series[gap_indices]

    # Ensure we don't go out of bounds when getting end times
    valid_end_indices = [i for i in gap_indices if i + 1 < len(timestamps_series)]
    print(f"valid_end_indices: {valid_end_indices}")
    print(f"Type of valid_end_indices: {type(valid_end_indices)}")

    # Check if there are any valid indices to avoid further errors
    if not valid_end_indices:
        print("No valid end indices found. Skipping interpolation.")
        return timestamps_series, values_series, pd.Series([False] * len(timestamps_series), index=timestamps_series)

    try:
        gap_end_times = timestamps_series[np.array(valid_end_indices) + 1]
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        print(f"np.array(valid_end_indices) + 1: {np.array(valid_end_indices) + 1}")
        print(f"Type of np.array(valid_end_indices) + 1: {type(np.array(valid_end_indices) + 1)}")
        raise

    # Generate new timestamps for each gap
    new_timestamps = []
    for start, end, diff in zip(gap_start_times, gap_end_times, normalized_diffs[valid_end_indices]):
        num_new_points = int(np.floor(diff)) 
        if num_new_points > 0:
            new_timestamps.extend(
                pd.date_range(start=start, end=end, periods=num_new_points + 2)[1:-1]
            )

    # Combine original and new timestamps, sort, and remove duplicates
    all_timestamps = pd.to_datetime(sorted(set(timestamps_series) | set(new_timestamps)))

    # Interpolate values for all timestamps
    new_values = values_series.reindex(all_timestamps).interpolate()

    # Create interpolated flags
    interpolated_flags = pd.Series(False, index=all_timestamps)
    interpolated_flags.loc[new_timestamps] = True

    return all_timestamps, new_values, interpolated_flags

def analyse_sensor_gaps(df):
    """
    Analyzes gaps in sensor data and performs interpolation.
    
    Args:
    df (DataFrame): DataFrame containing sensor data.
    
    Returns:
    DataFrame: Results of gap analysis and interpolation.
    """
    def process_row(row):
        granularity_parts = str(row['Deduced_Granularity']).split(" ")
        time_granularity = int(granularity_parts[0])
        time_unit = granularity_parts[1]

        time_granularity_seconds = {
            'minutes': 60,
            'hours': 3600,
        }.get(time_unit, 86400)

        granularity_in_seconds = time_granularity * time_granularity_seconds

        timestamps = pd.to_datetime(row['Timestamps'])

        time_diffs = np.diff(timestamps).astype('timedelta64[s]').astype(float) / granularity_in_seconds
        
        (small_gap, medium_gap, large_gap, normal, total_gaps, gap_percentage, 
         all_gap_indices, small_gap_indices, medium_gap_indices, large_gap_indices, 
         small_gap_interp_count, medium_gap_interp_count, large_gap_interp_count) = categorise_gaps(
            time_diffs, len(timestamps), granularity_in_seconds
        )
        print(row['StreamID'])
        new_timestamps, new_values, interpolated_flags = insert_new_timestamps_and_interpolate(
            timestamps, row['Values'], granularity_in_seconds, all_gap_indices, time_diffs
        )

        return (small_gap, medium_gap, large_gap, total_gaps, gap_percentage, 
                small_gap_interp_count, medium_gap_interp_count, large_gap_interp_count, 
                new_values, interpolated_flags, new_timestamps)

    gap_analysis = df.apply(process_row, axis=1, result_type="expand")

    gap_analysis.columns = [
        "Small_Gap_Count", "Medium_Gap_Count", "Large_Gap_Count", "Total_Gaps", 
        "Gap_Percentage", "Small_Gap_Interp_Count", "Medium_Gap_Interp_Count", "Large_Gap_Interp_Count",
        "Interpolated_Values", "Interpolated_Flags", "New_Timestamps"
    ]

    return gap_analysis
    
# Get the directory of the current script
current_script_dir = os.path.dirname(os.path.abspath(__file__))

# Navigate up to the project root
project_root = os.path.abspath(os.path.join(current_script_dir, '..', '..'))

# Load the CSV file containing the mappings
csv_file_path = os.path.join(project_root, 'datasets', 'bts_site_b_train', 'mapper_TrainOnly.csv')
mapping_df = pd.read_csv(csv_file_path)

# Dictionary to hold concatenated data
final_data = []

# Set the directory where the .pkl files are stored
pkl_directory = os.path.join(project_root, 'datasets', 'bts_site_b_train', 'train')

counter = 0
ingest_limit = 100
# Iterate through all .pkl files in the directory
for filename in os.listdir(pkl_directory):
    if counter <= ingest_limit:
        if filename.endswith('.pkl'):
            counter
            counter +=1
            # Load the .pkl file
            pkl_path = os.path.join(pkl_directory, filename)
            with open(pkl_path, 'rb') as f:
                data = pickle.load(f)
            
            # Find the corresponding row in the CSV based on the filename
            matching_row = mapping_df[mapping_df['Filename'] == filename]
            
            if not matching_row.empty:
                stream_id = matching_row['StreamID'].values[0]
                str_brick_label = matching_row['strBrickLabel'].values[0]

                # Append the data along with the StreamID and strBrickLabel to final_data
                final_data.append({
                    'StreamID': stream_id,
                    'strBrickLabel': str_brick_label,
                    'data': data  # Include the data from the .pkl file
                })
    else:
        break

def safe_json_serializable(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()  
    elif isinstance(obj, (list, tuple)):
        return [safe_json_serializable(item) for item in obj]  # Recursively handle lists and tuples
    elif isinstance(obj, dict):
        return {k: safe_json_serializable(v) for k, v in obj.items()}  # Recursively handle dictionaries
    else:
        return obj


# Preprocess the data
prepared_data, labels = prepare_data_for_preprocessing(final_data)

sensor_df = preprocess_to_sensor_rows(prepared_data, labels)

# Profile the groups and flag outliers
sensor_df = profile_groups(sensor_df)

gap_analysis_results = analyse_sensor_gaps(sensor_df)

# Append the results to the original sensor_df
sensor_df = pd.concat([sensor_df, gap_analysis_results], axis=1)

sensor_df['Outliers'] = sensor_df['Outliers'].apply(lambda x: json.dumps(safe_json_serializable(x)))
sensor_df['Interpolated_Values'] = sensor_df['Interpolated_Values'].apply(lambda x: x.tolist() if isinstance(x, pd.Series) else x)
sensor_df['Interpolated_Flags'] = sensor_df['Interpolated_Flags'].apply(lambda x: x.tolist() if isinstance(x, pd.Series) else x)
sensor_df['New_Timestamps'] = sensor_df['New_Timestamps'].apply(
    lambda x: [ts.strftime('%Y-%m-%d %H:%M:%S.%f') for ts in x] if isinstance(x, pd.DatetimeIndex) else x
)

current_directory = os.path.dirname(os.path.abspath(__file__))

# Define the output Parquet file name
output_file_name = 'sensor_data_analysis.parquet'

# Create the full path for the output Parquet file
output_file_path = os.path.join(current_directory, output_file_name)

# Save sensor_df to Parquet
sensor_df.to_parquet(output_file_path, index=False)

print(f"Data saved to {output_file_path}")
