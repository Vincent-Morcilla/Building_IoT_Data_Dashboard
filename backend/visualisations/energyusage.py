import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

class EnergyUsage:
    def __init__(self, db, g):
        self.db = db
        self.g = g

    def get_electrical_meters(self, meter_type, sensor_type):
        # Define the SPARQL query with meter_type and sensor_type as variables
        query = f"""
            SELECT ?meter ?sensor ?stream_id ?phase_count ?phases ?unit ?power_complexity ?power_flow
            WHERE {{
            ?sensor rdf:type brick:{sensor_type} .
            ?meter rdf:type brick:{meter_type} .
            ?sensor brick:isPointOf ?meter .
            ?sensor senaps:stream_id ?stream_id .
            OPTIONAL {{ ?sensor brick:electricalPhaseCount [ brick:value ?phase_count ] . }}
            OPTIONAL {{ ?sensor brick:electricalPhases [ brick:value ?phases ] . }}
            OPTIONAL {{ ?sensor brick:hasUnit [ brick:value ?unit ] . }}
            OPTIONAL {{ ?sensor brick:powerComplexity [ brick:value ?power_complexity ] . }}
            OPTIONAL {{ ?sensor brick:powerFlow [ brick:value ?power_flow ] . }}
        }}
        ORDER BY ?meter
        """
        
        # Run the query
        results = self.g.query(query)

        # Prepare the data for the DataFrame
        data = []
        for row in results:
            data.append({
                'Meter': str(row['meter']),
                'Sensor': str(row['sensor']),
                'Stream ID': str(row['stream_id']),
                'Phase Count': str(row['phase_count']) if row['phase_count'] else None,
                'Phases': str(row['phases']) if row['phases'] else None,
                'Unit': str(row['unit']) if row['unit'] else None,
                'Power Complexity': str(row['power_complexity']) if row['power_complexity'] else None,
                'Power Flow': str(row['power_flow']) if row['power_flow'] else None
            })

        # Create a DataFrame from the results
        df_meters = pd.DataFrame(data)
        return df_meters

    def load_sensors_from_db(self, df):
        """
        Load the sensor data corresponding to the stream IDs in the DataFrame using the DBManager instance.
        """
        # Ensure that both StreamID columns are strings
        df['Stream ID'] = df['Stream ID'].astype(str).str.lower()
    
        # Function to retrieve sensor data from the database for a given stream ID
        def get_sensor_data_for_stream(stream_id):
            if pd.isna(stream_id):  # Handle missing stream_id
                print(f"Stream ID is missing: {stream_id}")
                return None
            
            # Fetch the sensor data from the database using the provided stream ID
            try:
                sensor_df = self.db.get_stream(stream_id).dropna()
                if not sensor_df.empty:
                    return {
                        'streamid': stream_id,
                        'sensor_type': sensor_df['label'].iloc[0],  # Assuming label is the sensor type
                        'timestamps': pd.to_datetime(sensor_df['time']),
                        'values': sensor_df['value']
                    }
                else:
                    print(f"No data found for Stream ID: {stream_id}")
                    return None
            except Exception as e:
                print(f"Error loading data for Stream ID {stream_id}: {e}")
                return None
    
        # Apply the function to load sensor data for each stream ID
        df['sensor_data'] = df['Stream ID'].apply(get_sensor_data_for_stream)
    
        return df

    def extract_identifier(self, uri):
        """
        Helper function to extract the identifier after the '#' in a URI.
        """
        return uri.split('#')[-1]

    def plot_sensor_data_grouped_by_power_complexity(self, df_with_sensor_data, plot_title):
        """
        Group the DataFrame by Power Complexity and combine time series values.
        """
        # Group the DataFrame by 'Power Complexity'
        grouped = df_with_sensor_data.groupby('Power Complexity')
        
        # Iterate over each Power Complexity group and plot the combined sensor data
        for power_complexity, group in grouped:
            plt.figure(figsize=(10, 6))  # Create a new figure for each power complexity group
            
            # Extract the power complexity identifier
            power_complexity_identifier = self.extract_identifier(power_complexity)
            
            combined_data = None  # Placeholder for combined time series data
            combined_unit = None  # Placeholder for the unit
            
            # Iterate through each sensor in the group and combine the time series
            for idx, row in group.iterrows():
                sensor_data = row['sensor_data']
                unit = row['Unit']  # Get the unit for this sensor
                
                if combined_unit is None:
                    combined_unit = unit  # Initialize the combined unit
                elif combined_unit != unit:
                    # Warn if units differ
                    print(f"Warning: Inconsistent units found for Power Complexity {power_complexity_identifier}")
                    combined_unit = "Mixed Units"
                
                if sensor_data:
                    # Convert sensor data into a DataFrame for easier time series operations
                    sensor_df = pd.DataFrame({
                        'timestamps': sensor_data['timestamps'],
                        'values': sensor_data['values']
                    })
                    
                    # Set timestamps as index and resample to 10-minute frequency, filling missing values
                    sensor_df.set_index('timestamps', inplace=True)
                    sensor_df = sensor_df.resample('10min').mean()
                    sensor_df['values'] = sensor_df['values'].interpolate(method='linear')  # Interpolate missing values
                    
                    # If combined_data is None, initialize it with the current sensor data
                    if combined_data is None:
                        combined_data = sensor_df
                    else:
                        # Combine with existing data by summing the values (aligned by timestamps)
                        combined_data = combined_data.add(sensor_df, fill_value=0)
            
            if combined_data is not None:
                # Plot the combined data
                timestamps = combined_data.index
                values = combined_data['values']
                plt.plot(timestamps, values, label=f"Power Complexity: {power_complexity_identifier}")
            
            # Set plot title and labels using the combined unit
            plt.title(f"{plot_title}: {power_complexity_identifier}")
            plt.xlabel("Timestamps")
            plt.ylabel(f"Values ({combined_unit})")  # Use the combined unit
            
            # Place the legend outside the plot, on the right side
            plt.legend(loc="upper left", bbox_to_anchor=(1, 1))
            
            # Show the plot
            plt.show()