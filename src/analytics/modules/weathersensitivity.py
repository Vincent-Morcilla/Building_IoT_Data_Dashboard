"""
This module identifies systems in the building model that have an associated 
usage metric (energy, power, gas, water, chiller, and boiler). For each system,
the module calculates the weather sensitivity, defined as the correlation 
between outside air temperature and the system's usage over time.
The module returns a dictionary containing the weather sensitivity analysis 
results, which include the correlation values for each system and insights into 
how outside temperature may impact building system usage.
"""

import pandas as pd
from scipy import stats


def get_electric_energy_query_str():
    """
    Returns a SPARQL query string to retrieve information about electrical energy sensors
    and their associated meters in the building model.

    Returns:
        str: A SPARQL query string that retrieves information about electrical energy sensors
             and meters, ordered by meter.
    """

    return """
            SELECT ?meter ?sensor ?stream_id ?phase_count ?phases ?unit ?power_complexity ?power_flow
            WHERE {
                ?sensor rdf:type brick:Electrical_Energy_Sensor .
                ?meter rdf:type brick:Electrical_Meter .
                ?sensor brick:isPointOf ?meter .
                ?sensor senaps:stream_id ?stream_id .
                OPTIONAL { ?sensor brick:electricalPhaseCount [ brick:value ?phase_count ] . }
                OPTIONAL { ?sensor brick:electricalPhases [ brick:value ?phases ] . }
                OPTIONAL { ?sensor brick:hasUnit [ brick:value ?unit ] . }
                OPTIONAL { ?sensor brick:powerComplexity [ brick:value ?power_complexity ] . }
                OPTIONAL { ?sensor brick:powerFlow [ brick:value ?power_flow ] . }
            }
            ORDER BY ?meter
            """


def get_electric_power_query_str():
    """
    Returns a SPARQL query string to retrieve information about electrical power sensors
    and their associated meters in the building model.

    Returns:
        str: A SPARQL query string that retrieves information about electrical power sensors
             and meters, ordered by meter.
    """

    return """
            SELECT ?meter ?sensor ?stream_id ?phase_count ?phases ?unit ?power_complexity ?power_flow
            WHERE {
                ?sensor rdf:type brick:Electrical_Power_Sensor .
                ?meter rdf:type brick:Electrical_Meter .
                ?sensor brick:isPointOf ?meter .
                ?sensor senaps:stream_id ?stream_id .
                OPTIONAL { ?sensor brick:electricalPhaseCount [ brick:value ?phase_count ] . }
                OPTIONAL { ?sensor brick:electricalPhases [ brick:value ?phases ] . }
                OPTIONAL { ?sensor brick:hasUnit [ brick:value ?unit ] . }
                OPTIONAL { ?sensor brick:powerComplexity [ brick:value ?power_complexity ] . }
                OPTIONAL { ?sensor brick:powerFlow [ brick:value ?power_flow ] . }
            }
            ORDER BY ?meter
            """


def get_gas_query_str():
    """
    Returns a SPARQL query string to retrieve information about gas usage sensors
    and their associated gas meters in the building model.

    Returns:
        str: A SPARQL query string that retrieves information about gas usage sensors
             and their associated meters, ordered by meter.
    """

    return """
            SELECT ?meter ?sensor ?stream_id
            WHERE {
                ?sensor rdf:type brick:Usage_Sensor .
                ?meter rdf:type brick:Building_Gas_Meter .
                ?sensor brick:isPointOf ?meter .
                ?sensor senaps:stream_id ?stream_id
            }
            ORDER BY ?meter
        """


def get_chiller_query_str():
    """
    Returns a SPARQL query string to retrieve information about chilled water
    differential temperature sensors and their associated chillers in the building model.

    Returns:
        str: A SPARQL query string that retrieves information about chilled water differential
             temperature sensors and their associated chillers, ordered by chiller.
    """

    return """
            SELECT ?meter ?sensor ?stream_id
            WHERE {
                ?sensor rdf:type brick:Chilled_Water_Differential_Temperature_Sensor .
                ?meter rdf:type brick:Chiller .
                ?sensor brick:isPointOf ?meter .
                ?sensor senaps:stream_id ?stream_id
            }
            ORDER BY ?meter
            """


def get_water_query_str():
    """
    Returns a SPARQL query string to retrieve information about water usage sensors
    and their associated water meters in the building model.

    Returns:
        str: A SPARQL query string that retrieves information about water usage sensors
             and their associated meters, ordered by meter.
    """

    return """
            SELECT ?meter ?sensor ?stream_id
            WHERE {
                ?sensor rdf:type brick:Usage_Sensor .
                ?meter rdf:type brick:Building_Water_Meter .
                ?sensor brick:isPointOf ?meter .
                ?sensor senaps:stream_id ?stream_id
            }
            ORDER BY ?meter
            """


def get_boiler_query_str():
    """
        Returns a SPARQL query string to retrieve information about water temperature sensors
    and their associated hot water systems (boilers) in the building model.

    Returns:
        str: A SPARQL query string that retrieves information about water temperature sensors
    and their associated hot water systems, ordered by meter.
    """

    return """
            SELECT ?meter ?sensor ?stream_id
            WHERE {
                ?sensor rdf:type brick:Water_Temperature_Sensor .
                ?meter rdf:type brick:Hot_Water_System .
                ?sensor brick:isPointOf ?meter .
                ?sensor senaps:stream_id ?stream_id
            }
            ORDER BY ?meter
            """


def get_outside_air_temperature_query_str():
    """
    Returns a SPARQL query string to retrieve information about outside air temperature sensors
    and their associated weather stations in the building model.

    Returns:
    str: A SPARQL query string that retrieves information about outside air temperature sensors
    and their associated weather stations, ordered by stream ID.
    """

    return """
        SELECT ?sensor ?stream_id 
        WHERE {
            ?sensor rdf:type brick:Outside_Air_Temperature_Sensor .
            ?sensor brick:isPointOf   ?loc .
            ?loc a brick:Weather_Station .
            ?sensor senaps:stream_id ?stream_id .
        }
        ORDER BY ?stream_id
        """


class WeatherSensitivity:
    """Class which encapsulate all functionalaity to evaluate Weather Sensitivity"""

    def __init__(self, db):
        self.db = db
        self.rdf_data = None

    def load_sensors_from_db(self, df):
        """
        Load sensor data from the database for each stream ID in the provided DataFrame.

        This method uses the DBManager instance to retrieve sensor data for each stream ID in the
        DataFrame `df`. For each stream ID, the corresponding sensor information, including sensor
        type, timestamps, and values, is fetched and stored in a new `sensor_data` column in `df`.

        Args:
            df (pd.DataFrame): A DataFrame containing a column named 'stream_id', where each stream ID
                            corresponds to a sensor in the database.

        Returns:
            pd.DataFrame: The input DataFrame `df` with an added `sensor_data` column, where each entry
                        contains a dictionary of sensor information (`streamid`, `sensor_type`,
                        `timestamps`, `values`) or `None` if the sensor data is missing or could not
                        be retrieved.
        """
        # Ensure that both StreamID columns are strings
        df["stream_id"] = df["stream_id"].astype(str).str.lower()

        # Function to retrieve sensor data from the database for a given stream ID
        def get_sensor_data_for_stream(stream_id):
            if pd.isna(stream_id):  # Handle missing stream_id
                # print(f"Stream ID is missing: {stream_id}")
                return None

            # Fetch the sensor data from the database using the provided stream ID
            try:
                sensor_df = self.db.get_stream(stream_id).dropna()
                if not sensor_df.empty:
                    return {
                        "streamid": stream_id,
                        "sensor_type": sensor_df["brick_class"].iloc[
                            0
                        ],  # Assuming label is the sensor type
                        "timestamps": pd.to_datetime(sensor_df["time"]),
                        "values": sensor_df["value"],
                    }
                else:
                    print(f"No data found for Stream ID: {stream_id}")
                    return None
            except Exception as e:
                print(f"Error loading data for Stream ID {stream_id}: {e}")
                return None

        # Apply the function to load sensor data for each stream ID
        df["sensor_data"] = df["stream_id"].apply(get_sensor_data_for_stream)
        return df

    def get_data_from_rdf(self):
        """
        Retrieves data from the RDF database and stores it in a dictionary.

        This method queries the database for various types of data, including:
        - Electric energy consumption
        - Electric power consumption
        - Gas consumption
        - Water consumption
        - Chiller operation data
        - Boiler operation data
        - Outside air temperature

        The data is retrieved as Pandas DataFrames and stored in the `self.rdf_data` dictionary,
        with the keys corresponding to the data types.

        Returns:
            None
        """

        df_electric_energy = self.db.query(
            get_electric_energy_query_str(), return_df=True
        )
        df_electric_power = self.db.query(
            get_electric_power_query_str(), return_df=True
        )
        df_gas = self.db.query(get_gas_query_str(), return_df=True)
        df_water = self.db.query(get_water_query_str(), return_df=True)
        df_chiller = self.db.query(get_chiller_query_str(), return_df=True)
        df_boiler = self.db.query(get_boiler_query_str(), return_df=True)

        df_outside_air_temp = self.db.query(
            get_outside_air_temperature_query_str(), return_df=True
        )

        self.rdf_data = {
            "electric_energy": df_electric_energy,
            "electric_power": df_electric_power,
            "gas": df_gas,
            "water": df_water,
            "chiller": df_chiller,
            "boiler": df_boiler,
            "outside_temp": df_outside_air_temp,
        }

    def get_sensor_data(self):
        """
        Retrieves sensor data from the previously loaded RDF data.

        This method iterates through the data stored in `self.rdf_data` and uses the
        `load_sensors_from_db()` method to load the sensor data for each data type.

        The sensor data is stored in a dictionary, where the keys correspond to the
        data types (e.g., "electric_energy", "electric_power", "gas", "water", "chiller",
        "boiler", "outside_temp") and the values are the corresponding sensor data.

        Returns:
            dict: A dictionary containing the sensor data for each data type.
        """
        sensor_data = {}
        if self.rdf_data:
            for sensor, data in self.rdf_data.items():
                sensor_data[sensor] = self.load_sensors_from_db(data)

        return sensor_data

    @classmethod
    def get_daily_median_outside_temperature(cls, df_outside_air_temp_data):
        """
        Calculates the daily median outside air temperature from the provided data.

        This function takes a dictionary of outside air temperature data, which is
        expected to have a "sensor_data" key that contains a list of dictionaries,
        each with "timestamps" and "values" keys.

        Args:
            df_outside_air_temp_data (dict): A dictionary containing the outside
            air temperature data, with a "sensor_data" key.

        Returns:
            pd.DataFrame: A DataFrame with two columns: "date" and "outside_temp",
            containing the daily median outside air temperature.
        """
        df_outside_temperature = pd.DataFrame(
            df_outside_air_temp_data["sensor_data"][0]
        )
        df_outside_temperature["timestamps"] = pd.to_datetime(
            df_outside_temperature["timestamps"]
        )
        daily_median_outside_temperature = (
            df_outside_temperature.groupby(
                df_outside_temperature["timestamps"].dt.date
            )["values"]
            .median()
            .reset_index()
        )
        daily_median_outside_temperature.columns = ["date", "outside_temp"]
        return daily_median_outside_temperature

    @classmethod
    def get_daily_median_sensor_data(cls, df_sensors_data):
        """
        Calculates the daily median values for multiple sensors and combines
        the results into a single DataFrame.

        This function takes a dictionary of sensor data, which is expected to
        have a "sensor_data" key that contains a list of dictionaries, each with
        "timestamps" and "values" keys.

        Args:
            df_sensors_data (dict): A dictionary containing the sensor data, with
            a "sensor_data" key.

        Returns:
            pd.DataFrame: A DataFrame with columns for the daily median values of
            each sensor, indexed by date.
        """
        sensor_data = []
        for i in range(df_sensors_data.shape[0]):
            sensor_data.append(pd.DataFrame(df_sensors_data["sensor_data"][i]))

        daily_median_sensors_data = []
        for _, sd in enumerate(sensor_data):
            df_each_sensor_data = sd
            df_each_sensor_data["timestamps"] = pd.to_datetime(
                df_each_sensor_data["timestamps"]
            )
            df_daily_median_sensor = (
                df_each_sensor_data.groupby(df_each_sensor_data["timestamps"].dt.date)[
                    "values"
                ]
                .median()
                .reset_index()
            )
            daily_median_sensors_data.append(df_daily_median_sensor)
        df_sensor_data_combined = daily_median_sensors_data[0].copy()
        df_sensor_data_combined.columns = ["date", "sensor1"]
        # Loop through the remaining dataframes and merge them
        for i, df in enumerate(daily_median_sensors_data[1:], start=2):
            df.columns = ["date", f"sensor{i}"]
            df_sensor_data_combined = pd.merge(
                df_sensor_data_combined, df, on="date", how="outer"
            )
        return df_sensor_data_combined

    @classmethod
    def combine_meter_outside_temp_data(cls, df_meters_data, df_outside_temp):
        """
        Combines meter data with outside temperature data, merging the data on
        the "date" column.

        Args:
            df_meters_data (pd.DataFrame): A DataFrame containing meter data,
            with a "date" column.
            df_outside_temp (pd.DataFrame): A DataFrame containing outside
            temperature data, with a "date" column.

        Returns:
            pd.DataFrame: A DataFrame containing the combined meter and outside
            temperature data, with the "date" column converted to datetime.
        """
        df_meter_outside_temperature_data = df_meters_data.merge(
            df_outside_temp, on="date", how="inner"
        )
        df_meter_outside_temperature_data["date"] = pd.to_datetime(
            df_meter_outside_temperature_data["date"]
        )
        return df_meter_outside_temperature_data

    @classmethod
    def get_weather_sensitivity(cls, df_sensor_outside_data):
        """
        Calculates the monthly Spearman correlation between sensor data and outside temperature.

        This function takes a DataFrame containing sensor data and outside temperature data,
        with a 'date' column. It calculates the monthly Spearman correlation between each
        sensor column and the outside temperature column, and returns a DataFrame with the results.

        Args:
            df_sensor_outside_data (pd.DataFrame): A DataFrame containing sensor data and outside
            temperature data, with a 'date' column.

        Returns:
            pd.DataFrame: A DataFrame with columns for each sensor, containing the monthly
            Spearman correlation between the sensor data and outside temperature.
        """
        # Create the year_month column
        df_sensor_outside_data["year_month"] = df_sensor_outside_data[
            "date"
        ].dt.to_period("M")

        def calculate_monthly_correlation(dataframe, sensor_column):
            def correlation_or_nan(sub_df):
                if (
                    sub_df["outside_temp"].nunique() <= 1
                    or sub_df[sensor_column].nunique() <= 1
                ):
                    return float("nan")
                return stats.spearmanr(sub_df["outside_temp"], sub_df[sensor_column])[0]

            return dataframe.groupby("year_month").apply(correlation_or_nan)

        sensor_columns = {}
        for i in range(1, df_sensor_outside_data.shape[1] - 2):
            if i <= 9:
                sensor_columns[f"sensor{i}"] = f"sensor0{i}"
            else:
                sensor_columns[f"sensor{i}"] = f"sensor{i}"

        monthly_correlations = {}

        for sensor, sensor_id in sensor_columns.items():
            monthly_correlations[sensor_id] = calculate_monthly_correlation(
                df_sensor_outside_data, sensor
            )

        # Convert results to a dataframe
        result_df = pd.DataFrame(monthly_correlations)

        # Reset the index to make year_month a column
        result_df = result_df.reset_index()
        result_df["date"] = result_df["year_month"].dt.to_timestamp()

        return result_df

    @classmethod
    def get_daily_median_data(cls, sensors_data):
        """
        Calculates the daily median values for each sensor and outside temperature
        data by calling two functions.

        Args:
            sensors_data (dict): A dictionary of sensor data, with sensor names as keys
            and sensor data as values.

        Returns:
            dict: A dictionary containing the daily median data for each sensor, with
            sensor names as keys and DataFrames as values.
        """
        sensors_daily_median_data = {}
        for sensor in sensors_data.keys():
            if sensor == "outside_temp":
                df = WeatherSensitivity.get_daily_median_outside_temperature(
                    sensors_data[sensor]
                )
                sensors_daily_median_data[sensor] = df
            else:
                df = WeatherSensitivity.get_daily_median_sensor_data(
                    sensors_data[sensor]
                )
                sensors_daily_median_data[sensor] = df
        return sensors_daily_median_data

    @classmethod
    def combine_meter_weather_data(cls, sensors_data):
        """
        Combines meter data with outside temperature data for weather sensitivity analysis.

        Args:
        sensors_data : dict
            A dictionary containing sensor data

        Returns
        dict
            A dictionary where:
            - Keys are the original meter keys (excluding 'outside_temp')
            - Values are DataFrames containing combined meter and temperature data

        """
        df_outside_temp = sensors_data["outside_temp"]
        combine_meter_outside_data = {}
        for meter in sensors_data.keys():
            if meter == "outside_temp":
                continue
            combine_meter_outside_data[meter] = (
                WeatherSensitivity.combine_meter_outside_temp_data(
                    sensors_data[meter], df_outside_temp
                )
            )
        return combine_meter_outside_data

    @classmethod
    def get_weather_sensitivity_results(cls, combine_meter_outside_data):
        """
        Calculates weather sensitivity results for multiple meters.

        Args:
        combine_meter_outside_data : dict
            Dictionary containing combined meter and temperature data where:
            - Keys are meter identifiers
            - Values are DataFrames with combined meter and temperature data

        Returns
        dict
            Dictionary containing weather sensitivity results where:
            - Keys are the original meter identifiers
            - Values are DataFrames with weather sensitivity calculations
            - NaN values are filled with 0
        """
        weather_sensitivity_results = {}
        for meter in combine_meter_outside_data.keys():
            weather_sensitivity_results[meter] = (
                WeatherSensitivity.get_weather_sensitivity(
                    combine_meter_outside_data[meter]
                ).fillna(0)
            )
        return weather_sensitivity_results

    @classmethod
    def transpose_dataframe_for_vis(cls, df):
        """
        Transforms a sensor data DataFrame into a format suitable for visualization.

        Args:
        df : pandas.DataFrame

        Returns
        pandas.DataFrame
            Transformed DataFrame with columns:
            - 'Date': Timestamp of the measurement
            - 'Sensor ID': Identifier of the sensor
            - 'Correlation': Correlation value for that sensor and date
        """
        df = df.reset_index(drop=True)
        sensor_cols = [col for col in df.columns if "sensor" in col.lower()]

        df_vis = pd.melt(
            df,
            id_vars=["date"],
            value_vars=sensor_cols,
            var_name="sensor",
            value_name="Correlation",
        )
        df_vis = df_vis.sort_values(["date", "sensor"]).reset_index(drop=True)
        df_vis.columns = ["Date", "Sensor ID", "Correlation"]
        return df_vis

    @classmethod
    def prepare_data_for_vis(cls, df, meter, title):
        """
        Prepares sensor correlation data for heatmap visualization.

        Args:
            df (pd.DataFrame): DataFrame with Date, Sensor ID, and Correlation columns
            meter (str): Meter identifier for plot labels
            title (str): Plot title

        Returns:
            dict: Plotly heatmap configuration dictionary
        """
        df["Sensor ID"] = df["Sensor ID"].str.replace(r"sensor(\d+)", r"\1", regex=True)
        return {
            "type": "plot",
            "library": "go",
            "function": "Heatmap",
            "id": f"ws-heatmap-{meter}",
            "data_frame": df,
            "trace_type": "Heatmap",
            "data_mappings": {
                "x": "Date",
                "y": "Sensor ID",
                "z": "Correlation",
            },
            "kwargs": {
                "colorscale": "Viridis",
                "colorbar": {
                    "title": "Correlation",
                    "orientation": "h",
                    "yanchor": "bottom",
                    "y": -0.7,
                    "xanchor": "center",
                    "x": 0.5,
                    "title_side": "bottom",
                },
                "zmin": -1,
                "zmax": 1,
            },
            "layout_kwargs": {
                "title": {
                    "text": title,
                    "x": 0.5,
                    "y": 0.9,
                    "xanchor": "center",
                },
                "xaxis_title": "Date",
                "yaxis_title": f"{meter.title().replace('_',' ')} Sensor",
                "font_color": "black",
                "plot_bgcolor": "white",
                "xaxis": {
                    "mirror": True,
                    "ticks": "outside",
                    "showline": True,
                    "linecolor": "black",
                    "gridcolor": "lightgrey",
                },
                "yaxis": {
                    "mirror": True,
                    "ticks": "outside",
                    "showline": True,
                    "linecolor": "black",
                    "gridcolor": "lightgrey",
                },
            },
            "css": {
                # "padding": "10px",
                "width": "49%",
                "display": "inline-block",
            },
        }

    @classmethod
    def get_data_for_dash(cls, weather_sensitivity_results):
        """
        Creates dashboard configuration from weather sensitivity results.

        Args:
            weather_sensitivity_results (dict): Dictionary of meter correlation DataFrames

        Returns:
            dict: Dashboard configuration with heatmap visualizations
        """
        data_for_vis = {}
        result = []
        for meter in weather_sensitivity_results.keys():
            transpose_df = WeatherSensitivity.transpose_dataframe_for_vis(
                weather_sensitivity_results[meter]
            )
            df_vis = WeatherSensitivity.prepare_data_for_vis(
                transpose_df,
                meter,
                # f"Correlation between {meter.title().replace('_',' ')} Usage and Outside Temperature",
                f"{meter.title().replace('_',' ')} Usage and Outside Temperature",
            )
            result.append(df_vis)
        data_for_vis[("WeatherSensitivity", "Correlation Analysis")] = {
            "components": result,
        }
        return data_for_vis

    def get_weather_sensitivity_data(self):
        """
        Calculates weather sensitivity from sensor data and prepares dashboard visualizations.

        Returns:
            dict: Dashboard configuration with weather sensitivity analysis
        """
        self.get_data_from_rdf()
        sensors_data = self.get_sensor_data()
        sensors_daily_median_data = WeatherSensitivity.get_daily_median_data(
            sensors_data
        )
        combine_meter_outside_data = WeatherSensitivity.combine_meter_weather_data(
            sensors_daily_median_data
        )
        weather_sensitivity_results = (
            WeatherSensitivity.get_weather_sensitivity_results(
                combine_meter_outside_data
            )
        )
        return WeatherSensitivity.get_data_for_dash(weather_sensitivity_results)


def run(db):
    """Entry point for the module which encapuslate all the functionality"""
    ws = WeatherSensitivity(db)
    data = ws.get_weather_sensitivity_data()
    return data
