import warnings
warnings.filterwarnings('ignore')

import rdflib
import brickschema
import plotly.express as px
import plotly.io as pio

import visualisations.roomtemp as roomtemp
import visualisations.energyusage as energyusage
import visualisations.utilitiesusage as utilitiesusage
import visualisations.TemperatureMonitor as temperaturemonitor


import numpy as np
import pandas as pd
from scipy import stats
from collections import Counter
from rdflib import Graph, Namespace, RDF, URIRef
from collections import deque

BRICK = Namespace("https://brickschema.org/schema/Brick#")

class VisManager:
    def __init__(self, db):
        self.db = db

        # Provided model (for Bassel)
        building_ttl_file = '../datasets/bts_site_b_train/Site_B.ttl'

        # Altered model (for Tim)
        building_tim_ttl_file = '../datasets/bts_site_b_train/Site_B_tim.ttl'
       
        # Load the Brick ontology into a separate graph
        self.brick_ontology = rdflib.Graph()
        self.brick_ontology.parse("../datasets/brick_ontology_file/Brick_v1.2.1.ttl", format="turtle")

        # Define categories for different entity types (using the ontology)
        self.categories = self.define_categories()

        # Provided graph (for Bassel)
        self.g = rdflib.Graph()
        self.g.parse(building_ttl_file)

        # Altered graph (for Tim)
        self.g_tim = brickschema.Graph(load_brick=True)
        self.g_tim.load_file(building_tim_ttl_file)
        self.g_tim.expand(profile="rdfs")
        # self.g_tim.expand(profile="shacl") # may take too long for live demo

        # Room temperature visualisation
        self.room_temp = roomtemp.RoomTemp(self.db, self.g_tim)

        # Energy Usage Visualisation
        self.energy_usage = energyusage.EnergyUsage(self.db, self.g)

        # Utilities Usage Visualisation
        self.utilities_usage = utilitiesusage.UtilitiesUsage(self.db, self.g)
        
        # Temperature monitor Chiller and Hot Water System
        self.temperature_monitor = temperaturemonitor.TemperatureMonitor(self.db, self.g)


        # Initialize data quality analysis
        self.sensor_df = None
        self.summary_table = None
        self.perform_data_quality_analysis()

        # Prepare entity data for sunburst chart
        self.df_entities_sorted = self.prepare_entity_data()

    def get_rooms_with_temp(self):
        return self.room_temp.get_rooms_with_temp()
    
    def plot_daily_room_temp(self, room_uri):
        return self.room_temp.plot_daily_room_temp(room_uri)

    def get_electrical_meters(self, meter_type, sensor_type):
        return self.energy_usage.get_electrical_meters(meter_type, sensor_type)

    def load_sensors_from_db(self, df):
        return self.energy_usage.load_sensors_from_db(df)

    def plot_sensor_data_grouped_by_power_complexity(self, df_with_sensor_data, plot_title):
        return self.energy_usage.plot_sensor_data_grouped_by_power_complexity(df_with_sensor_data, plot_title)

    def get_utilities_meters(self, meter_type, sensor_type):
        return self.utilities_usage.get_utilities_meters(meter_type, sensor_type)

    def load_utilities_sensors_from_db(self, df):
        return self.utilities_usage.load_utilities_sensors_from_db(df)

    def plot_sensor_data_grouped_by_meter(self, df_with_sensor_data, plot_title):
        return self.utilities_usage.plot_sensor_data_grouped_by_meter(df_with_sensor_data, plot_title)

    
    def get_temperature_meters(self, meter_type, sensor_type):
        return self.temperature_monitor.get_temperature(meter_type, sensor_type)

    def load_temperature_sensors_from_db(self, df):
        return self.temperature_monitor.load_sensors_from_db(df)

    def plot_temp_sensor_data_grouped_by_meter(self, df_with_sensor_data, plot_title):
        return self.temperature_monitor.plot_sensor_data_grouped_by_meter(df_with_sensor_data, plot_title)
    
    
    def perform_data_quality_analysis(self):
        prepared_data, labels = self.prepare_data_for_preprocessing()
        self.sensor_df = self.preprocess_to_sensor_rows(prepared_data, labels)
        self.sensor_df = self.profile_groups(self.sensor_df)
        gap_analysis_results = self.analyse_sensor_gaps(self.sensor_df)
        self.sensor_df = pd.concat([self.sensor_df, gap_analysis_results], axis=1)
        self.calculate_group_gap_percentages()
        self.create_summary_table()

    def prepare_data_for_preprocessing(self):
        prepared_data = {}
        labels = {}
        for stream_id, data_df in self.db.db.items():
            prepared_data[stream_id] = data_df.set_index('time')
            labels[stream_id] = data_df['label'].iloc[0] if 'label' in data_df.columns else None
        return prepared_data, labels

    def remove_outlier(self, data):
        outlier_info = {}
        for stream_id, df in data.items():
            mean = df['value'].mean()
            std = df['value'].std()
            lower_limit = mean - 3 * std
            upper_limit = mean + 3 * std

            outliers = df[(df['value'] < lower_limit) | (df['value'] > upper_limit)]
            outlier_info[stream_id] = (len(outliers), list(zip(outliers.index, outliers['value'])))

            data[stream_id]['value'] = data[stream_id]['value'].apply(
                lambda x: mean if x < lower_limit or x > upper_limit else x
            )
        return data, outlier_info

    def detect_missing_and_zeros(self, data):
        quality_info = {}
        for stream_id, df in data.items():
            missing_count = df['value'].isna().sum()
            zero_count = (df['value'] == 0).sum()
            quality_info[stream_id] = {
                'Missing': missing_count,
                'Zeros': zero_count
            }
        return quality_info

    def deduce_granularity(self, timestamps):
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

    def preprocess_to_sensor_rows(self, data, labels):
        data, outlier_info = self.remove_outlier(data)
        quality_info = self.detect_missing_and_zeros(data)

        sensor_data = []
        for stream_id, df in data.items():
            timestamps = df.index.to_numpy()
            values = df['value'].to_numpy()
            granularity = self.deduce_granularity(timestamps)
            
            outlier_count, outliers = outlier_info[stream_id]
            missing_count = quality_info[stream_id]['Missing']
            zero_count = quality_info[stream_id]['Zeros']
            label = labels[stream_id]

            start_timestamp = timestamps[0] if len(timestamps) > 0 else None
            end_timestamp = timestamps[-1] if len(timestamps) > 0 else None

            row = {
                'StreamID': stream_id,
                'Label': label,
                'Timestamps': timestamps,
                'Values': values,
                'Deduced_Granularity': granularity,
                'Outliers': [outlier_count, outliers],
                'Missing': missing_count,
                'Zeros': zero_count,
                'Start_Timestamp': start_timestamp,
                'End_Timestamp': end_timestamp,
                'Sensor_Mean': np.mean(values)
            }
            sensor_data.append(row)
        
        return pd.DataFrame(sensor_data)

    def profile_groups(self, df):
        df['Group_Mean'] = np.nan
        df['Group_Std'] = np.nan
        df['FlaggedForRemoval'] = 0

        grouped = df.groupby('Label')

        for label, group in grouped:
            cleaned_values = np.concatenate(group['Values'].to_numpy())
            group_mean = cleaned_values.mean()
            group_std = cleaned_values.std()

            if group_std == 0:
                continue

            lower_limit = group_mean - 3 * group_std
            upper_limit = group_mean + 3 * group_std

            df.loc[group.index, 'Group_Mean'] = group_mean
            df.loc[group.index, 'Group_Std'] = group_std

            df.loc[group.index, 'FlaggedForRemoval'] = df.loc[group.index, 'Sensor_Mean'].apply(
                lambda x: 1 if x < lower_limit or x > upper_limit else 0
            )

        return df

    def analyse_sensor_gaps(self, df):
        def categorise_gaps(data):
            small_gap = medium_gap = large_gap = normal = 0
            total_counts = sum(data.values())
            for key, value in data.items():
                if 1.5 <= float(key) < 3:
                    small_gap += value
                elif 3 <= float(key) < 6:
                    medium_gap += value
                elif float(key) >= 6:
                    large_gap += value
                else:
                    normal += value
            
            total_gaps = small_gap + medium_gap + large_gap
            gap_percentage = (total_gaps / total_counts) * 100 if total_counts > 0 else 0
            gap_percentage_label = f'{round(gap_percentage, 2)} %'
            
            return {
                "Small_Gap_Count": small_gap,
                "Medium_Gap_Count": medium_gap,
                "Large_Gap_Count": large_gap,
                "Normal_Count": normal,
                "Total_Counts": total_counts,
                "Gap_Percentage": gap_percentage,
                "Gap_Percentage_Label": gap_percentage_label
            }

        results = []
        for _, row in df.iterrows():
            granularity_parts = str(row['Deduced_Granularity']).split(" ")
            time_granularity = int(granularity_parts[0])
            time_unit = granularity_parts[1]

            if time_unit == 'minutes':
                time_granularity_seconds = 60
            elif time_unit == 'hours':
                time_granularity_seconds = 3600
            else:
                time_granularity_seconds = 86400

            granularity_in_seconds = time_granularity * time_granularity_seconds

            timestamps = pd.to_datetime(row['Timestamps'])
            time_diffs = np.diff(timestamps).astype('timedelta64[s]').astype(int)

            counter_data = Counter(time_diffs)
            divided_counter = {key / granularity_in_seconds: value for key, value in counter_data.items()}
            sorted_divided_counter = dict(reversed(sorted(divided_counter.items())))

            results.append(categorise_gaps(sorted_divided_counter))

        return pd.DataFrame(results)

    def get_formatted_sensor_df(self):
        """Get the first 15 rows of the sensor_df formatted like the summary table."""
        if self.sensor_df is None:
            print("sensor_df is not initialized yet.")
            return None
        
        # Format numbers in sensor_df just like the summary table
        sensor_df_formatted = self.sensor_df.copy()
        for column in sensor_df_formatted.columns:
            if sensor_df_formatted[column].dtype in ['int64', 'float64']:
                sensor_df_formatted[column] = sensor_df_formatted[column].apply(self.format_number)
        
        # Return the first 15 rows of the formatted dataframe
        return sensor_df_formatted.head(15)


    def calculate_group_gap_percentages(self):
        if 'Gap_Percentage' not in self.sensor_df.columns:
            print("Warning: 'Gap_Percentage' column not found. Skipping group gap percentage calculation.")
            return

        aggregated_results = self.sensor_df.groupby('Label').agg({
            'Gap_Percentage': 'mean'
        }).reset_index()
        aggregated_results_sorted = aggregated_results.sort_values('Gap_Percentage', ascending=False)
        aggregated_results_sorted.columns = ["Label", "Group_Mean_Gap_Percentage"]
        
        self.sensor_df = self.sensor_df.merge(aggregated_results_sorted, on='Label', how='left')
        self.sensor_df['Gap_Percentage_Stream_vs_Group'] = self.sensor_df['Group_Mean_Gap_Percentage'] - self.sensor_df['Gap_Percentage']

    def create_summary_table(self):
        self.summary_table = self.sensor_df.groupby('Label').agg({
            'StreamID': 'count',
            'Group_Mean_Gap_Percentage': 'first',
            'Outliers': lambda x: x.apply(lambda y: y[0]).sum(),
            'Missing': 'sum',
            'Zeros': 'sum',
            'FlaggedForRemoval': 'sum',
            # 'Deduced_Granularity': lambda x: stats.mode(x)[0][0],
            'Deduced_Granularity': lambda x: np.unique(x, return_counts=True)[0][np.argmax(np.unique(x, return_counts=True)[1])],
            'Small_Gap_Count': 'sum',
            'Medium_Gap_Count': 'sum',
            'Large_Gap_Count': 'sum',
            'Values': lambda x: sum(x.apply(len))
        }).reset_index()

        self.summary_table.columns = [
            'Label', 'Stream_Count', 'Group_Mean_Gap_Percentage',
            'Total_Outliers', 'Total_Missing', 'Total_Zeros', 
            'Streams_Flagged_For_Removal', 'Common_Granularity',
            'Total_Small_Gaps', 'Total_Medium_Gaps', 'Total_Large_Gaps',
            'Total_Data_Points'
        ]

        self.summary_table = self.summary_table.sort_values('Group_Mean_Gap_Percentage', ascending=False)

    def get_data_quality_summary(self):
        if self.summary_table is None:
            self.perform_data_quality_analysis()
        return self.summary_table

    def format_number(self, x):
        if pd.isna(x):
            return x
        if isinstance(x, (int, np.integer)):
            return f"{x:,}"
        if x == 0:
            return "0"
        if abs(x) < 0.01:
            return f"{x:.2e}"
        return f"{x:.2f}".rstrip('0').rstrip('.')

    def get_formatted_summary_table(self):
        summary_table = self.get_data_quality_summary()
        for column in summary_table.columns:
            if summary_table[column].dtype in ['int64', 'float64']:
                summary_table[column] = summary_table[column].apply(self.format_number)
        return summary_table
    

    def simplify_identifier(self, identifier):
        if identifier is None:
            return None
        if '#' in identifier:
            return identifier.split('#')[-1]
        elif '/' in identifier:
            return identifier.split('/')[-1]
        return identifier

    def define_categories(self):
        """Define entity categories using ontology subclasses."""
        categories = {
            'Sensor': list(self.get_all_subclasses(BRICK.Sensor)) + [
                BRICK.b1a78aa6_d50d_4c0a_920e_5ff7019ab663_Sensor,
                BRICK.Electrical_Energy_Sensor,
                BRICK.Electrical_Power_Sensor
            ],
            'Point': list(self.get_all_subclasses(BRICK.Point)),
            'Equipment': list(self.get_all_subclasses(BRICK.Equipment)) + [
                BRICK.Electrical_Circuit,
                BRICK.Electrical_Generation_Meter,
                BRICK.PV_Array,
                BRICK.Solar_Inverter
            ],
            'Location': list(self.get_all_subclasses(BRICK.Location)) + [
                BRICK.Enclosed_space,
                BRICK.Open_space,
                BRICK.Shared_space,
                BRICK.space,
                BRICK.space_Kitchen
            ],
            'System': [BRICK.System, BRICK.Chilled_Water_System, BRICK.Hot_Water_System, BRICK.Domestic_Hot_Water_System],
            'Other': [BRICK.Unit]
        }
        return categories

    def get_all_subclasses(self, class_type):
        """Get all subclasses of a Brick class type using the Brick ontology graph."""
        subclasses = set()
        for subclass in self.brick_ontology.transitive_subjects(rdflib.RDFS.subClassOf, class_type):
            subclasses.add(subclass)
        return subclasses

    def get_category(self, entity_type):
        """Categorize entity type based on the defined categories."""
        entity_type_uri = URIRef(entity_type)
        for category, types in self.categories.items():
            if entity_type_uri in types:
                return category
        return 'Other'

    def bfs_traverse_entities(self, graph, starting_entity, predicates, building_id):
        visited = set()
        queue = deque([(starting_entity, None, None, 0, None, 'parent-to-child', building_id)])
        entities = []

        while queue:
            entity, parent_id, parent_type, level, predicate, direction, building_id = queue.popleft()

            if entity in visited:
                continue

            visited.add(entity)
            child_count = sum(1 for _ in graph.subjects(predicate=None, object=entity))
            child_count += sum(1 for _ in graph.objects(subject=entity, predicate=None))

            if parent_id:
                parent_type = graph.value(parent_id, RDF.type)

            entities.append({
                'EntityID': str(entity),
                'EntityType': str(graph.value(entity, RDF.type)),
                'ParentID': str(parent_id) if parent_id else None,
                'ParentType': str(parent_type) if parent_type else None,
                'Predicate': str(predicate) if predicate else None,
                'Direction': direction,
                'BuildingID': building_id,
                'Level': level,
                'ChildCount': child_count
            })

            for pred in predicates:
                for related_entity in graph.objects(entity, pred):
                    if related_entity not in visited:
                        queue.append((related_entity, entity, graph.value(entity, RDF.type), level + 1, pred, 'parent-to-child', building_id))

                for related_entity in graph.subjects(pred, entity):
                    if related_entity not in visited:
                        queue.append((related_entity, entity, graph.value(entity, RDF.type), level + 1, pred, 'child-to-parent', building_id))

        return entities

    def find_all_related_entities_bfs(self, graph, starting_entities, predicates):
        entities_data = []
        for building in starting_entities:
            building_id = str(building)
            related_entities = self.bfs_traverse_entities(graph, building, predicates, building_id)
            entities_data.extend(related_entities)
        
        return entities_data

    def prepare_entity_data(self):
        starting_entities = list(self.g.subjects(RDF.type, BRICK.Building))
        predicates = [BRICK.hasPart, BRICK.isPartOf, BRICK.isLocationOf, BRICK.hasLocation, BRICK.hasPoint, BRICK.isPointOf, BRICK.feeds, BRICK.isFedBy]

        entities_data = self.find_all_related_entities_bfs(self.g, starting_entities, predicates)

        df_entities = pd.DataFrame(entities_data)
        df_entities['Category'] = df_entities['EntityType'].apply(self.get_category)
        
        for col in ['EntityID', 'EntityType', 'ParentID', 'ParentType', 'Predicate', 'BuildingID']:
            df_entities[col] = df_entities[col].apply(self.simplify_identifier)

        return df_entities[['EntityID', 'EntityType', 'Category', 'ParentID', 'ParentType', 'Predicate', 'Direction', 'Level', 'BuildingID', 'ChildCount']]

    def create_hierarchical_sunburst(self):
        df_plot = self.df_entities_sorted.copy()

        df_plot['CleanedEntityID'] = df_plot['EntityID'].apply(self.simplify_identifier)
        df_plot['CleanedParentID'] = df_plot['ParentID'].apply(self.simplify_identifier)

        df_plot['BuildingLabel'] = df_plot.apply(lambda x: f"Building ({x['BuildingID'][-8:]})", axis=1)
        df_plot['ParentLabel'] = df_plot.apply(
            lambda x: f"{x['EntityType']} ({x['CleanedParentID'][-8:]})" if x['CleanedParentID'] is not None else x['BuildingLabel'],
            axis=1
        )
        df_plot['EntityLabel'] = df_plot.apply(lambda x: f"{x['EntityType']} ({x['CleanedEntityID'][-8:]})", axis=1)

        df_plot['hover_text'] = df_plot.apply(lambda row:
            f"Type: {row['EntityType']}<br>Category: {row['Category']}<br>Level: {row['Level']}<br>ID: {row['CleanedEntityID'][-8:]}", axis=1)

        colour_map = {
            'Location': '#FFB6C1', 'Equipment': '#FFD700', 'Sensor': '#87CEFA',
            'Point': '#90EE90', 'System': '#DDA0DD', 'Other': '#FF69B4'
        }

        fig = px.sunburst(
            df_plot,
            ids='EntityID',
            parents='ParentID',
            names='EntityLabel',
            color='Category',
            color_discrete_map=colour_map,
            hover_data=['hover_text'],
            custom_data=['hover_text'],
            title="Building Hierarchy Sunburst Chart",
            height=1500,
            width=1500
        )

        fig.update_traces(textinfo="label", hovertemplate="%{customdata[0]}<extra></extra>")
        fig.update_layout(uniformtext=dict(minsize=8, mode='hide'), margin=dict(t=30, l=0, r=0, b=0), plot_bgcolor='#F0F0F0', paper_bgcolor='#F0F0F0')

        return fig

    def plot_building_hierarchy(self):
        fig = self.create_hierarchical_sunburst()
        fig.show()
        # pio.write_html(fig, file='location_sunburst.html', auto_open=True)