import warnings
warnings.filterwarnings('ignore')

import rdflib
import brickschema

import visualisations.roomtemp as roomtemp
import visualisations.energyusage as energyusage
import visualisations.utilitiesusage as utilitiesusage


class VisManager:
    def __init__(self, db):
        self.db = db

        # Provided model (for Bassel)
        building_ttl_file = '../datasets/bts_site_b_train/Site_B.ttl'

        # Altered model (for Tim)
        building_tim_ttl_file = '../datasets/bts_site_b_train/Site_B_tim.ttl'
        
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