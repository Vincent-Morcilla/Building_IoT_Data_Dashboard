import warnings
warnings.filterwarnings('ignore')

import rdflib
import brickschema

import visualisations.roomtemp as roomtemp


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

    def get_rooms_with_temp(self):
        return self.room_temp.get_rooms_with_temp()
    
    def plot_daily_room_temp(self, room_uri):
        return self.room_temp.plot_daily_room_temp(room_uri)
