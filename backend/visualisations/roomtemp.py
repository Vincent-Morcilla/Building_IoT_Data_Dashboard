import matplotlib.pyplot as plt
import pandas as pd

class RoomTemp:
    def __init__(self, db, g):
        self.db = db
        self.g = g

    def get_room_temp_stream_ids(self, room_uri):
        room_temp_query = '''
            SELECT ?ats_sid ?atsp_sid WHERE  {
                ?ats  a                 brick:Air_Temperature_Sensor .
                ?ats  brick:isPointOf   ?room_uri .
                ?ats  senaps:stream_id  ?ats_sid .
                ?atsp a                 brick:Room_Air_Temperature_Setpoint .
                ?atsp brick:isPointOf   ?room_uri .
                ?atsp senaps:stream_id  ?atsp_sid .
            }
        '''

        results = self.g.query(room_temp_query, initBindings={'room_uri': room_uri})
        return results.bindings[0]
    
    def get_outside_temp_stream_id(self):
        outside_temp_query = '''
            SELECT ?ats_sid WHERE  {
                ?ats  a                 brick:Outside_Air_Temperature_Sensor .
                ?ats  brick:isPointOf   ?loc .
                ?loc   a                brick:Weather_Station .
                ?ats  senaps:stream_id  ?ats_sid .
            }
        '''

        results = self.g.query(outside_temp_query)
        return results.bindings[0]
    
    @staticmethod
    def get_daily_median(df):
        df['time'] = pd.to_datetime(df['time'])
        df['date'] = df['time'].dt.date
        df = df.groupby(['date', 'label'])['value'].median().unstack()
        return df
    
    def get_room_class(self, room_uri):
        room_type_query = '''
            SELECT ?class WHERE {
                ?room_uri  a                ?class .
                ?class     rdfs:subClassOf  brick:Room .
            }
        '''

        results = self.g.query(room_type_query, initBindings={'room_uri': room_uri})
        return results.bindings[0]['class'].replace('https://brickschema.org/schema/Brick#', '')
    
    def get_rooms_with_temp(self):
        rooms_with_temp_query = '''
            SELECT DISTINCT ?class ?loc WHERE  {
                ?ats    a                 brick:Air_Temperature_Sensor .
                ?atsp   a                 brick:Room_Air_Temperature_Setpoint .
                ?ats    brick:isPointOf   ?loc .
                ?atsp   brick:isPointOf   ?loc .
                ?loc    a                 brick:Room .
                ?loc    a                 ?class   .
                ?class  rdfs:subClassOf   brick:Room .
            }
            ORDER BY ?class ?loc
        '''

        return self.g.query(rooms_with_temp_query)
    
    def plot_daily_room_temp(self, room_uri):
        outside_temp_stream_id = self.get_outside_temp_stream_id()
        room_temp_stream_ids = self.get_room_temp_stream_ids(room_uri)

        outside_air_temp_df = self.db.get_stream(outside_temp_stream_id['ats_sid'])
        inside_air_temp_df = self.db.get_stream(room_temp_stream_ids['ats_sid'])
        room_air_temp_setpoint_df = self.db.get_stream(room_temp_stream_ids['atsp_sid'])

        outside_median_df = self.get_daily_median(outside_air_temp_df)
        inside_median_df = self.get_daily_median(inside_air_temp_df)
        setpoint_median_df = self.get_daily_median(room_air_temp_setpoint_df)

        # Create the plot
        _, ax = plt.subplots(figsize=(12, 6))
        ax.plot(inside_median_df.index, inside_median_df[inside_median_df.columns[0]], label=inside_median_df.columns[0], marker=',')
        ax.plot(outside_median_df.index, outside_median_df[outside_median_df.columns[0]], label=outside_median_df.columns[0], color='g', marker=',', alpha=0.5)
        ax.plot(setpoint_median_df.index, setpoint_median_df[setpoint_median_df.columns[0]], label=setpoint_median_df.columns[0], color='r', marker=',', alpha=0.5)

        title = self.get_room_class(room_uri).replace('_', ' ') + ' Daily Median Temperature'
        subtitle = f'{room_uri}'
        subtitle = 'URI: ' + subtitle[subtitle.find('#')+1:]

        plt.suptitle(title, fontsize=12, y=0.97)
        plt.title(subtitle, fontsize=8)
        plt.xlabel('Date')
        plt.ylabel('Daily Median Temperature')
        plt.legend()
        plt.grid(True)

        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()