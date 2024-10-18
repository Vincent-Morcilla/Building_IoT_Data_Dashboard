from pathlib import Path

import brickschema
import numpy as np
import pandas as pd
import rdflib
# from rdflib import Namespace
# from rdflib.namespace import RDFS, SKOS, BRICK

class ModelQuality:
    '''TODO'''
    def __init__(self, brick_model, brick_schema=None, mapping=None, time_series=None):
        self.brick_model = Path(brick_model)

        if not self.brick_model.is_file():
            raise FileNotFoundError(f"Brick model file not found: {self.brick_model}")

        if mapping is not None and not Path(mapping).is_file():
            raise FileNotFoundError(f"Mapping file not found: {mapping}")

        if time_series is not None and not Path(time_series).is_file():
            raise FileNotFoundError(f"Time series file not found: {time_series}")

        if brick_schema is None:
            self.schema = brickschema.Graph(load_brick=True)
            self.model = brickschema.Graph(load_brick=True)
            self.expanded_model = brickschema.Graph(load_brick=True)
        elif Path(brick_schema).is_file():
            self.schema = brickschema.Graph().load_file(brick_schema)
            self.model = brickschema.Graph().load_file(brick_schema)
            self.expanded_model = brickschema.Graph().load_file(brick_schema)
        else:
            raise FileNotFoundError(f"Brick schema file not found: {brick_schema}")

        self.model.load_file(self.brick_model)

        self.expanded_model.load_file(self.brick_model)
        self.expanded_model.expand(profile="rdfs")
        self.expanded_model.expand(profile="owlrl", backend="reasonable")
        # self.expanded_model.expand(profile='brick', backend='reasonable')

    def unrecognised_entities(self):
        '''TODO'''
        # q = """
        # SELECT ?s WHERE {
        #     ?s a ?o .
        #     FILTER NOT EXISTS { ?o rdfs:subClassOf* brick:Class }
        # }
        # """
        q = '''
            SELECT DISTINCT ?class WHERE {
                ?class a owl:Class .
                { ?class rdfs:subClassOf* brick:Collection } UNION 
                { ?class rdfs:subClassOf* brick:Equipment } UNION 
                { ?class rdfs:subClassOf* brick:Location } UNION 
                { ?class rdfs:subClassOf* brick:Measurable } UNION 
                { ?class rdfs:subClassOf* brick:Point } . 
            }
        '''
        return self.sparql_to_df(self.schema, q)

    @staticmethod
    def sparql_to_df(g, q):
        '''TODO'''
        res = g.query(q)
        df = pd.DataFrame(res.bindings)
        # are these necessary?
        df = df.map(str)
        df.drop_duplicates(inplace=True)
        return df


if __name__ == "__main__":
    model = "../../datasets/bts_site_b_train/Site_B_tim.ttl"
    schema = "../../datasets/bts_site_b_train/Brick_v1.2.1.ttl"
    mapping = None
    time_series = None
    mq = ModelQuality(model, schema, mapping, time_series)
    print(mq.unrecognised_entities())