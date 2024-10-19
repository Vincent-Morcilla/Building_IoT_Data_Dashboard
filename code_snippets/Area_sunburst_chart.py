from brickschema import Graph
import matplotlib.pyplot as plt
import pandas as pd
import rdflib
from rdflib import Graph, Namespace, URIRef

# Define namespaces
BRICK = Namespace("https://brickschema.org/schema/Brick#")
RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
from brickschema import Graph

g = Graph(load_brick=True)
g.load_file('/Users/dan/Uni/COMP9900/DIEF_BTS/Site_B_updated.ttl')
print(f'Building model has {len(g)} triples')

def load_graph(file_path: str) -> Graph:
    """Load a TTL file into an RDF graph."""
    g = Graph()
    g.parse(file_path, format="turtle")
    return g

def simplify_identifier(identifier: str) -> str:
    """Simplify the identifier by removing unwanted prefixes."""
    if '#' in identifier:
        identifier = identifier.split('#')[-1]
    
    if '/' in identifier:
        identifier = identifier.split('/')[-1]
    
    return identifier

def recursive_traverse_entities(graph: Graph, entity: URIRef, parent_id=None, level=0, visited=None, building_id=None):
    """Recursively traverse the graph to find entities related to the building via hasPart or isPartOf."""
    if visited is None:
        visited = set()

    # If entity is already visited, skip it to prevent cycles
    if entity in visited:
        return []

    # Mark this entity as visited
    visited.add(entity)

    # Collect information about the current entity, along with its parent, level, and building
    entities = [{
        'EntityID': simplify_identifier(str(entity)),
        'EntityType': simplify_identifier(str(graph.value(entity, RDF.type))),
        'ParentID': simplify_identifier(str(parent_id)) if parent_id else None,  # Track parent entity
        'Level': level,  # Track the hierarchy level
        'BuildingID': simplify_identifier(str(building_id))  # Track building ID
    }]

    # Recursively traverse entities connected by hasPart or isPartOf
    for related_entity in graph.objects(entity, BRICK.hasPart):
        entities += recursive_traverse_entities(graph, related_entity, entity, level + 1, visited, building_id)

    for related_entity in graph.subjects(BRICK.isPartOf, entity):
        entities += recursive_traverse_entities(graph, related_entity, entity, level + 1, visited, building_id)

    return entities

def find_all_related_entities(graph: Graph):
    """Find all entities associated with buildings in the graph."""
    entities_data = []
    for building in graph.subjects(RDF.type, BRICK.Building):
        building_id = simplify_identifier(str(building))
        related_entities = recursive_traverse_entities(graph, building, parent_id=None, level=0, building_id=building_id)
        entities_data.extend(related_entities)
    
    return entities_data


# Find all related entities
entities_data = find_all_related_entities(g)

# Create a DataFrame to display the hierarchy
df_entities = pd.DataFrame(entities_data)

# Display the results in a DataFrame
df_entities_sorted = df_entities.sort_values(by=['BuildingID', 'Level', 'EntityType'])

# Show the final DataFrame
df_entities_sorted


import plotly.express as px

# Function to clean the EntityID by removing prefixes and handle None values
def simplify_identifier(identifier: str) -> str:
    """Simplify the identifier by removing unwanted prefixes before the period, if present."""
    if identifier is None:
        return None
    if '.' in identifier:
        # If there's a period, split and keep only the second part
        return identifier.split('.', 1)[-1]
    return identifier  # Otherwise, return the original identifier

# Assuming df_entities contains the relevant hierarchy data
df_sunburst = df_entities.copy()

# Apply the simplify_identifier function to clean both EntityID and ParentID
df_sunburst['CleanedEntityID'] = df_sunburst['EntityID'].apply(simplify_identifier)
df_sunburst['CleanedParentID'] = df_sunburst['ParentID'].apply(simplify_identifier)

# Ensure that all rows have the correct BuildingLabel
df_sunburst['BuildingLabel'] = df_sunburst.apply(lambda x: f"Building ({x['BuildingID'][:8]})", axis=1)

# Fill in None ParentID values with the corresponding BuildingID to avoid missing hierarchy levels
df_sunburst['CleanedParentID'] = df_sunburst['CleanedParentID'].fillna(df_sunburst['BuildingID'])

# Create a dictionary to map CleanedEntityID to its EntityType
parent_label_map = df_sunburst.set_index('CleanedEntityID')['EntityType'].to_dict()

# Adjust ParentLabel to reference the correct parent entity's type
df_sunburst['ParentLabel'] = df_sunburst.apply(
    lambda x: f"{parent_label_map.get(x['CleanedParentID'], 'Unknown')} ({x['CleanedParentID'][:8]})" if x['CleanedParentID'] != x['BuildingID'] else x['BuildingLabel'], axis=1
)

# Combine EntityType with CleanedEntityID for unique labels (use this to handle duplicate labels like multiple "Restroom")
df_sunburst['EntityLabel'] = df_sunburst.apply(lambda x: f"{x['EntityType']} ({x['CleanedEntityID'][:8]})", axis=1)

# Assign a numerical value (e.g., 1) for each entity to be used as the values
df_sunburst['value'] = 1

# Ensure that the labels for all hierarchy levels are filled
df_sunburst['ParentLabel'] = df_sunburst['ParentLabel'].fillna(df_sunburst['BuildingLabel'])

# Create the sunburst chart with meaningful labels at all levels
fig = px.sunburst(df_sunburst, path=['BuildingLabel', 'ParentLabel', 'EntityLabel'], values='value',
                  title="Building Hierarchy (by EntityType with Unique Labels)", height=1200, width=1200)

# Display the sunburst chart
fig.show()
