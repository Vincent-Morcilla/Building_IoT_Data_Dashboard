#! /usr/bin/env python3

'''Check for streams that have units.'''

from pathlib import Path
import sys

import rdflib


def main():
    '''Check for streams that have units.'''
    if len(sys.argv) != 2:
        sys.exit('Usage: {sys.argv[0]} <model_ttl>')

    model_file = Path(sys.argv[1])
    if not model_file.exists():
        sys.exit(f'Error: {model_file} does not exist.')

    g = rdflib.Graph()
    g.parse(model_file)


    # # Query for all streams and boolean indicating if they have a unit
    # query = '''
    #     SELECT DISTINCT ?entity ?has_unit WHERE {
    #         ?entity senaps:stream_id ?sid .
    #         BIND( EXISTS { ?entity brick:hasUnit ?unit } as ?has_unit )
    #     }
    # '''
    # results = g.query(query)
    # for result in results:
    #     print(result['entity'], result['has_unit'])

    # Query for all streams that have a unit
    query = '''
        SELECT DISTINCT ?entity WHERE {
            ?entity senaps:stream_id ?sid .
            ?entity brick:hasUnit ?unit .
        }
    '''
    results = g.query(query)
    print(f'Streams with units:    {len(results)}')

    # Query for all streams that don't have a unit
    query = '''
        SELECT DISTINCT ?entity WHERE {
            ?entity senaps:stream_id ?sid .
            FILTER NOT EXISTS {?entity brick:hasUnit ?unit} .
        }
    '''
    results = g.query(query)
    print(f'Streams without units: {len(results)}')


if __name__ == "__main__":
    main()
