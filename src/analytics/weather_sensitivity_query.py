electric_energy_query_str = """
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

electric_power_query_str = """
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

gas_query_str = """
SELECT ?meter ?sensor ?stream_id
WHERE {
    ?sensor rdf:type brick:Usage_Sensor .
    ?meter rdf:type brick:Building_Gas_Meter .
    ?sensor brick:isPointOf ?meter .
    ?sensor senaps:stream_id ?stream_id
}
ORDER BY ?meter
"""

outside_air_temperature_query_str = """
SELECT ?sensor ?stream_id 
WHERE {
    ?sensor rdf:type brick:Outside_Air_Temperature_Sensor .
    ?sensor brick:isPointOf   ?loc .
    ?loc a brick:Weather_Station .
    ?sensor senaps:stream_id ?stream_id .
}
ORDER BY ?stream_id
"""