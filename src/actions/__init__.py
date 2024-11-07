"""
Actions module for processing interactive data updates.

This module contains reusable functions that handle specific 
interactive actions for updating plots and components based on 
user inputs.

Modules:
    - process_interaction: Processes and applies filters and transformations to data 
                           based on user interactions.
    - update_components_based_on_grouped_table_selection: Updates grouped table components 
                                                          according to selected row data.
    - update_components_based_on_table_selection: Manages updates to components when 
                                                  rows in a table are selected.

Customisation:
    Custom functions can be added to this folder as needed to handle specific 
    interactive actions. Any new functions should be integrated into 
    `callbacks/plot_callbacks.py` to enable their use as part of the app's callback 
    system.
"""
