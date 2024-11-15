"""
Defines custom type annotations and aliases used throughout the application.

This module provides type definitions for the various configurations, data structures,
and components used in the app. It includes type aliases for plot configurations,
category structures, filter conditions, and component configurations, enhancing code
readability and maintainability.
"""

from typing import Any, Dict, List, Tuple, Union

import dash_bootstrap_components as dbc

# Plot Configuration Types

PlotConfig = Dict[
    Tuple[str, str],  # Tuple of category and subcategory as strings
    Dict[
        str,
        Union[
            str,  # For attributes like 'title', 'title_element', etc.
            List[Dict[str, Any]],  # Components (each component is a dictionary)
            List[Dict[str, Any]],  # Interactions (each interaction is a dictionary)
        ],
    ],
]

PlotConfigsKeys = List[  # List of category/subcategory tuples or strings
    Union[Tuple[str, str], str]
]

SpecificCategorySubcategoryPlotConfig = Dict[
    str,
    Union[
        str,  # Attributes like 'title', 'title_element', etc.
        Dict[str, Any],  # Dictionaries for styling, e.g., 'title_style'
        List[Dict[str, Any]],  # Components, where each component is a dictionary
    ],
]

# Sidebar and Layout Types

Categories = Dict[str, List[str]]  # Sidebar categories
CategoryKeyMapping = Dict[str, str]  # Mapping display names to original keys
SubcategoryKeyMapping = Dict[
    Tuple[str, str], str
]  # Mapping category and subcategory keys
NavLinks = List[dbc.NavLink]  #  Navigation links in the sidebar
CategoriesStructure = Tuple[
    Categories, CategoryKeyMapping, SubcategoryKeyMapping
]  # Full sidebar structure

# Filter, Input, and Transformation Types

FilterConditions = Dict[
    str, Union[str, List[str], Dict[str, str]]
]  # Conditions in filter settings
Filters = Dict[str, FilterConditions]  # Full filters dictionary
InputMapping = Dict[str, Any]  # Mapping of input values for filtering
Transformation = Dict[
    str, Any
]  # Configuration for transformations in `apply_transformation`

# Type aliases for components in `analytics.py`

PlotComponentConfig = Dict[str, Any]  # Configuration for plot components
KwargsConfig = Dict[str, Any]  # Configuration for keyword argments
DataProcessingConfig = Dict[
    str, Any
]  # Configuration for data processing in `process_data_frame`
TableComponentConfig = Dict[str, Any]  # Configuration for table components
UIComponentConfig = Dict[
    str, Any
]  # Configuration for UI components like inputs or dropdowns
DataMappings = Dict[
    str, str
]  # Mapping of trace arguments to data frame columns or values.
