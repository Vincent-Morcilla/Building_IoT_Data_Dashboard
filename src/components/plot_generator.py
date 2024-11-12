"""
Generates plot components and layouts based on configuration.

This module is responsible for creating the visual components of the app,
including plots, tables, and UI elements, based on the provided configurations.
It processes data frames, applies data mappings, and generates Plotly figures
and Dash components dynamically, facilitating interactive and data-driven
visualizations.
"""

from typing import Any, Dict, List

from dash import dcc, html, dash_table
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from components.download_button import create_global_download_button
from models.types import (
    DataMappings,
    DataProcessingConfig,
    KwargsConfig,
    PlotConfig,
    PlotComponentConfig,
    SpecificCategorySubcategoryPlotConfig,
    TableComponentConfig,
    UIComponentConfig,
)


def create_plot_component(component: PlotComponentConfig) -> dcc.Graph:
    """Create a Plotly-based plot component for the Dash app.

    Args:
        component (PlotComponentConfig): Configuration for plot components.

    Returns:
        dcc.Graph: A Dash Graph component containing the plot.
    """
    component_id = component.get("id")
    if component_id is None:
        raise ValueError("Plot component must have an 'id' field.")

    library = component.get("library", "px")
    if library is None:
        raise ValueError("Plot component must have a 'library' field.")

    func_name = component.get("function")
    kwargs = component.get("kwargs", {}).copy()
    layout_kwargs = component.get("layout_kwargs", {})
    css = component.get("css", {})
    data_processing = component.get("data_processing", {})

    # Retrieve data_frame based on the `library` type
    if library == "px":
        data_frame = kwargs.get("data_frame")
    elif library == "go":
        data_frame = component.get("data_frame")
    else:
        raise ValueError(f"Unsupported library '{library}'. Use 'px' or 'go'.")

    # Apply data processing if specified
    if data_processing:
        data_frame = process_data_frame(data_frame, data_processing)

    # Generate the figure based on library
    if library == "px":
        fig = create_px_figure(func_name, data_frame, kwargs)
    else:
        fig = create_go_figure(data_frame, data_processing, component, kwargs)

    # Update layout and return the figure as a Dash Graph
    fig.update_layout(**layout_kwargs)
    return dcc.Graph(figure=fig, id=component_id, style=css)


def create_px_figure(
    func_name: str, data_frame: pd.DataFrame, kwargs: KwargsConfig
) -> go.Figure:
    """Create a Plotly Express figure.

    Args:
        func_name (str): The name of the Plotly Express function to use.
        data_frame (pd.DataFrame): The data frame to use.
        kwargs (KwargsConfig): Dictionary of keyword arguments for the plotting function.

    Returns:
        go.Figure: The Plotly figure.
    """
    plot_func = getattr(px, func_name)
    if data_frame is not None:
        kwargs["data_frame"] = data_frame
    return plot_func(**kwargs)


def create_go_figure(
    data_frame: pd.DataFrame,
    data_processing: DataProcessingConfig,
    component: PlotComponentConfig,
    kwargs: KwargsConfig,
) -> go.Figure:
    """Create a Plotly Graph Objects figure with flexibility for various trace types.

    Args:
        data_frame (pd.DataFrame): The data frame to use.
        data_processing (DataProcessingConfig): Instructions for processing the data frame.
        component (PlotComponentConfig): The component configuration containing trace_type.
        kwargs (KwargsConfig): Dictionary of keyword arguments for the trace.

    Returns:
        go.Figure: The Plotly figure with the appropriate trace.
    """
    # Determine the trace type (e.g., Heatmap, Pie, Bar, etc.)
    trace_type = component.get("trace_type")
    if not trace_type:
        raise ValueError("Trace type is required in 'component' to create the figure.")

    # Get the trace class dynamically from `go`
    trace_class = getattr(go, trace_type, None)
    if trace_class is None:
        raise ValueError(f"Invalid trace type '{trace_type}' specified.")

    # Copy the data frame and kwargs
    df_processed = data_frame.copy()
    updated_kwargs = kwargs.copy()

    # Map columns from `df_processed` to trace properties using `data_mappings` if specified
    data_mappings = component.get("data_mappings", {})
    for key, column_name in data_mappings.items():
        # Handle nested keys like 'marker.color'
        if "." in key:
            # Split the nested keys
            keys = key.split(".")
            # Initialize nested dicts as needed
            current_level = updated_kwargs
            for subkey in keys[:-1]:
                if subkey not in current_level:
                    current_level[subkey] = {}
                current_level = current_level[subkey]
            # Set the value at the deepest level
            if column_name in df_processed.columns:
                current_level[keys[-1]] = df_processed[column_name]
        else:
            # Replace column name strings with actual data from the processed DataFrame
            if column_name in df_processed.columns:
                updated_kwargs[key] = df_processed[column_name]
            else:
                # If column not found, leave the value as is or handle as needed
                pass

    # Apply any additional trace-specific arguments from `trace_kwargs`
    trace_kwargs = data_processing.get("trace_kwargs", {})
    updated_kwargs.update(trace_kwargs)

    # Create the trace using dynamically resolved kwargs
    trace = trace_class(**updated_kwargs)

    # Generate the figure and apply layout customizations
    fig = go.Figure(data=[trace])
    fig.update_layout(**component.get("layout_kwargs", {}))

    return fig


def process_data_frame(
    data_frame: pd.DataFrame, data_processing: Dict[str, Any]
) -> pd.DataFrame:
    """Process the data frame by applying filters, grouping/aggregation, and transformations.

    Args:
        data_frame (pd.DataFrame): The data frame to process.
        data_processing (Dict): Instructions for processing, including filters, groupby,
        and transformations.

    Returns:
        pd.DataFrame: The processed data frame.
    """
    df = data_frame.copy()

    # Step 1: Apply filtering
    filters = data_processing.get("filter", {})
    for column, value in filters.items():
        if column not in df.columns:
            raise KeyError(f"Column '{column}' not found in DataFrame.")
        if isinstance(value, list):
            df = df[df[column].isin(value)]
        else:
            df = df[df[column] == value]

    if df.empty:
        print("Warning: DataFrame is empty after filtering.")
        return df  # Return the empty DataFrame if no data remains after filtering

    # Step 2: Apply grouping and aggregation
    groupby_columns = data_processing.get("groupby", [])
    aggregations = data_processing.get("aggregation", {})
    if groupby_columns and aggregations:
        missing_groupby_cols = [col for col in groupby_columns if col not in df.columns]
        if missing_groupby_cols:
            print(
                f"Warning: Groupby columns {missing_groupby_cols} not found in DataFrame."
            )
            return df
        agg_dict = {}
        for new_col, (col, func) in aggregations.items():
            if col not in df.columns:
                print(f"Warning: Aggregation column '{col}' not found in DataFrame.")
                continue
            agg_dict[new_col] = (col, func)
        if not agg_dict:
            print("Warning: No valid aggregations to perform.")
            return df
        df = df.groupby(groupby_columns).agg(**agg_dict).reset_index()

    # Step 3: Apply transformations
    transformations = data_processing.get("transformations", [])
    for transformation in transformations:
        if transformation["type"] == "explode":
            for column in transformation["columns"]:
                if column in df.columns and isinstance(df[column].iloc[0], list):
                    df = df.explode(column, ignore_index=True)
                else:
                    raise ValueError(
                        f"Column '{column}' cannot be exploded. Ensure it contains lists."
                    )
    return df


def create_traces(
    data_frame: pd.DataFrame, component: PlotComponentConfig
) -> List[go.Trace]:
    """Create Plotly traces based on the processed data frame and component configuration.

    Args:
        data_frame (pd.DataFrame): The processed data frame.
        component (PlotComponentConfig): The component configuration.

    Returns:
        List[go.Trace]: A list of Plotly graph objects traces.
    """
    if data_frame.empty:
        print("Warning: DataFrame is empty. No traces will be created.")
        return []

    data_processing = component.get("data_processing", {})
    data_mappings = data_processing.get("data_mappings", {})
    trace_type = component.get("trace_type")
    if trace_type is None:
        raise ValueError("For 'go' plots, 'trace_type' must be specified.")

    trace_class = getattr(go, trace_type)
    split_by = data_processing.get("split_by")
    trace_kwargs_base = data_processing.get("trace_kwargs", {})
    traces = []

    # Validate data mappings
    required_columns = set(data_mappings.values())
    missing_columns = required_columns - set(data_frame.columns)
    if missing_columns:
        print(f"Warning: Missing columns in data_frame: {missing_columns}")
        return []

    if split_by:
        if split_by not in data_frame.columns:
            print(f"Warning: 'split_by' column '{split_by}' not found in DataFrame.")
            return []
        unique_values = data_frame[split_by].unique()
        for value in unique_values:
            df_subset = data_frame[data_frame[split_by] == value]
            trace_kwargs = map_data_to_trace(df_subset, data_mappings)
            trace_kwargs.update(trace_kwargs_base)
            trace_kwargs["name"] = str(value)
            trace = trace_class(**trace_kwargs)
            traces.append(trace)
    else:
        trace_kwargs = map_data_to_trace(data_frame, data_mappings)
        trace_kwargs.update(trace_kwargs_base)
        trace = trace_class(**trace_kwargs)
        traces.append(trace)

    return traces


def map_data_to_trace(
    data_frame: pd.DataFrame, data_mappings: DataMappings
) -> Dict[str, Any]:
    """Map data frame columns to trace arguments.

    Args:
        data_frame (pd.DataFrame): The data frame to use.
        data_mappings (DataMappings): Mapping of trace arguments to data frame columns or values.

    Returns:
        Dict[str, Any]: Keyword arguments for the trace.

    Raises:
        ValueError: If `data_mappings` is not a dictionary.
    """
    if not isinstance(data_mappings, dict):
        raise ValueError("`data_mappings` must be a dictionary.")

    trace_kwargs = {}
    for arg, col in data_mappings.items():
        if col in data_frame.columns:
            trace_kwargs[arg] = data_frame[col].tolist()
        else:
            trace_kwargs[arg] = col  # Use the value directly if not a column
    return trace_kwargs


def create_table_component(component: TableComponentConfig) -> html.Div:
    """Create a table component using Dash DataTable with an optional title.

    Args:
        component (TableComponentConfig): Configuration for the table.

    Returns:
        html.Div: A Div containing the title (if any) and the DataTable.
    """
    dataframe = component.get("dataframe")
    kwargs = component.get("kwargs", {})
    css = component.get("css", {})
    component_id = component.get("id")

    # Get title-related configurations
    title = component.get("title", "")
    title_element = component.get("title_element", "H5")
    title_kwargs = component.get("title_kwargs", {})

    if dataframe is None:
        raise ValueError("Table component requires a 'dataframe'.")

    # Prepare columns and data for the table
    columns = kwargs.get(
        "columns", [{"name": col, "id": col} for col in dataframe.columns]
    )
    data = dataframe.to_dict("records")

    # Remove specific kwargs to avoid duplication
    kwargs_filtered = kwargs.copy()
    kwargs_filtered.pop("columns", None)
    kwargs_filtered.pop("data", None)
    kwargs_filtered.pop("id", None)

    # Create the DataTable
    table = dash_table.DataTable(
        data=data, columns=columns, id=component_id, **kwargs_filtered
    )

    # Create the title component if title is specified
    if title:
        # Get the HTML title element dynamically
        title_component = getattr(html, title_element, html.H5)(title, **title_kwargs)
        children = [title_component, table]
    else:
        children = [table]

    # Wrap in a Div, including CSS if provided
    return html.Div(children, style=css)


def create_ui_component(component: UIComponentConfig) -> html.Div:
    """Create a generic UI component (input, dropdown, etc.) for the Dash app.

    Args:
        component (UIComponentConfig): Configuration for the UI element.

    Returns:
        html.Div: A Dash component, optionally wrapped in a Div with CSS and label.
    """
    # Extract required attributes from the component configuration
    element_type = component.get("element")
    kwargs = component.get("kwargs", {}).copy()
    css = component.get("css", {})
    component_id = component.get("id")

    # Assign component ID to kwargs if available
    if component_id:
        kwargs["id"] = component_id

    # Get label and positioning settings
    label_text = component.get("label", "")
    label_position = component.get("label_position", "above")

    # Get title-related configurations
    title = component.get("title", "")
    title_element = component.get("title_element", "H5")
    title_kwargs = component.get("title_kwargs", {})

    # Locate the Dash component (from dcc, html, or dash_table)
    dash_component = (
        getattr(dcc, element_type, None)
        or getattr(html, element_type, None)
        or getattr(dash_table, element_type, None)
    )
    if not dash_component:
        raise ValueError(f"Unsupported UI element type '{element_type}'")

    # Initialize the UI element
    ui_element = dash_component(**kwargs)

    # Create a label if specified
    if label_text:
        label_element = html.Label(
            label_text, htmlFor=component_id if component_id else None
        )
        if label_position == "next":
            ui_element = html.Div(
                [label_element, ui_element],
                style={"display": "flex", "alignItems": "center", **css},
            )
        else:
            ui_element = html.Div(
                [label_element, ui_element],
                style={"display": "flex", "flexDirection": "column", **css},
            )
    else:
        ui_element = html.Div(ui_element, style=css) if css else ui_element

    # Create the title component if title is specified
    if title:
        # Validate title element
        title_component = getattr(html, title_element, html.H5)(title, **title_kwargs)
        children = [title_component, ui_element]
    else:
        children = [ui_element]

    # Wrap in a Div, including CSS if provided
    return html.Div(children, style=css)


def create_layout_for_category(
    selected_plot_config: SpecificCategorySubcategoryPlotConfig,
) -> List[html.Div]:
    """
    Generate the layout for a category based on the provided specific category-subcategory
    plot configuration.

    Args:
        selected_plot_config (SpecificCategorySubcategoryPlotConfig): Configuration
        dictionary for the layout of a specific category and subcategory, containing:
            title (str): The section title.
            title_element (str): The HTML element for the title (e.g., 'H2').
            title_style (dict): Styling for the title, such as font size and alignment.
            components (list): List of components (plot, table, UI) to include in the layout.

    Returns:
        List[html.Div]: A list of Dash components representing the layout for the specific
        category-subcategory pair, including:
            - Section title component.
            - Specified UI, plot, table, separator, and placeholder components.
            - A global download button for downloading data.
    """
    components = []

    # Create the section title
    title_text = selected_plot_config.get("title", "")
    title_element = selected_plot_config.get("title_element", "H2")
    title_style = selected_plot_config.get(
        "title_style",
        {
            "fontSize": "28px",
            "textAlign": "center",
            "marginTop": "20px",
            "marginBottom": "20px",
        },
    )
    title_component = getattr(html, title_element, html.H2)(
        title_text, style=title_style
    )
    components.append(title_component)

    # Add components (UI, plots, tables) to the layout
    for component in selected_plot_config.get("components", []):
        comp_type = component.get("type")
        if comp_type == "UI":
            components.append(create_ui_component(component))
        elif comp_type == "plot":
            components.append(create_plot_component(component))
        elif comp_type == "table":
            components.append(create_table_component(component))
        elif comp_type == "separator":
            separator_style = component.get("style", {})
            components.append(html.Hr(style=separator_style))
        elif comp_type == "placeholder":
            # Create an empty Div with the specified ID
            placeholder_id = component.get("id")
            placeholder_css = component.get("css", {})
            components.append(html.Div(id=placeholder_id, style=placeholder_css))
        elif comp_type == "error":
            error_message = component.get("message", "An error occurred.")
            error_css = component.get("css", {})
            error_component = html.Div(f"Error: {error_message}", style=error_css)
            components.append(error_component)
        else:
            raise ValueError(f"Unsupported component type '{comp_type}'")

    components.append(html.Hr())

    global_download_button = create_global_download_button()
    components.append(global_download_button)

    return components


def find_component_by_id(
    component_id: str, plot_configs: PlotConfig
) -> PlotComponentConfig:
    """
    Find and return a component by its ID from the plot configuration.

    Args:
        component_id (str): The ID of the component to find.
        plot_configs (PlotConfig): A dictionary of plot configurations.

    Returns:
        PlotComponentConfig: The component's configuration dictionary.

    Raises:
        ValueError: If no component with the given ID is found.
    """
    for config in plot_configs.values():
        for component in config.get("components", []):
            if component.get("id") == component_id:
                return component
    raise ValueError(f"Component with id '{component_id}' not found.")
