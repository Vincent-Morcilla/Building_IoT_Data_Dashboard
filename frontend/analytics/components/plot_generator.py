from dash import dcc, html, dash_table
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List
from components.download_button import create_global_download_button

def create_plot_component(component):
    """Create a Plotly-based plot component for the Dash app.

    Args:
        component (dict): Configuration for the plot, including:
            - library (str): Either 'px' (Plotly Express) or 'go' (Graph Objects).
            - function (str): The plotting function to use.
            - kwargs (dict): Keyword arguments for the plotting function.
            - layout_kwargs (dict): Layout configuration for the plot.
            - css (dict): Optional CSS styling for the plot.
            - id (str): A unique identifier for the component.
            - data_frame (pd.DataFrame): The data frame for the plot.
            - data_processing (dict): Instructions for processing the data frame.
            - trace_type (str): The type of trace for 'go' plots.

    Returns:
        dcc.Graph: A Dash Graph component containing the plot.
    """
    component_id = component.get('id')
    if component_id is None:
        raise ValueError("Plot component must have an 'id' field.")

    library = component.get('library', 'px')
    func_name = component.get('function')
    kwargs = component.get('kwargs', {}).copy()
    layout_kwargs = component.get('layout_kwargs', {})
    css = component.get('css', {})
    data_processing = component.get('data_processing', {})

    # Retrieve data_frame based on the `library` type
    if library == 'px':
        data_frame = kwargs.get("data_frame")
    elif library == 'go':
        data_frame = component.get("data_frame")
    else:
        raise ValueError(f"Unsupported library '{library}'. Use 'px' or 'go'.")

    # Apply data processing if specified
    if data_processing:
        data_frame = process_data_frame(data_frame, data_processing)

    # Generate the figure based on library
    if library == 'px':
        fig = create_px_figure(func_name, data_frame, kwargs)
    elif library == 'go':
        fig = create_go_figure(data_frame, data_processing, component, kwargs)

    # Update layout and return the figure as a Dash Graph
    fig.update_layout(**layout_kwargs)
    return dcc.Graph(figure=fig, id=component_id, style=css)


def create_px_figure(func_name, data_frame, kwargs):
    """Create a Plotly Express figure.

    Args:
        func_name (str): The name of the Plotly Express function to use.
        data_frame (pd.DataFrame): The data frame to use.
        kwargs (dict): Keyword arguments for the plotting function.

    Returns:
        plotly.graph_objects.Figure: The Plotly figure.
    """
    plot_func = getattr(px, func_name)
    if data_frame is not None:
        kwargs['data_frame'] = data_frame
    return plot_func(**kwargs)


def create_go_figure(data_frame, data_processing, component, kwargs):
    """Create a Plotly Graph Objects figure with flexibility for various trace types.
    
    Args:
        data_frame (pd.DataFrame): The data frame to use.
        data_processing (dict): Instructions for processing the data frame.
        component (dict): The component configuration containing trace_type.
        kwargs (dict): Keyword arguments for the trace.

    Returns:
        plotly.graph_objects.Figure: The Plotly figure with the appropriate trace.
    """
    # Determine the trace type (e.g., Heatmap, Pie, etc.)
    trace_type = component.get("trace_type")
    if not trace_type:
        raise ValueError("Trace type is required in 'component' to create the figure.")
    
    # Get the trace class dynamically from `go`
    trace_class = getattr(go, trace_type, None)
    if trace_class is None:
        raise ValueError(f"Invalid trace type '{trace_type}' specified.")
    
    # Map columns from `df_processed` to trace properties using `data_mappings` if specified
    df_processed = data_frame.copy()
    updated_kwargs = kwargs.copy()

    # If `data_mappings` is present, map columns to their corresponding trace properties
    data_mappings = data_processing.get("data_mappings", {})
    for key, column_name in data_mappings.items():
        # Replace column name strings with actual data from the processed DataFrame
        if column_name in df_processed.columns:
            updated_kwargs[key] = df_processed[column_name]
    
    # For Heatmap or other traces requiring x, y, and z, map those columns directly
    for axis in ['x', 'y', 'z']:
        if axis in updated_kwargs and isinstance(updated_kwargs[axis], str):
            column_name = updated_kwargs[axis]
            if column_name in df_processed.columns:
                updated_kwargs[axis] = df_processed[column_name]
    
    # Apply any additional trace-specific arguments from `trace_kwargs`
    trace_kwargs = data_processing.get("trace_kwargs", {})
    updated_kwargs.update(trace_kwargs)

    # Create the trace using dynamically resolved kwargs
    trace = trace_class(**updated_kwargs)

    # Generate the figure and apply layout customizations
    fig = go.Figure(data=[trace])
    fig.update_layout(**component.get("layout_kwargs", {}))

    return fig


def process_data_frame(data_frame, data_processing):
    """Process the data frame according to the data_processing instructions.

    Args:
        data_frame (pd.DataFrame): The data frame to process.
        data_processing (dict): Instructions for processing the data frame.

    Returns:
        pd.DataFrame: The processed data frame.
    """
    df = data_frame.copy()

    # Apply filtering
    filters = data_processing.get('filter', {})
    for column, value in filters.items():
        if column not in df.columns:
            raise KeyError(f"Column '{column}' not found in DataFrame.")
        if isinstance(value, list):
            df = df[df[column].isin(value)]
        else:
            df = df[df[column] == value]

    if df.empty:
        print("Warning: DataFrame is empty after filtering.")
        return df  # Return the empty DataFrame

    # Apply grouping and aggregation
    groupby_columns = data_processing.get('groupby', [])
    aggregations = data_processing.get('aggregation', {})
    if groupby_columns and aggregations:
        missing_groupby_cols = [col for col in groupby_columns if col not in df.columns]
        if missing_groupby_cols:
            print(f"Warning: Groupby columns {missing_groupby_cols} not found in DataFrame.")
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

    # Apply explode transformation
    transformations = data_processing.get('transformations', [])
    for transformation in transformations:
        if transformation['type'] == 'explode':
            for column in transformation['columns']:
                if column in df.columns and isinstance(df[column].iloc[0], list):
                    df = df.explode(column, ignore_index=True)
                else:
                    raise ValueError(f"Column '{column}' cannot be exploded. Ensure it contains lists.")
    
    return df


def create_traces(data_frame, component):
    """Create Plotly traces based on the processed data frame and component configuration.

    Args:
        data_frame (pd.DataFrame): The processed data frame.
        component (dict): The component configuration.

    Returns:
        list: A list of Plotly graph objects traces.
    """
    if data_frame.empty:
        print("Warning: DataFrame is empty. No traces will be created.")
        return []

    data_processing = component.get('data_processing', {})
    data_mappings = data_processing.get('data_mappings', {})
    trace_type = component.get('trace_type')
    if trace_type is None:
        raise ValueError("For 'go' plots, 'trace_type' must be specified.")

    trace_class = getattr(go, trace_type)
    split_by = data_processing.get('split_by')
    trace_kwargs_base = data_processing.get('trace_kwargs', {})
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
            trace_kwargs['name'] = str(value)
            trace = trace_class(**trace_kwargs)
            traces.append(trace)
    else:
        trace_kwargs = map_data_to_trace(data_frame, data_mappings)
        trace_kwargs.update(trace_kwargs_base)
        trace = trace_class(**trace_kwargs)
        traces.append(trace)

    return traces


def map_data_to_trace(data_frame, data_mappings):
    """Map data frame columns to trace arguments.

    Args:
        data_frame (pd.DataFrame): The data frame to use.
        data_mappings (dict): Mapping of trace arguments to data frame columns or values.

    Returns:
        dict: Keyword arguments for the trace.
    """
    trace_kwargs = {}
    for arg, col in data_mappings.items():
        if col in data_frame.columns:
            trace_kwargs[arg] = data_frame[col].tolist()
        else:
            trace_kwargs[arg] = col  # Use the value directly if not a column
    return trace_kwargs


def create_table_component(component):
    """
    Create a table component using Dash DataTable.

    Args:
        component (dict): A dictionary containing configuration for the table, including:
                          - dataframe: the data to display in the table.
                          - kwargs: keyword arguments for the DataTable.
                          - css: optional CSS styling for the table.
                          - id: a unique identifier for the component.

    Returns:
        dash_table.DataTable or html.Div: A DataTable wrapped in a Div if CSS is provided.
    """
    dataframe = component.get("dataframe")
    kwargs = component.get("kwargs", {})
    css = component.get("css", {})
    component_id = component.get("id")

    if dataframe is None:
        raise ValueError("Table component requires a 'dataframe'.")

    # Prepare columns and data for the table
    columns = kwargs.get("columns", [{"name": col, "id": col} for col in dataframe.columns])
    data = dataframe.to_dict('records')

    # Remove specific kwargs to avoid duplication
    kwargs_filtered = kwargs.copy()
    kwargs_filtered.pop('columns', None)
    kwargs_filtered.pop('data', None)
    kwargs_filtered.pop('id', None)

    # Create the DataTable
    table = dash_table.DataTable(
        data=data,
        columns=columns,
        id=component_id,
        **kwargs_filtered
    )

    # Wrap in a Div if 'css' is provided
    return html.Div(table, style=css) if css else table


def create_ui_component(component):
    """
    Create a generic UI component (input, dropdown, etc.) for the Dash app.

    Args:
        component (dict): A dictionary containing configuration for the UI element, including:
                          - element: the type of UI component (e.g., Input, Dropdown).
                          - kwargs: keyword arguments for the UI component.
                          - css: optional CSS styling for the component.
                          - id: a unique identifier for the component.
                          - label: optional label text for the component.
                          - label_position: where to place the label ('above' or 'next').

    Returns:
        html.Div or dcc component: A Dash component, optionally wrapped in a Div with CSS and label.
    """
    element_type = component.get("element")
    kwargs = component.get("kwargs", {}).copy()
    css = component.get("css", {})
    component_id = component.get("id")
    if component_id:
        kwargs["id"] = component_id
    label_text = component.get("label", "")
    label_position = component.get("label_position", "above")

    # Get the Dash component (from dcc, html, or dash_table)
    dash_component = (
        getattr(dcc, element_type, None)
        or getattr(html, element_type, None)
        or getattr(dash_table, element_type, None)
    )
    if not dash_component:
        raise ValueError(f"Unsupported UI element type '{element_type}'")
    ui_element = dash_component(**kwargs)

    # Add label if provided
    if label_text:
        label_element = html.Label(label_text, htmlFor=component_id)
        if label_position == "next":
            ui_element = html.Div([label_element, ui_element], style={"display": "flex", "alignItems": "center", **css})
        else:
            ui_element = html.Div([label_element, ui_element], style={"display": "flex", "flexDirection": "column", **css})
    else:
        ui_element = html.Div(ui_element, style=css) if css else ui_element

    return ui_element


def create_layout_for_category(category_key: str, plot_config: Dict) -> List:
    """
    Generate the layout for a category based on the provided plot configuration.

    Args:
        category_key (str): The key representing the category.
        plot_config (dict): Configuration dictionary for the category's layout, containing:
            title (str): The section title.
            title_element (str): The HTML element for the title (e.g., 'H2').
            title_style (dict): Styling for the title, such as font size and alignment.
            components (list): List of components (plot, table, UI) to include in the layout.

    Returns:
        list: A list of Dash components representing the category's layout, including:
            - Section title component.
            - Specified UI, plot, table, and separator components.
            - A global download button for downloading data.
    """
    components = []

    # Create the section title
    title_text = plot_config.get("title", "")
    title_element = plot_config.get("title_element", "H2")
    title_style = plot_config.get(
        "title_style",
        {
            "fontSize": "28px",
            "textAlign": "center",
            "marginTop": "20px",
            "marginBottom": "20px",
        },
    )
    title_component = getattr(html, title_element, html.H2)(title_text, style=title_style)
    components.append(title_component)

    # Add components (UI, plots, tables) to the layout
    for component in plot_config.get("components", []):
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
        else:
            raise ValueError(f"Unsupported component type '{comp_type}'")

    components.append(html.Hr())

    global_download_button = create_global_download_button()
    components.append(global_download_button)

    return components


def find_component_by_id(component_id, plot_configs):
    """
    Find and return a component by its ID from the plot configuration.

    Args:
        component_id (str): The ID of the component to find.

    Returns:
        dict: The component's configuration dictionary.

    Raises:
        ValueError: If no component with the given ID is found.
    """
    for config in plot_configs.values():
        for component in config.get("components", []):
            if component.get("id") == component_id:
                return component
    raise ValueError(f"Component with id '{component_id}' not found.")
