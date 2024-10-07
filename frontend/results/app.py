import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, dcc, html
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np

# Initialize the app with external stylesheets
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True
)

# Define main categories and subcategories
main_categories = {
    "Home": [],
    "Electricity": ["General Analysis", "Usage", "Breakout Detection", "Data Quality"],
    "Temperature": ["General Analysis", "Weather Sensitivity", "Breakout Detection", "Data Quality"],
    "Area": ["General Analysis", "Breakout Detection", "Data Quality"]
}

# Map pathnames to category names
path_to_category = {
    'electricity': 'Electricity',
    'temperature': 'Temperature',
    'area': 'Area',
}

# Read the data from CSV into a DataFrame
data = pd.read_csv('building_iot_data.csv', parse_dates=['Timestamp'])

# Sidebar layout with navigation links
sidebar = html.Div(
    [
        dcc.Link(
            html.Div(html.Img(src="/assets/logo.svg", className="sidebar-logo"),
                     className="sidebar-logo-container"),
            href="/",
            className="logo-link"
        ),
        html.Hr(),
        dbc.Nav(
            [
                dbc.NavLink(
                    category,
                    href="/" if category == "Home" else f"/{category.lower().replace(' ', '-')}",
                    active="exact"
                )
                for category in main_categories.keys()
            ],
            vertical=True,
            pills=True,
        ),
    ],
    className="sidebar"
)

content = html.Div(id="page-content", className="content")

# Layout includes sidebar and content area and used a dcc.Store component to hold screen size data
app.layout = html.Div([
    dcc.Location(id="url"),
    sidebar,
    content,
    dcc.Store(id='screen-size', storage_type='session')
])

# Clientside callback to app
app.clientside_callback(
    """
    function(pathname) {
        return window.innerWidth;
    }
    """,
    Output('screen-size', 'data'),
    Input('url', 'pathname')
)

# Function to filter data based on date range and frequency
def filter_data(df, start_date, end_date, variables, frequency=None):
    df_filtered = df[(df['Timestamp'] >= start_date) & (df['Timestamp'] <= end_date)]
    if frequency:
        df_filtered = df_filtered.set_index('Timestamp').resample(frequency).mean().reset_index()
    return df_filtered[['Timestamp'] + variables]

# Function to create line plot
def create_line_plot(data, x_column, y_columns, title):
    fig = px.line(data, x=x_column, y=y_columns)
    fig.update_layout(
        title={'text': title, 'x': 0.5, 'xanchor': 'center'},
        font_color='black',
        plot_bgcolor='white',
        legend=dict(
            orientation='h',
            yanchor='top',
            y=-0.3,
            xanchor='center',
            x=0.5
        )
    )
    fig.update_xaxes(
        mirror=True, ticks='outside', showline=True,
        linecolor='black', gridcolor='lightgrey'
    )
    fig.update_yaxes(
        mirror=True, ticks='outside', showline=True,
        linecolor='black', gridcolor='lightgrey'
    )
    return fig

# Function to create box plot
def create_box_plot(data, y_columns, title):
    fig = px.box(data, y=y_columns)
    fig.update_layout(
        title={'text': title, 'x': 0.5, 'xanchor': 'center'},
        font_color='black',
        plot_bgcolor='white',
        margin=dict(l=50, r=50, b=100, t=50), 
        autosize=True,
        font=dict(size=10),
        legend=dict(font=dict(size=10))
    )
    fig.update_xaxes(
        mirror=True, ticks='outside', showline=True,
        linecolor='black', gridcolor='lightgrey'
    )
    fig.update_yaxes(
        mirror=True, ticks='outside', showline=True,
        linecolor='black', gridcolor='lightgrey'
    )
    fig.update_traces(marker_color='#3c9639')
    return fig

# Function to create heatmap
def create_heatmap(data, x_column, y_column, z_column, color_scale, title):
    fig = px.density_heatmap(
        data, x=x_column, y=y_column, z=z_column,
        color_continuous_scale=color_scale
    )
    fig.update_layout(
        title={'text': title, 'x': 0.5, 'xanchor': 'center'},
        font_color='black',
        plot_bgcolor='white',
        coloraxis_colorbar=dict(
            orientation='h',
            yanchor='top',
            y=-0.4,
            xanchor='center',
            x=0.5,
            title_side='bottom'
        )
    )
    fig.update_xaxes(
        mirror=True, ticks='outside', showline=True,
        linecolor='black', gridcolor='lightgrey'
    )
    fig.update_yaxes(
        mirror=True, ticks='outside', showline=True,
        linecolor='black', gridcolor='lightgrey'
    )
    return fig

# Function to create surface plot
def create_surface_plot(X, Y, Z, color_scale, title):
    fig = go.Figure(data=[go.Surface(
        x=X, y=Y, z=Z, colorscale=color_scale,
        colorbar=dict(
            orientation='h',
            yanchor='top',
            y=-0.3,
            xanchor='center',
            x=0.5,
            title_side='bottom'
        )
    )])
    
    fig.update_layout(
        title={'text': title, 'x': 0.5, 'xanchor': 'center'},
        autosize=True,
        margin=dict(l=65, r=50, b=65, t=90),
        font_color='black',
        plot_bgcolor='white',
        scene=dict(
            xaxis=dict(
                mirror=True, ticks='outside', showline=True,
                linecolor='black', gridcolor='lightgrey'
            ),
            yaxis=dict(
                mirror=True, ticks='outside', showline=True,
                linecolor='black', gridcolor='lightgrey'
            ),
            zaxis=dict(
                mirror=True, ticks='outside', showline=True,
                linecolor='black', gridcolor='lightgrey'
            ),
        )
    )
    return fig

# Helper function to get category from pathname
def get_category_from_pathname(pathname):
    pathname = pathname.lstrip("/")
    if pathname == "" or pathname == "home":
        return "Home"
    elif pathname in path_to_category:
        return path_to_category[pathname]
    else:
        return None

# Helper function to get variables for a category
def get_variables_for_category(category):
    if category == "Electricity":
        return {
            'general_vars': ['Electricity_General_Equipment_1', 'Electricity_General_Equipment_2', 'Electricity_General_Equipment_3'],
            'usage_vars': ['Electricity_Usage_Equipment_1', 'Electricity_Usage_Equipment_2', 'Electricity_Usage_Equipment_3'],
            'breakout_vars': ['Electricity_Breakout_Equipment_1', 'Electricity_Breakout_Equipment_2', 'Electricity_Breakout_Equipment_3'],
            'quality_vars': ['Electricity_Quality_Equipment_1', 'Electricity_Quality_Equipment_2', 'Electricity_Quality_Equipment_3'],
            'sensitivity_vars': []
        }
    elif category == "Temperature":
        return {
            'general_vars': ['Average_Temperature_Inside', 'Average_Temperature_Outside'],
            'sensitivity_vars': ['Area_Inside_Room_Sensitivity', 'Area_Outside_Building_Sensitivity'],
            'breakout_vars': ['Temperature_Breakout_Inside', 'Temperature_Breakout_Outside'],
            'quality_vars': ['Temperature_Quality_Inside', 'Temperature_Quality_Outside'],
            'usage_vars': []
        }
    elif category == "Area":
        return {
            'general_vars': ['Area_Inside_Room_Temperature', 'Area_Outside_Building_Temperature'],
            'sensitivity_vars': [],
            'breakout_vars': ['Area_Inside_Room_Sensitivity', 'Area_Outside_Building_Sensitivity'],
            'quality_vars': ['Area_Quality_Inside', 'Area_Quality_Outside'],
            'usage_vars': []
        }
    else:
        return {
            'general_vars': [],
            'usage_vars': [],
            'breakout_vars': [],
            'quality_vars': [],
            'sensitivity_vars': []
        }

# Function to create the download buttons
def get_download_buttons():
    download_buttons = dbc.Row(
        [
            dbc.Col(
                dbc.Button("Download Report", id="download-left", outline=True, color="success"),
                width="auto"
            ),
            dbc.Col(
                dbc.Button("Download Data", id="download-right", outline=True, color="success"),
                width="auto"
            )
        ],
        justify="end",
        className="ml-auto"
    )
    return download_buttons

# Callback to render content based on URL
@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def render_page_content(pathname):
    category = get_category_from_pathname(pathname)
    if category == "Home":
        return html.P("Welcome to the Home page!")
    elif category in main_categories:
        tabs = main_categories[category]
        return html.Div([
            dbc.Tabs(
                [
                    dbc.Tab(label=subcat, tab_id=subcat.lower().replace(" ", "-"))
                    for subcat in tabs
                ],
                id="tabs",
                active_tab=tabs[0].lower().replace(" ", "-"),
                className='tab-class'
            ),
            html.Div(id="tab-content"),
            html.Br(),
            get_download_buttons()
        ])
    else:
        # 404 page
        return html.Div(
            [
                html.H1("404: Not found", className="text-danger"),
                html.Hr(),
                html.P(f"The pathname /{pathname} was not recognized..."),
            ],
            className="p-3 bg-light rounded-3",
        )

# Callback to render tab content based on active tab
@app.callback(
    Output("tab-content", "children"),
    [Input("tabs", "active_tab"),
     Input("url", "pathname")]
)
def render_tab_content(active_tab, pathname):
    if active_tab is None:
        return dash.no_update

    category = get_category_from_pathname(pathname)
    if category not in main_categories:
        return html.P("Category not found.")

    variables = get_variables_for_category(category)

    if active_tab == "general-analysis":
        # Controls
        controls = html.Div([
            html.Label('Select Variables'),
            dcc.Dropdown(
                id='general-analysis-variable-dropdown',
                options=[{'label': var, 'value': var} for var in variables['general_vars']],
                value=variables['general_vars'],
                multi=True
            ),
            html.Br(),
            html.Label('Select Date Range'),
            dcc.DatePickerRange(
                id='general-analysis-date-picker',
                min_date_allowed=data['Timestamp'].min().date(),
                max_date_allowed=data['Timestamp'].max().date(),
                start_date=data['Timestamp'].min().date(),
                end_date=data['Timestamp'].max().date()
            ),
            html.Br(),
            html.Label('Select Frequency'),
            dcc.RadioItems(
                id='general-analysis-frequency-radio',
                options=[
                    {'label': 'Hourly', 'value': 'h'},
                    {'label': 'Daily', 'value': 'D'},
                    {'label': 'Monthly', 'value': 'ME'},
                ],
                value='D',
                labelStyle={'display': 'inline-block', 'margin-right': '10px'}
            ),
        ])
        # Graph
        graph = dcc.Graph(id='general-analysis-graph')
        return html.Div([
            dcc.Loading(graph, type='circle', color='#3c9639'),
            html.Br(),
            controls,
            html.Br(),
        ])
    elif active_tab == "breakout-detection":
        # Controls
        breakout_vars = variables['breakout_vars']
        if not breakout_vars:
            return html.P("No breakout variables available for this category.")
        controls = html.Div([
            html.Label('Select Equipment'),
            dcc.Dropdown(
                id='breakout-variable-dropdown',
                options=[{'label': var, 'value': var} for var in breakout_vars],
                value=breakout_vars[0],
                multi=False
            ),
            html.Label('Select Color Scale'),
            dcc.Dropdown(
                id='breakout-color-scale-dropdown',
                options=[{'label': scale, 'value': scale} for scale in px.colors.named_colorscales()],
                value='Viridis',
                clearable=False
            ),
        ])
        # Graph
        graph = dcc.Graph(id='breakout-detection-graph')
        return html.Div([
            dcc.Loading(graph, type='circle', color='#3c9639'),
            html.Br(),
            controls,
            html.Br(),
        ])
    elif active_tab == "data-quality":
        # Controls
        controls = html.Div([
            html.Label('Select Variables'),
            dcc.Dropdown(
                id='data-quality-variable-dropdown',
                options=[{'label': var, 'value': var} for var in variables['general_vars']],
                value=variables['general_vars'],
                multi=True
            ),
        ])
        # Graph
        graph = dcc.Graph(id='data-quality-graph')
        return html.Div([
            dcc.Loading(graph, type='circle', color='#3c9639'),
            html.Br(),
            controls,
            html.Br(),
        ])
    elif active_tab == "weather-sensitivity" and category == "Temperature":
        # Controls
        controls = html.Div([
            html.Label('Select Color Scale'),
            dcc.Dropdown(
                id='weather-sensitivity-color-dropdown',
                options=[{'label': scale, 'value': scale} for scale in px.colors.named_colorscales()],
                value='Viridis',
                clearable=False
            ),
        ])
        # Graph
        graph = dcc.Graph(id='weather-sensitivity-graph')
        return html.Div([
            dcc.Loading(graph, type='circle', color='#3c9639'),
            html.Br(),
            controls,
            html.Br(),
        ])
    elif active_tab == "usage":
        # Controls
        controls = html.Div([
            html.Label('Select Variables'),
            dcc.Dropdown(
                id='usage-variable-dropdown',
                options=[{'label': var, 'value': var} for var in variables['usage_vars']],
                value=variables['usage_vars'],
                multi=True
            ),
            html.Br(),
            html.Label('Select Date Range'),
            dcc.DatePickerRange(
                id='usage-date-picker',
                min_date_allowed=data['Timestamp'].min().date(),
                max_date_allowed=data['Timestamp'].max().date(),
                start_date=data['Timestamp'].min().date(),
                end_date=data['Timestamp'].max().date()
            ),
            html.Br(),
            html.Label('Select Frequency'),
            dcc.RadioItems(
                id='usage-frequency-radio',
                options=[
                    {'label': 'Hourly', 'value': 'h'},
                    {'label': 'Daily', 'value': 'D'},
                    {'label': 'Monthly', 'value': 'ME'},
                ],
                value='D',
                labelStyle={'display': 'inline-block', 'margin-right': '10px'}
            ),
        ])
        # Graph
        graph = dcc.Graph(id='usage-graph')
        return html.Div([
            dcc.Loading(graph, type='circle', color='#3c9639'),
            html.Br(),
            controls,
            html.Br(),
        ])
    else:
        return html.P("Tab content not found.")

# Helper function to create an empty figure with custom styling
def create_empty_figure(message_text, category, analysis_type):
    fig = go.Figure()
    fig.add_annotation(
        text=message_text,
        xref="paper", yref="paper",
        x=0.5, y=0.5, showarrow=False,
        font=dict(size=16)
    )
    fig.update_layout(
        title={'text': f'{category} - {analysis_type}', 'x': 0.5, 'xanchor': 'center'},
        font_color='black',
        plot_bgcolor='white'
    )
    fig.update_xaxes(
        mirror=True, ticks='outside', showline=True,
        linecolor='black', gridcolor='lightgrey'
    )
    fig.update_yaxes(
        mirror=True, ticks='outside', showline=True,
        linecolor='black', gridcolor='lightgrey'
    )
    return fig

# General Analysis Callback
@app.callback(
    Output('general-analysis-graph', 'figure'),
    [Input('general-analysis-date-picker', 'start_date'),
     Input('general-analysis-date-picker', 'end_date'),
     Input('general-analysis-variable-dropdown', 'value'),
     Input('general-analysis-frequency-radio', 'value'),
     Input("url", "pathname")]
)
def update_general_analysis(start_date, end_date, selected_vars, freq, pathname):
    category = get_category_from_pathname(pathname)
    if not selected_vars or not category:
        return create_empty_figure("No variables selected.", category or "Unknown", "General Analysis")

    df_filtered = filter_data(data, start_date, end_date, selected_vars, frequency=freq)

    if df_filtered.empty or df_filtered[selected_vars].isnull().all().all():
        return create_empty_figure(
            "No data available at the selected frequency for the chosen date range.",
            category, "General Analysis"
        )

    fig = create_line_plot(
        df_filtered,
        x_column='Timestamp',
        y_columns=selected_vars,
        title=f'{category} - General Analysis'
    )
    return fig

# Breakout Detection Callback
@app.callback(
    Output('breakout-detection-graph', 'figure'),
    [Input('breakout-variable-dropdown', 'value'),
     Input('breakout-color-scale-dropdown', 'value'),
     Input("url", "pathname")]
)
def update_breakout_detection(selected_breakout_var, color_scale, pathname):
    category = get_category_from_pathname(pathname)
    variables = get_variables_for_category(category)

    if selected_breakout_var not in variables['breakout_vars']:
        return create_empty_figure("Selected equipment not available.", category, "Breakout Detection")

    df_filtered = data[['Timestamp', selected_breakout_var]].dropna()

    if df_filtered.empty:
        return create_empty_figure("No breakout events detected.", category, "Breakout Detection")

    fig = create_heatmap(
        data=df_filtered,
        x_column='Timestamp',
        y_column='Timestamp',
        z_column=selected_breakout_var,
        color_scale=color_scale,
        title=f'{category} - Breakout Detection'
    )
    return fig

# Data Quality Callback
@app.callback(
    Output('data-quality-graph', 'figure'),
    [Input('data-quality-variable-dropdown', 'value'),
     Input("url", "pathname"),
     Input('screen-size', 'data')]
)
def update_data_quality(selected_vars, pathname, screen_width):
    category = get_category_from_pathname(pathname)
    if not selected_vars:
        return create_empty_figure("No variables selected.", category, "Data Quality")
    
    variables = get_variables_for_category(category)
    quality_vars = variables['quality_vars']
    
    df_filtered = data[['Timestamp'] + quality_vars + selected_vars].dropna()
    if df_filtered.empty or df_filtered[selected_vars].isnull().all().all():
        return create_empty_figure("No data available for the selected variables.", category, "Data Quality")
    
    fig = create_box_plot(df_filtered[selected_vars], y_columns=selected_vars, title=f'{category} - Data Quality')
    
    # Conditionally rotate x-axis labels based on screen size
    if screen_width and int(screen_width) < 768:  # Adjust threshold for small screens
        fig.update_layout(xaxis_tickangle=-45)
    
    return fig

# Weather Sensitivity Callback
@app.callback(
    Output('weather-sensitivity-graph', 'figure'),
    [Input('weather-sensitivity-color-dropdown', 'value'),
     Input("url", "pathname")]
)
def update_weather_sensitivity(color_scale, pathname):
    category = get_category_from_pathname(pathname)
    if category != "Temperature":
        return create_empty_figure("Weather Sensitivity is only available under Temperature.", category, "Weather Sensitivity")

    if 'Area_Inside_Room_Sensitivity' not in data.columns or 'Area_Outside_Building_Sensitivity' not in data.columns:
        return create_empty_figure("Weather sensitivity data not available.", category, "Weather Sensitivity")

    df_filtered = data[['Timestamp', 'Area_Inside_Room_Sensitivity', 'Area_Outside_Building_Sensitivity']].dropna()

    if df_filtered.empty:
        return create_empty_figure("No data available for weather sensitivity.", category, "Weather Sensitivity")

    # Convert 'Timestamp' to numeric index for plotting
    X_numeric = np.arange(len(df_filtered['Timestamp']))
    Y = df_filtered['Area_Inside_Room_Sensitivity']
    Z = df_filtered['Area_Outside_Building_Sensitivity']

    # Create a meshgrid for X and Y
    X_mesh, Y_mesh = np.meshgrid(X_numeric, Y)

    # Broadcast Z to match the shape of X_mesh and Y_mesh
    Z_mesh = np.tile(Z, (len(Y), 1))

    fig = create_surface_plot(
        X=X_mesh, Y=Y_mesh, Z=Z_mesh,
        color_scale=color_scale,
        title=f'{category} - Weather Sensitivity'
    )
    return fig

# Usage Callback
@app.callback(
    Output('usage-graph', 'figure'),
    [Input('usage-date-picker', 'start_date'),
     Input('usage-date-picker', 'end_date'),
     Input('usage-variable-dropdown', 'value'),
     Input('usage-frequency-radio', 'value'),
     Input("url", "pathname")]
)
def update_usage_graph(start_date, end_date, selected_vars, freq, pathname):
    category = get_category_from_pathname(pathname)
    if not selected_vars or not category:
        return create_empty_figure("No variables selected.", category or "Unknown", "Usage Analysis")

    df_filtered = filter_data(data, start_date, end_date, selected_vars, frequency=freq)

    if df_filtered.empty or df_filtered[selected_vars].isnull().all().all():
        return create_empty_figure(
            "No data available at the selected frequency for the chosen date range.",
            category, "Usage Analysis"
        )

    fig = create_line_plot(
        df_filtered,
        x_column='Timestamp',
        y_columns=selected_vars,
        title=f'{category} - Usage Analysis'
    )
    return fig

# Run the app
if __name__ == "__main__":
    app.run_server(port=8050, debug=True)
