def general_callbacks(app, categories_structure):
    from dash import Input, Output, html
    from components.layout import home_page_content
    from components.tabs import create_tab_layout

    # Callback to go back to the homepage when the logo is clicked
    @app.callback(
        Output('url', 'href'),
        Input('logo-button', 'n_clicks')
    )
    def redirect_to_home(n_clicks):
        if n_clicks and n_clicks > 0:
            return "/"
        return None

    # Callback to generate the page content based on the URL
    @app.callback(
        Output('page-content', 'children'),
        [Input('url', 'pathname')]
    )
    def display_page(pathname):
        # If user is on the home page, display home page content
        if pathname == '/':
            return home_page_content()

        # Extract the selected category from the pathname (remove the leading '/')
        selected_category = pathname.lstrip('/')

        # Handle the case where the selected category does not exist in the categories structure
        if selected_category and selected_category != "home":
            return create_tab_layout(selected_category, categories_structure)

        # Default content if no valid category is selected or invalid path
        return html.Div([html.H1("Sorry, there was no visualisation generated.")])
