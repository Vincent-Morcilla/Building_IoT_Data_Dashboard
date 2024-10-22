def general_callbacks(app):
    from dash import dcc, Input, Output, html
    from components.layout import home_page_content

    # A hidden `dcc.Location` component to manage URL changes
    app.layout.children.append(dcc.Location(id='url_callback', refresh=True))

    # Callback to go back to the homepage when the logo is clicked
    @app.callback(
        Output('url_callback', 'href'),  # This will change the URL
        Input('logo-button', 'n_clicks')
    )
    def redirect_to_home(n_clicks):
        if n_clicks > 0:
            return "/"
        return None
    

    @app.callback(
    Output('page-content', 'children'),
    [Input('url', 'pathname')]  # Listening to the URL changes
    )
    def display_page(pathname):
        if pathname == '/':  # Check if the user is on the home page
            return home_page_content()  # Display home page content
        else:
            # Return a 404 message or default content if the path doesn't match
            return html.Div([html.H1("Sorry, there was no visualisation generated.")])