def register_callbacks(app):
    import dash
    from dash import callback_context, html
    from dash.dependencies import Input, Output, State
    from dash.exceptions import PreventUpdate
    import threading
    import sys
    import os

    from .file_screening import screen_uploaded_files, screen_zip_file

    # Add the root directory to the system path
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

    # PipelineProgress class and pipeline functions defined
    from backend.pipeline import get_pipeline_progress, data_pipeline

    # Callback to handle file uploads, automatically screen them, and display result
    @app.callback(
        [Output('file-names', 'children'),
         Output('file-validity-store', 'data')],
        [Input('upload-data', 'contents')],
        [State('upload-data', 'filename')]
    )
    def handle_file_upload(file_contents, file_names):
        if file_contents is None:
            return "", False

        # Screen uploaded files
        is_valid, message = screen_uploaded_files(file_names)

        if is_valid:
            return message, True  # Display file names and set validity
        else:
            return html.Div(f"Error: {message}", style={'color': '#7a1905'}), False  # Show error message

    # Enable the "Screen Files" button based on zip file path input
    @app.callback(
        Output('screen-files-button', 'disabled'),
        [Input('zip-file-path', 'value')]
    )
    def toggle_screen_files_button(zip_file_path):
        return not zip_file_path or not zip_file_path.strip()  # Enable only when path is non-empty

    # Screen zip file upon "Screen Files" button click and display result
    @app.callback(
        [Output('screen-files-button', 'children'),
         Output('screen-result', 'children'),
         Output('zip-validity-store', 'data')],
        [Input('screen-files-button', 'n_clicks')],
        [State('zip-file-path', 'value')],
        prevent_initial_call=True
    )
    def handle_zip_screen(n_clicks, zip_file_path):
        # Screen the zip file for .pkl files
        is_valid, message = screen_zip_file(zip_file_path)

        if is_valid:
            return "Screen Files", message, True  # Show success message and set validity
        else:
            return "Screen Files", html.Div(f"Error: {message}", style={'color': '#7a1905'}), False  # Show error message

    # Enable/Disable the "Analyse" button based on form inputs and screening analytics
    @app.callback(
        Output('analyse-button', 'disabled'),
        [Input('dataset-name-input', 'value'),
         Input('file-validity-store', 'data'),
         Input('zip-validity-store', 'data')]
    )
    def toggle_analyse_button(dataset_name, file_validity, zip_validity):
        if dataset_name and dataset_name.strip() and file_validity and zip_validity:
            return False  # Enable the "Analyse" button
        return True  # Disable if any condition is not met

    # Manage progress with callbacks
    @app.callback(
        [Output('form-container', 'style'),
        Output('progress-section', 'style'),
        Output('interval-component', 'disabled'),
        Output('session-id-store', 'data')],
        [Input('analyse-button', 'n_clicks')],
        [State('dataset-name-input', 'value'),
        State('upload-data', 'contents'),
        State('upload-data', 'filename'),
        State('zip-file-path', 'value')]
    )
    def start_pipeline(n_clicks, dataset_name, uploaded_files_contents, uploaded_files_names, zip_file_path):
        if n_clicks > 0:
            # Hide the form
            form_style = {'display': 'none'}
            # Show the progress section
            progress_style = {'display': 'block'}
            # Enable the interval component
            interval_disabled = False

            # Process the uploaded files
            processed_files = []
            if uploaded_files_contents and uploaded_files_names:
                for content, name in zip(uploaded_files_contents, uploaded_files_names):
                    processed_files.append({
                        'filename': name,
                        'content': content
                    })

            # Prepare data for the pipeline
            data = {
                'dataset_name': dataset_name,
                'uploaded_files': processed_files,
                'zip_file_path': zip_file_path
            }

            # Generate a session ID
            session_id = f"session_{n_clicks}"

            # Start the pipeline in a new thread
            thread = threading.Thread(target=data_pipeline, args=(data, session_id), daemon=True)
            thread.start()

            # Return the session ID
            return form_style, progress_style, interval_disabled, session_id

        raise PreventUpdate
    
    # Update progress and pipline error handling
    @app.callback(
        [Output('status', 'children'),
        Output('progress-bar', 'value'),
        Output('progress-bar', 'label'),
        Output('url', 'pathname'),
        Output('url', 'search'),
        Output('error-message', 'children'),
        Output('error-modal', 'is_open')],
        [Input('interval-component', 'n_intervals'),
        Input('close-error-modal', 'n_clicks')],
        [State('session-id-store', 'data')]
    )
    def update_progress_or_close_modal(n_intervals, close_clicks, session_id):
        ctx = dash.callback_context  # Determine which input triggered the callback
        if not ctx.triggered:
            raise PreventUpdate

        trigger = ctx.triggered[0]['prop_id'].split('.')[0]

        if trigger == 'interval-component':
            if session_id:
                # Get the progress from the pipeline using the session_id
                progress = get_pipeline_progress(session_id)
                if progress:
                    progress_data = progress.get_progress()
                    stage = progress_data.get('stage', 'Unknown')
                    percentage = progress_data.get('percentage', 0)
                    error = progress_data.get('error')

                    # Handle errors
                    if error:
                        # Trigger the modal to show the error and return the error message
                        return f"Error: {error}", 0, "", dash.no_update, dash.no_update, error, True

                    # If the pipeline is complete, redirect to the analytics page
                    if percentage == 100:
                        return f"Stage: {stage}", percentage, f"{percentage}%", '/analytics', f"?session_id={session_id}", dash.no_update, False

                    return f"Stage: {stage}", percentage, f"{percentage}%", dash.no_update, dash.no_update, dash.no_update, False

        elif trigger == 'close-error-modal':
            if close_clicks > 0:
                # Close the modal and redirect to the homepage
                return dash.no_update, dash.no_update, dash.no_update, '/', dash.no_update, dash.no_update, False

        raise PreventUpdate