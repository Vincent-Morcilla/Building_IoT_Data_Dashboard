import dash_bootstrap_components as dbc

def create_error_modal():
    return dbc.Modal(
        [
            dbc.ModalHeader("Error"),
            dbc.ModalBody(id="error-message"),
            dbc.ModalFooter(
                dbc.Button("Close", id="close-error-modal", className="ml-auto", n_clicks=0)
            ),
        ],
        id="error-modal",
        centered=True,
        is_open=False
    )

def create_warning_modal():
    return dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle("Navigate to Homepage?")),
            dbc.ModalBody("Are you sure you want to go to the homepage?"),
            dbc.ModalFooter(
                [
                    dbc.Button("Yes", id="modal-yes-button", color="success", n_clicks=0),
                    dbc.Button("No", id="modal-no-button", outline=True, color="success", n_clicks=0),
                ]
            ),
        ],
        id="warning-modal",
        is_open=False,
    )