# components/modals.py
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
