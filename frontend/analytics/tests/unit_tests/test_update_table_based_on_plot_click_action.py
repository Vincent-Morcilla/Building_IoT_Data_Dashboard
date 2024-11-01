import pytest
import pandas as pd
from callbacks.plot_callbacks import update_table_based_on_plot_click_action
from data.plot_configs import plot_configs


def test_update_table_based_on_plot_click_action():
    """
    Test the `update_table_based_on_plot_click_action` function to ensure that it correctly
    updates the table data based on a simulated plot click event with inconsistent classes.
    """
    # Source data for the test
    source_data = pd.DataFrame(
        {
            "entity": ["Entity_1", "Entity_2", "Entity_3", "Entity_4"],
            "brick_class": ["Class_A", "Class_B", "Class_C", "Class_D"],
            "brick_class_in_mapping": ["Class_A", "Class_E", "Class_C", "Class_F"],
            "brick_class_is_consistent": [True, False, True, False],
        }
    )

    # Simulated input for a plot click on inconsistent classes
    input_values = [False]
    outputs = [
        {"component_id": "inconsistent-classes-table", "component_property": "data"}
    ]

    # Define the interaction configuration
    interaction = {
        "action": "update_table_based_on_plot_click",
        "data_mapping": {
            "from": "class-consistency-pie",
            "to": "inconsistent-classes-table",
        },
        "data_processing": {
            "filter": {"brick_class_is_consistent": {"equals": "selected_class"}}
        },
    }

    # Define the trigger configuration
    triggers = [{"input_key": "selected_class"}]

    # Mock plot_configs with necessary components
    plot_configs["test-key"] = {
        "components": [
            {
                "id": "class-consistency-pie",
                "type": "plot",
                "library": "px",
                "function": "pie",
                "kwargs": {
                    "data_frame": source_data,
                    "names": "brick_class_is_consistent",
                },
                "layout_kwargs": {},
                "css": {},
            },
            {
                "id": "inconsistent-classes-table",
                "type": "table",
                "dataframe": source_data,
                "kwargs": {},
                "css": {},
            },
        ]
    }

    # Execute the function with test inputs
    output_results = update_table_based_on_plot_click_action(
        input_values,
        outputs,
        interaction,
        triggers=triggers,
    )

    # Assert expected outcomes
    assert len(output_results) == 1, "Expected a single output result"
    assert isinstance(
        output_results[0], list
    ), "Expected output to be a list of records"

    # Expected filtered data based on input values
    expected_df = source_data[source_data["brick_class_is_consistent"] == False]

    # Validate that the returned table data matches the expected filtered data
    assert output_results[0] == expected_df.to_dict(
        "records"
    ), "Filtered data does not match expected result"
