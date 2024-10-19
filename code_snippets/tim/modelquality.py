"""This module provides a class for assessing the quality of a Brick model."""

from pathlib import Path

import brickschema
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import rdflib

# from rdflib import Namespace
# from rdflib.namespace import RDFS, SKOS, BRICK


class ModelQuality:
    """TODO"""

    def __init__(
        self,
        brick_model="../../datasets/bts_site_b_train/Site_B.ttl",
        brick_schema="../../datasets/bts_site_b_train/Brick_v1.2.1.ttl",
        mapper="../../datasets/bts_site_b_train/mapper_TrainOnly.csv",
        time_series=None,
    ):
        """TODO
        + FIXME: remove these default values, decide whether we need the time_series parameter
        """
        self._brick_model = Path(brick_model)
        self._brick_schema = Path(brick_schema)
        self._mapper = Path(mapper)

        if not self._brick_model.is_file():
            raise FileNotFoundError(f"Brick model file not found: {self._brick_model}")

        if not self._brick_schema.is_file():
            raise FileNotFoundError(
                f"Brick schema file not found: {self._brick_schema}"
            )

        if not self._mapper.is_file():
            raise FileNotFoundError(f"Mapper file not found: {self._mapper}")

        self._g_building = brickschema.Graph().load_file(self._brick_model)
        self._g_brick = brickschema.Graph().load_file(self._brick_schema)
        self._mapping_df = pd.read_csv(self._mapper, index_col=0)

        # Remove rows where the filename is 'FILE NOT SAVED'
        self._mapping_df = self._mapping_df[
            self._mapping_df["Filename"].str.contains("FILE NOT SAVED") == False
        ]

        # Initialise our DataFrame with all entities in the Brick model
        self._df = self._get_brick_entities()

        self._run_analysis()
        self._df.to_csv("model_quality_v2.csv")

    def get_analyses(self):
        """TODO"""
        analyses = {}
        analyses |= self.get_recognised_entity_analysis()
        analyses |= self.get_associated_units_analysis()
        analyses |= self.get_associated_timeseries_data_analysis()
        analyses |= self.get_class_consistency_analysis()

        return analyses

    def get_recognised_entity_analysis(self):
        """TODO"""
        df = self._df[["brick_class", "entity_id", "class_in_brick_schema"]].copy()
        df.sort_values(
            by=["class_in_brick_schema", "brick_class", "entity_id"], inplace=True
        )

        recognised_df = df[df["class_in_brick_schema"] == True].copy()
        recognised_df.drop(columns=["class_in_brick_schema"], inplace=True)

        unrecognised_df = df[df["class_in_brick_schema"] == False].copy()
        unrecognised_df.drop(columns=["class_in_brick_schema"], inplace=True)

        df["class_in_brick_schema"] = df["class_in_brick_schema"].apply(
            lambda x: "Recognised" if x else "Unrecognised"
        )

        recognised_df_pie = df["class_in_brick_schema"]
        unrecognised_df_pie = unrecognised_df["brick_class"]

        config = {
            "ModelQuality_RecognisedEntities": {
                "PieChartAndTable": {
                    "title": "Brick Entities in Building Model Recognised by Brick Schema",
                    "pie_charts": [
                        {
                            "title": "Proportion of Recognised vs Unrecognised Entities",
                            "labels": "class_in_provided_brick",
                            "textinfo": "percent+label",
                            "dataframe": recognised_df_pie,
                        },
                        {
                            "title": "Unrecognised Entities by Class",
                            "labels": "brick_class",
                            "textinfo": "percent+label",
                            "dataframe": unrecognised_df_pie,
                        },
                    ],
                    "tables": [
                        {
                            "title": "Unrecognised Entities",
                            "columns": ["Brick Class", "Entity ID"],
                            "rows": ["brick_class", "entity_id"],
                            "dataframe": recognised_df,
                        },
                        {
                            "title": "Recognised Entities",
                            "columns": ["Brick Class", "Entity ID"],
                            "rows": ["brick_class", "entity_id"],
                            "dataframe": unrecognised_df,
                        },
                    ],
                },
            }
        }

        return config

    def plot_recognised_entity_analysis(self):
        """TODO"""
        df = self._df[["brick_class", "entity_id", "class_in_brick_schema"]].copy()
        df.sort_values(
            by=["class_in_brick_schema", "brick_class", "entity_id"], inplace=True
        )

        recognised_df = df[df["class_in_brick_schema"] == True].copy()
        unrecognised_df = df[df["class_in_brick_schema"] == False].copy()

        df["class_in_brick_schema"] = df["class_in_brick_schema"].apply(
            lambda x: "Recognised" if x else "Unrecognised"
        )

        number_recognised_labels = df["class_in_brick_schema"].value_counts().index
        number_recognised_values = df["class_in_brick_schema"].value_counts().values

        unrecognised_by_class_labels = (
            unrecognised_df["brick_class"].value_counts().index
        )
        unrecognised_by_class_values = (
            unrecognised_df["brick_class"].value_counts().values
        )

        fig = make_subplots(
            rows=3,
            cols=2,
            vertical_spacing=0.05,
            subplot_titles=[
                "Proportion of Recognised Entities",
                "Unrecognised Entities by Class",
                "Unrecognised Entities",
                "Recognised Entities",
            ],
            specs=[
                [{"type": "pie"}, {"type": "pie"}],
                [{"type": "table", "colspan": 2}, None],
                [{"type": "table", "colspan": 2}, None],
            ],
        )

        fig.add_trace(
            go.Pie(
                labels=number_recognised_labels,
                values=number_recognised_values,
                textposition="inside",
                textinfo="percent+label",
                name="",
            ),
            row=1,
            col=1,
        )

        fig.add_trace(
            go.Pie(
                labels=unrecognised_by_class_labels,
                values=unrecognised_by_class_values,
                textposition="inside",
                textinfo="percent+label",
                name="",
            ),
            row=1,
            col=2,
        )

        fig.add_trace(
            go.Table(
                header=dict(
                    values=["Brick Class", "Entity ID"],
                    font=dict(size=10),
                    align="left",
                ),
                cells=dict(
                    values=[
                        unrecognised_df[k].tolist() for k in unrecognised_df.columns[:2]
                    ],
                    align="left",
                ),
            ),
            row=2,
            col=1,
        )

        fig.add_trace(
            go.Table(
                header=dict(
                    values=["Brick Class", "Entity ID"],
                    font=dict(size=10),
                    align="left",
                ),
                cells=dict(
                    values=[
                        recognised_df[k].tolist() for k in recognised_df.columns[:2]
                    ],
                    align="left",
                ),
            ),
            row=3,
            col=1,
        )

        fig.update_layout(
            height=1200,
            showlegend=False,
            title_text="Brick Entities in Building Model Recognised by Brick Schema",
            title_x=0.5,
        )

        return fig

    def get_associated_units_analysis(self):
        """TODO"""
        df = self._df[["brick_class", "stream_id", "unit", "unit_is_named"]].copy()
        df.dropna(subset=["stream_id"], inplace=True)
        df.sort_values(by=["brick_class", "stream_id"], inplace=True)
        df["has_unit"] = df["unit"].apply(
            lambda x: "No units" if pd.isna(x) else "Units"
        )

        proportion_with_units_pie = df["has_unit"]

        streams_without_units = df[pd.isna(df["unit"])].copy()
        streams_without_units.sort_values(by=["brick_class", "stream_id"], inplace=True)
        streams_without_units.drop(
            columns=["unit", "unit_is_named", "has_unit"], inplace=True
        )

        stream_with_named_units = df.dropna(subset=["unit"]).copy()
        stream_with_named_units["has_named_unit"] = df["unit_is_named"].apply(
            lambda x: "Machine readable" if x else "Not machine readable"
        )
        stream_with_named_units_pie = stream_with_named_units["has_named_unit"]

        streams_with_anonymous_units = df[df["unit_is_named"] == False].copy()
        streams_with_anonymous_units.drop(
            columns=["unit_is_named", "has_unit"], inplace=True
        )

        config = {
            "ModelQuality_AssociatedUnits": {
                "PieChartAndTable": {
                    "title": "Brick Entities in Building Model Recognised by Brick Schema",
                    "pie_charts": [
                        {
                            "title": "Proportion of Streams with Units",
                            "labels": "has_unit",
                            "textinfo": "percent+label",
                            "dataframe": proportion_with_units_pie,
                        },
                        {
                            "title": "Units that are Machine Readable",
                            "labels": "has_named_unit",
                            "textinfo": "percent+label",
                            "dataframe": stream_with_named_units_pie,
                        },
                    ],
                    "tables": [
                        {
                            "title": "Streams without Units",
                            "columns": ["Brick Class", "Stream ID"],
                            "rows": ["brick_class", "stream_id"],
                            "dataframe": streams_without_units,
                        },
                        {
                            "title": "Streams with Non-Machine Readable Units",
                            "columns": ["Brick Class", "Stream ID", "Unit"],
                            "rows": ["brick_class", "stream_id", "unit"],
                            "dataframe": streams_with_anonymous_units,
                        },
                    ],
                }
            }
        }

        return config

    def plot_associated_units_analysis(self):
        """TODO"""
        df = self._df[["brick_class", "stream_id", "unit", "unit_is_named"]].copy()
        df.dropna(subset=["stream_id"], inplace=True)
        df.sort_values(by=["brick_class", "stream_id"], inplace=True)
        df["has_unit"] = df["unit"].apply(
            lambda x: "No units" if pd.isna(x) else "Units"
        )

        number_with_units_labels = df["has_unit"].value_counts().index
        number_with_units_values = df["has_unit"].value_counts().values

        streams_without_units = df[pd.isna(df["unit"])].copy()
        streams_without_units.sort_values(by=["brick_class", "stream_id"], inplace=True)

        stream_with_named_units = df.dropna(subset=["unit"]).copy()
        stream_with_named_units["has_named_unit"] = df["unit_is_named"].apply(
            lambda x: "Machine readable" if x else "Not machine readable"
        )

        streams_with_anonymous_units = df[df["unit_is_named"] == False]
        number_with_named_units_labels = (
            stream_with_named_units["has_named_unit"].value_counts().index
        )
        number_with_named_units_values = (
            stream_with_named_units["has_named_unit"].value_counts().values
        )

        fig = make_subplots(
            rows=3,
            cols=2,
            vertical_spacing=0.05,
            subplot_titles=[
                "Proportion of Streams with Units",
                "Units that are Machine Readable",
                "Streams without Units",
                "Streams with Non-Machine Readable Units",
            ],
            specs=[
                [{"type": "pie"}, {"type": "pie"}],
                [{"type": "table", "colspan": 2}, None],
                [{"type": "table", "colspan": 2}, None],
            ],
        )

        fig.add_trace(
            go.Pie(
                labels=number_with_units_labels,
                values=number_with_units_values,
                textposition="inside",
                textinfo="percent+label",
                name="",
            ),
            row=1,
            col=1,
        )

        fig.add_trace(
            go.Pie(
                labels=number_with_named_units_labels,
                values=number_with_named_units_values,
                textposition="inside",
                textinfo="percent+label",
                name="",
            ),
            row=1,
            col=2,
        )

        fig.add_trace(
            go.Table(
                header=dict(
                    values=["Brick Class", "Stream ID"],
                    font=dict(size=10),
                    align="left",
                ),
                cells=dict(
                    values=[
                        streams_without_units[k].tolist()
                        for k in streams_without_units.columns[:2]
                    ],
                    align="left",
                ),
            ),
            row=2,
            col=1,
        )

        fig.add_trace(
            go.Table(
                header=dict(
                    values=["Brick Class", "Stream ID", "Unit"],
                    font=dict(size=10),
                    align="left",
                ),
                cells=dict(
                    values=[
                        streams_with_anonymous_units[k].tolist()
                        for k in streams_with_anonymous_units.columns[:3]
                    ],
                    align="left",
                ),
            ),
            row=3,
            col=1,
        )

        fig.update_layout(
            height=1200,
            showlegend=False,
            title_text="Data Sources in Building Model with Associated Units",
            title_x=0.5,
        )

        return fig

    def get_associated_timeseries_data_analysis(self):
        """TODO"""
        df = self._df[["brick_class", "stream_id", "stream_exists_in_mapping"]].copy()
        df.dropna(subset=["stream_id"], inplace=True)
        df.sort_values(by=["brick_class", "stream_id"], inplace=True)
        df["has_data"] = df["stream_exists_in_mapping"].apply(
            lambda x: "Data" if x else "No data"
        )

        proportion_with_streams_pie = df["has_data"].copy()

        missing_streams_by_class_pie = df[
            df["stream_exists_in_mapping"] == False
        ].copy()["brick_class"]

        have_data_df = df[df["stream_exists_in_mapping"] == True].copy()[
            ["brick_class", "stream_id"]
        ]
        missing_data_df = df[df["stream_exists_in_mapping"] == False].copy()[
            ["brick_class", "stream_id"]
        ]

        config = {
            "ModelQuality_TimeseriesData": {
                "PieChartAndTable": {
                    "title": "Data Sources in Building Model with Timeseries Data",
                    "pie_charts": [
                        {
                            "title": "Proportion of Data Sources with Timeseries Data",
                            "labels": "has_data",
                            "textinfo": "percent+label",
                            "dataframe": proportion_with_streams_pie,
                        },
                        {
                            "title": "Missing Data by Class",
                            "labels": "stream_exists_in_mapping",
                            "textinfo": "percent+label",
                            "dataframe": missing_streams_by_class_pie,
                        },
                    ],
                    "tables": [
                        {
                            "title": "Data Sources with Missing Timeseries Data",
                            "columns": ["Brick Class", "Stream ID"],
                            "rows": ["brick_class", "stream_id"],
                            "dataframe": missing_data_df,
                        },
                        {
                            "title": "Data Sources with Available Timeseries Data",
                            "columns": ["Brick Class", "Stream ID"],
                            "rows": ["brick_class", "stream_id"],
                            "dataframe": have_data_df,
                        },
                    ],
                }
            }
        }

        return config

    def plot_associated_timeseries_data_analysis(self):
        """TODO"""
        df = self._df[["brick_class", "stream_id", "stream_exists_in_mapping"]].copy()
        df.dropna(subset=["stream_id"], inplace=True)
        df.sort_values(by=["brick_class", "stream_id"], inplace=True)
        df["has_data"] = df["stream_exists_in_mapping"].apply(
            lambda x: "Data" if x else "No data"
        )

        number_with_data_labels = df["has_data"].value_counts().index
        number_with_data_values = df["has_data"].value_counts().values

        missing_data_by_class = df[df["stream_exists_in_mapping"] == False].copy()
        missing_data_by_class = missing_data_by_class.groupby("brick_class").count()

        missing_data_by_class_labels = (
            df[df["stream_exists_in_mapping"] == False]["brick_class"]
            .value_counts()
            .index
        )
        missing_data_by_class_values = (
            df[df["stream_exists_in_mapping"] == False]["brick_class"]
            .value_counts()
            .values
        )

        have_data_df = df[df["stream_exists_in_mapping"] == True]
        missing_data_df = df[df["stream_exists_in_mapping"] == False]

        fig = make_subplots(
            rows=3,
            cols=2,
            vertical_spacing=0.05,
            subplot_titles=[
                "Proportion of Data Sources",
                "Missing Data by Class",
                "Data Sources with Missing Timeseries Data",
                "Data Sources with Available Timeseries Data",
            ],
            specs=[
                [{"type": "pie"}, {"type": "pie"}],
                [{"type": "table", "colspan": 2}, None],
                [{"type": "table", "colspan": 2}, None],
            ],
        )

        fig.add_trace(
            go.Pie(
                labels=number_with_data_labels,
                values=number_with_data_values,
                textposition="inside",
                textinfo="percent+label",
                name="",
            ),
            row=1,
            col=1,
        )

        fig.add_trace(
            go.Pie(
                labels=missing_data_by_class_labels,
                values=missing_data_by_class_values,
                textposition="inside",
                textinfo="percent+label",
                name="",
            ),
            row=1,
            col=2,
        )

        fig.add_trace(
            go.Table(
                header=dict(
                    values=["Brick Class", "Stream ID"],
                    font=dict(size=10),
                    align="left",
                ),
                cells=dict(
                    values=[
                        missing_data_df[k].tolist() for k in missing_data_df.columns[:2]
                    ],
                    align="left",
                ),
            ),
            row=2,
            col=1,
        )

        fig.add_trace(
            go.Table(
                header=dict(
                    values=["Brick Class", "Stream ID"],
                    font=dict(size=10),
                    align="left",
                ),
                cells=dict(
                    values=[have_data_df[k].tolist() for k in have_data_df.columns[:2]],
                    align="left",
                ),
            ),
            row=3,
            col=1,
        )

        fig.update_layout(
            height=1200,
            showlegend=False,
            title_text="Data Sources in Building Model with Timeseries Data",
            title_x=0.5,
        )

        return fig

    def get_class_consistency_analysis(self):
        """TODO"""
        df = self._df[
            [
                "brick_class",
                "brick_class_in_mapper",
                "entity_id",
                "brick_class_is_consistent",
            ]
        ].copy()
        df.dropna(subset=["brick_class_in_mapper"], inplace=True)
        df.sort_values(
            by=["brick_class", "brick_class_in_mapper", "entity_id"], inplace=True
        )
        df["consistency"] = df["brick_class_is_consistent"].apply(
            lambda x: "Consistent" if x else "Inconsistent"
        )

        proportion_consistent_pie = df["consistency"].copy()

        inconsistent_df = df[df["brick_class_is_consistent"] == False].copy()[
            ["brick_class", "brick_class_in_mapper", "entity_id"]
        ]

        inconsistent_by_class_pie = inconsistent_df["brick_class"].copy()

        config = {
            "ModelQuality_ClassConsistency": {
                "PieChartAndTable": {
                    "title": "Data Sources in Building Model with Timeseries Data",
                    "pie_charts": [
                        {
                            "title": "Proportion of Data Sources",
                            "labels": "consistency",
                            "textinfo": "percent+label",
                            "dataframe": proportion_consistent_pie,
                        },
                        {
                            "title": "Inconsistent Data Sources by Class",
                            "labels": "brick_class",
                            "textinfo": "percent+label",
                            "dataframe": inconsistent_by_class_pie,
                        },
                    ],
                    "tables": [
                        {
                            "title": "Data Sources with Inconsistent Brick Class",
                            "columns": [
                                "Brick Class in Model",
                                "Brick Class in Mapper",
                                "Entity ID",
                            ],
                            "rows": [
                                "brick_class",
                                "brick_class_in_mapper",
                                "entity_id",
                            ],
                            "dataframe": inconsistent_df,
                        },
                    ],
                }
            }
        }

        return config

    def plot_class_consistency_analysis(self):
        """TODO"""
        df = self._df[
            [
                "brick_class",
                "brick_class_in_mapper",
                "entity_id",
                "brick_class_is_consistent",
            ]
        ].copy()
        df.dropna(subset=["brick_class_in_mapper"], inplace=True)
        df.sort_values(
            by=["brick_class", "brick_class_in_mapper", "entity_id"], inplace=True
        )
        df["consistency"] = df["brick_class_is_consistent"].apply(
            lambda x: "Consistent" if x else "Inconsistent"
        )

        number_consistent_labels = df["consistency"].value_counts().index
        number_consistent_values = df["consistency"].value_counts().values

        inconsistent_df = df[df["brick_class_is_consistent"] == False].copy()

        inconsistent_by_class_labels = (
            inconsistent_df["brick_class"].value_counts().index
        )
        inconsistent_by_class_values = (
            inconsistent_df["brick_class"].value_counts().values
        )

        fig = make_subplots(
            rows=2,
            cols=2,
            # shared_xaxes=True,
            # vertical_spacing=0.1,
            vertical_spacing=0.05,
            subplot_titles=[
                "Proportion of Data Sources",
                "Inconsistent by Class",
                "Data Sources with Inconsistent Brick Class",
            ],
            specs=[
                [{"type": "pie"}, {"type": "pie"}],
                [{"type": "table", "colspan": 2}, None],
            ],
        )

        fig.add_trace(
            go.Pie(
                labels=number_consistent_labels,
                values=number_consistent_values,
                textposition="inside",
                textinfo="percent+label",
                name="",
            ),
            row=1,
            col=1,
        )

        fig.add_trace(
            go.Pie(
                labels=inconsistent_by_class_labels,
                values=inconsistent_by_class_values,
                textposition="inside",
                textinfo="percent+label",
                name="",
            ),
            row=1,
            col=2,
        )

        fig.add_trace(
            go.Table(
                header=dict(
                    values=[
                        "Brick Class in Model",
                        "Brick Class in Mapper",
                        "Entity ID",
                    ],
                    font=dict(size=10),
                    align="left",
                ),
                cells=dict(
                    values=[
                        inconsistent_df[k].tolist() for k in inconsistent_df.columns[:3]
                    ],
                    align="left",
                ),
            ),
            row=2,
            col=1,
        )

        fig.update_layout(
            height=800,
            showlegend=False,
            title_text="Data Sources with Inconsistent Brick Class between Model and Mapper",
            title_x=0.5,
        )

        return fig

    def _run_analysis(self):
        """TODO"""

        # Check if all entities in the building model are in the Brick schema
        self._recognised_entity_analysis()

        # Check if all streams in the building model have units of measurement
        self._associated_units_analysis()

        # Check if all streams in the building model have associated timeseries data
        self._associated_timeseries_data_analysis()

        # Check if the classes of the entities in the building model are consistent with the mapping
        self._class_consistency_analysis()

        for col in self._df.columns:
            self._df[col] = self._df[col].apply(ModelQuality._defrag_uri)

    def _recognised_entity_analysis(self):
        """Amend the DataFrame to include the results of the recognised entity analysis."""

        # Check if all entities in the provided model are in the Brick schema
        self._df["class_in_brick_schema"] = self._df["brick_class"].apply(
            lambda x: (x, None, None) in self._g_brick
        )

    def _associated_units_analysis(self):
        """Amend the DataFrame to include the results of the associated units analysis."""

        # If there are no named units or no anonymous units, then these columns
        # will not be present in the DataFrame. Add them if they are missing.
        if "named_unit" not in self._df.columns:
            self._df["named_unit"] = None
        if "anonymous_unit" not in self._df.columns:
            self._df["anonymous_unit"] = None

        # Add a `unit` column to the DataFrame that combines the named and anonymous units
        self._df = self._df.assign(
            unit=lambda x: x["named_unit"].combine_first(x["anonymous_unit"])
        )

        def unit_is_named(r):
            if pd.isna(r.unit):
                return None

            return not pd.isna(r.named_unit)

        self._df["unit_is_named"] = self._df.apply(unit_is_named, axis=1)

    def _associated_timeseries_data_analysis(self):
        """Amend the DataFrame to include the results of the associated timeseries data analysis."""

        def stream_exists_in_mapping(s, mapping_df):
            if pd.isna(s):
                return None
            return str(s).strip() in mapping_df["StreamID"].values

        self._df["stream_exists_in_mapping"] = self._df["stream_id"].apply(
            stream_exists_in_mapping, args=(self._mapping_df,)
        )

    def _class_consistency_analysis(self):
        """Amend the DataFrame to include the results of the class consistency analysis."""

        # def brick_class_in_mapper(s, mapping_df):
        #     if pd.isna(s):
        #         return None
        #     mapping_df['StreamID']
        #     return str(s).strip() in mapping_df['StreamID'].values

        # Create a temporary column with 'stream_id' converted to a string for the join
        self._df["stream_id_str"] = self._df["stream_id"].apply(lambda x: str(x))

        # Perform the left join
        self._df = pd.merge(
            self._df,
            self._mapping_df[["StreamID", "strBrickLabel"]],
            how="left",
            left_on="stream_id_str",
            right_on="StreamID",
        )

        # Rename the 'strBrickLabel' column to 'brick_class_in_mapper'
        self._df.rename(
            columns={"strBrickLabel": "brick_class_in_mapper"}, inplace=True
        )

        # Create a temporary column with the fragment of the 'brick_class' for comparison
        self._df["brick_class_fragment"] = self._df["brick_class"].apply(
            lambda x: str(x.fragment) if x is not None else None
        )

        # Compare the fragment of the 'brick_class' with the 'brick_class_in_mapper'
        self._df["brick_class_is_consistent"] = np.where(
            pd.isna(
                self._df["brick_class_in_mapper"]
            ),  # Check if brick_class_in_mapper is empty
            None,  # Leave empty where there's no mapping value
            self._df["brick_class_fragment"]
            == self._df["brick_class_in_mapper"],  # Compare fragment with the mapping
        )

        # Drop the temporary columns
        self._df.drop(
            columns=["stream_id_str", "StreamID", "brick_class_fragment"], inplace=True
        )

    def _get_brick_entities(self):
        """
        Get all entities in the Brick model and their associated classes.
        If a stream is associated with an entity, the stream ID is also included.
        If the entity has a named unit, the unit is included.
        If the entity has an anonymous unit, the unit value is included.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing all entities in the Brick model.
        """
        query = """
        SELECT ?entity_id ?brick_class ?stream_id ?named_unit ?anonymous_unit WHERE {
            ?entity_id a ?brick_class .
            OPTIONAL { ?entity_id senaps:stream_id ?stream_id } .
            OPTIONAL { ?entity_id brick:hasUnit ?named_unit .
                        filter ( strstarts(str(?named_unit),str(unit:)) ) } .
            OPTIONAL { ?entity_id brick:hasUnit [ brick:value ?anonymous_unit ] } .
            filter ( strstarts(str(?brick_class),str(brick:)) ) .
        }
        """
        return ModelQuality._sparql_to_df(self._g_building, query)

    @staticmethod
    def _sparql_to_df(g, q, **kwargs):
        """
        Run a SPARQL query on the graph and return the results as a pandas DataFrame.

        Parameters
        ----------
        g : rdflib.Graph
            The graph to query.
        q : str
            The SPARQL query string.
        kwargs : dict
            Additional keyword arguments to pass to the query method.

        Returns
        -------
        pd.DataFrame
            The results of the query as a DataFrame.
        """
        res = g.query(q, **kwargs)
        df = pd.DataFrame(res.bindings)
        df.columns = df.columns.map(str)
        # TODO: should be considering duplicates as a dimension of quality?
        df.drop_duplicates(inplace=True)
        return df

    @staticmethod
    def _defrag_uri(uri):
        """
        Extract the fragment from a URI.

        Parameters
        ----------
        uri : rdflib.term.URIRef
            The URI to defragment.

        Returns
        -------
        str
            The fragment of the URI.
        """
        if isinstance(uri, rdflib.term.URIRef):
            if "#" in uri:
                return uri.fragment
            elif "/" in uri:
                return uri.split("/")[-1]
        return uri

    @staticmethod
    def _to_csv(df, path):
        """TODO"""
        # Strip out all the RDF prefixes from the DataFrame to make it more readable
        for col in df.columns:
            df[col] = df[col].apply(ModelQuality._defrag_uri)

        df.to_csv(path, index=False)


if __name__ == "__main__":
    # model = "../../datasets/bts_site_b_train/Site_B_tim.ttl"
    # schema = "../../datasets/bts_site_b_train/Brick_v1.2.1.ttl"
    # mapping = None
    # time_series = None
    # mq = ModelQuality(model, schema, mapping, time_series)
    mq = ModelQuality()
    # print(mq._df.head())
    # mq.plot_recognised_entity_analysis()
    # mq.plot_associated_units_analysis()
    # mq.plot_class_consistency_analysis()
    config = mq.get_analyses()
    for key, value in config.items():
        print(key)
        for k, v in value.items():
            print(k)
            for i, j in v.items():
                print(i)
                print(j)
        print("====================")
    print()
