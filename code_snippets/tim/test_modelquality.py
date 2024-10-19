from dash import Dash, dcc, html

from modelquality import ModelQuality

mq = ModelQuality()
fig1 = mq.plot_recognised_entity_analysis()
fig2 = mq.plot_associated_units_analysis()
fig3 = mq.plot_associated_timeseries_data_analysis()
fig4 = mq.plot_class_consistency_analysis()

app = Dash()
# app.layout = html.Div([dcc.Graph(figure=fig)])


app.layout = html.Div(
    [
        dcc.Tabs(
            [
                dcc.Tab(label="Tab one", children=[dcc.Graph(figure=fig1)]),
                dcc.Tab(label="Tab two", children=[dcc.Graph(figure=fig2)]),
                dcc.Tab(label="Tab three", children=[dcc.Graph(figure=fig3)]),
                dcc.Tab(label="Tab four", children=[dcc.Graph(figure=fig4)]),
            ]
        )
    ]
)


app.run_server(debug=True, use_reloader=True)  # Turn off reloader if inside Jupyter
