import pandas as pd
# from dash import Dash, dash_table, dcc, html, Input, Output
from dash import Dash, html, dcc, dash_table, Input, Output, State, ctx

import plotly.graph_objs as go
import re
import os
import webbrowser
import threading


from datetime import datetime




def save_dataframe(df, product_name, path, filename_suffix="consensus_data", extension="xlsx"):
    output_dir = os.path.join(r"C:\Users\joshu\Documents\DASH", path)
    os.makedirs(output_dir, exist_ok=True)

    # Clean filename components
    safe_product_name = product_name.replace("/", "_").replace(":", "_")
    filename = f"{safe_product_name}_{filename_suffix}.{extension}"

    # Full save path
    save_path = os.path.join(output_dir, filename)

    # Save DataFrame
    df.to_excel(save_path, index=False)
    print(f"Saved to: {save_path}")

# def update_data(data):
#     # data['Final Consensus'] = 1 / 2 * (data['Analytical Forecast (Kay)'] + data['Financial Forecast (Poll)'])
#     # current inventory is last months Ending Inventory
#     for i in range(1, len(data)):
#         data.loc[i - 1, 'Ending Inventory'] = data.loc[i - 1, 'Inventory'] - data.loc[i - 1, 'Final Consensus'] + \
#                                               data.loc[i - 1, 'Purchases']
#         data.loc[i, 'Inventory'] = data.loc[i - 1, 'Ending Inventory']
#     # do last column in table
#     data.loc[len(data) - 1, 'Ending Inventory'] = data.loc[len(data) - 1, 'Inventory'] - data.loc[
#         len(data) - 1, 'Final Consensus'] + data.loc[len(data) - 1, 'Purchases']
#
#     #round to 2 decimal places
#     numeric_cols = data.select_dtypes(include='number').columns
#     data[numeric_cols] = data[numeric_cols].round(0)
#
#     return data


def update_data(data, inventory_name):
    # data['Final Consensus'] = 1 / 2 * (data['Analytical Forecast (Kay)'] + data['Financial Forecast (Poll)'])
    # current inventory is last months Ending Inventory
    for i in range(1, len(data)):
        data.loc[i - 1, 'Ending Inventory'] = data.loc[i - 1, inventory_name] - data.loc[i - 1, 'Final Consensus'] + \
                                              data.loc[i - 1, 'Purchases']
        data.loc[i, inventory_name] = data.loc[i - 1, 'Ending Inventory']
    # do last column in table
    data.loc[len(data) - 1, 'Ending Inventory'] = data.loc[len(data) - 1, inventory_name] - data.loc[
        len(data) - 1, 'Final Consensus'] + data.loc[len(data) - 1, 'Purchases']

    #round to 2 decimal places
    numeric_cols = data.select_dtypes(include='number').columns
    data[numeric_cols] = data[numeric_cols].round(0)

    return data

def write_consensus_report(df, product_name, path, selected_month, selected_year):
    df.set_index('Metric', inplace=True)
    df = df.T
    df.index.name = 'Year-Month'
    df.reset_index(inplace=True)
    df = df[['Year-Month', 'Final Consensus']]

    extension = "xlsx"
    file_name = 'All_Forecasts'
    target_period = f"{selected_year}-{selected_month}"
    output_dir = os.path.join(r"C:\Users\joshu\Documents\DASH", path)
    os.makedirs(output_dir, exist_ok=True)  # Ensure directory exists
    filename = f"{file_name}_{target_period}.{extension}"
    full_path = os.path.join(output_dir, filename)

    row1 = ['Year-Month'] + df['Year-Month'].tolist()
    row2 = [product_name] + df['Final Consensus'].tolist()
    new_data = pd.DataFrame([row1, row2])
    new_data.index = ['Year-Month', product_name]

    if not os.path.exists(full_path):
        # File doesn't exist — create new
        new_data.to_excel(full_path, index=False, header=False)

    else:
        # File exists — load and update
        existing_data = pd.read_excel(full_path, header=None)

        # Check if product_name already exists in first column
        product_rows = existing_data.iloc[:, 0].astype(str)
        if product_name in product_rows.values:
            # Update existing row
            row_index = product_rows[product_rows == product_name].index[0]
            existing_data.iloc[row_index, 1:] = row2[1:]
        else:
            # Append new row
            existing_data = pd.concat([existing_data, pd.DataFrame([row2])], ignore_index=True)

        # Save updated file
        existing_data.to_excel(full_path, index=False, header=False)



def dash_app(data, product_name, path):
    import pandas as pd
    import plotly.graph_objects as go
    from dash import Dash, html, dcc, dash_table, Input, Output, State, ctx
    import matplotlib
    from multiprocessing import Event
    import sys
    import threading
    import requests
    from flask import request
    import os

    matplotlib.use('Agg')

    # Preprocess date
    data['Year-Month'] = pd.to_datetime(data['Year-Month'])
    data['Year-Month'] = data['Year-Month'].dt.to_period('M').dt.to_timestamp().dt.strftime('%Y-%m')

    inventory_name = [col for col in data.columns if col.startswith('Inventory')][0]
    print("in app: ", inventory_name)

    data = update_data(data, inventory_name)

    metrics = ['Analytical Forecast (Kay)', 'Financial Forecast (Poll)', 'Final Consensus', inventory_name, 'Ending Inventory', 'Purchases']
    data = data[['Year-Month'] + metrics]

    transposed = data.set_index('Year-Month').T
    transposed.reset_index(inplace=True)
    transposed.rename(columns={'index': 'Metric'}, inplace=True)

    app = Dash(__name__)
    server = app.server  # Access Flask server

    # Add shutdown route
    @server.route('/shutdown', methods=['POST'])
    def shutdown():
        shutdown_func = request.environ.get('werkzeug.server.shutdown')
        if shutdown_func:
            shutdown_func()
        return 'Server shutting down...'

    app.layout = html.Div([
        html.H1(product_name + " Forecast Dashboard", style={'textAlign': 'center', 'marginBottom': '20px'}),

        dash_table.DataTable(
            id='editable-table',
            data=transposed.to_dict('records'),
            columns=[{'name': col, 'id': col, 'editable': True} for col in transposed.columns],
            style_table={'overflowX': 'auto'}
        ),

        html.Label("Select metrics to display:"),
        dcc.Checklist(
            id='metric-selector',
            options=[{'label': m, 'value': m} for m in metrics],
            value=['Analytical Forecast (Kay)', 'Financial Forecast (Poll)', 'Final Consensus', inventory_name],
            inline=True
        ),

        dcc.Graph(id='line-chart'),

        html.Div([
            html.Label("Select Month:"),
            dcc.Dropdown(
                id='month-dropdown',
                options=[{'label': f'{m:02d}', 'value': f'{m:02d}'} for m in range(1, 13)],
                placeholder='Select month'
            ),
            html.Label("Select Year:"),
            dcc.Dropdown(
                id='year-dropdown',
                options=[{'label': str(y), 'value': str(y)} for y in range(2025, 2031)],
                placeholder='Select year'
            ),
            html.Button('Save', id='save-button', n_clicks=0, style={'marginTop': '10px'}),
            html.Button("Quit App", id="quit-btn"),
            html.Div(id="status")
        ], style={'marginTop': '20px'})
    ])

    @app.callback(
        Output('line-chart', 'figure'),
        Output('editable-table', 'data'),
        Input('editable-table', 'data'),
        Input('editable-table', 'columns'),
        Input('metric-selector', 'value')
    )
    def update_graph_and_table(rows, columns, selected_metrics):
        df = pd.DataFrame(rows)
        df.set_index('Metric', inplace=True)
        df = df.T
        df.index.name = 'Year-Month'
        df.reset_index(inplace=True)

        for col in df.columns[1:]:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        if all(metric in df.columns for metric in metrics):
            df = update_data(df, inventory_name)

        save_dataframe(df, product_name, path)

        fig = go.Figure()
        for col in selected_metrics:
            fig.add_trace(go.Scatter(x=df['Year-Month'], y=df[col], mode='lines+markers', name=col))

        fig.update_layout(title='Metrics Over Time', xaxis_title='Year-Month', yaxis_title='Value')

        updated_transposed = df.set_index('Year-Month').T
        updated_transposed.reset_index(inplace=True)
        updated_transposed.rename(columns={'index': 'Metric'}, inplace=True)

        return fig, updated_transposed.to_dict('records')

    @app.callback(
        Output('editable-table', 'data', allow_duplicate=True),
        Input('save-button', 'n_clicks'),
        State('editable-table', 'data'),
        State('month-dropdown', 'value'),
        State('year-dropdown', 'value'),
        prevent_initial_call=True
    )
    def handle_save(n_clicks, table_data, selected_month, selected_year):
        if not selected_month or not selected_year:
            raise Dash.exceptions.PreventUpdate

        df = pd.DataFrame(table_data)
        write_consensus_report(df, product_name, path, selected_month, selected_year)

        return table_data


    @app.callback(
        Output("status", "children"),
        Input("quit-btn", "n_clicks"),
        prevent_initial_call=True
    )
    def shutdown(n_clicks):
        os._exit(0)

    webbrowser.open("http://127.0.0.1:8050")
    app.run(debug=False, use_reloader=False)



# def dash_app(data, product_name, path):
#     import pandas as pd
#     import plotly.graph_objects as go
#     from dash import Dash, html, dcc, dash_table, Input, Output, State, ctx
#     import matplotlib
#     matplotlib.use('Agg')
#
#     # Preprocess date
#     data['Year-Month'] = pd.to_datetime(data['Year-Month'])
#     data['Year-Month'] = data['Year-Month'].dt.to_period('M').dt.to_timestamp().dt.strftime('%Y-%m')
#
#     data = update_data(data)
#
#     metrics = ['Analytical Forecast (Kay)', 'Financial Forecast (Poll)', 'Final Consensus', 'Inventory', 'Ending Inventory', 'Purchases']
#     data = data[['Year-Month'] + metrics]
#
#     transposed = data.set_index('Year-Month').T
#     transposed.reset_index(inplace=True)
#     transposed.rename(columns={'index': 'Metric'}, inplace=True)
#
#     app = Dash(__name__)
#
#     app.layout = html.Div([
#         html.H1(product_name + " Forecast Dashboard", style={'textAlign': 'center', 'marginBottom': '20px'}),
#
#         dash_table.DataTable(
#             id='editable-table',
#             data=transposed.to_dict('records'),
#             columns=[{'name': col, 'id': col, 'editable': True} for col in transposed.columns],
#             style_table={'overflowX': 'auto'}
#         ),
#
#         html.Label("Select metrics to display:"),
#         dcc.Checklist(
#             id='metric-selector',
#             options=[{'label': m, 'value': m} for m in metrics],
#             value=['Analytical Forecast (Kay)', 'Financial Forecast (Poll)', 'Final Consensus', 'Inventory'],
#             inline=True
#         ),
#
#         dcc.Graph(id='line-chart'),
#
#         html.Div([
#             html.Label("Select Month:"),
#             dcc.Dropdown(
#                 id='month-dropdown',
#                 options=[{'label': f'{m:02d}', 'value': f'{m:02d}'} for m in range(1, 13)],
#                 placeholder='Select month'
#             ),
#             html.Label("Select Year:"),
#             dcc.Dropdown(
#                 id='year-dropdown',
#                 # options=[{'label': str(y), 'value': str(y)} for y in sorted(data['Year-Month'].str[:4].astype(int).unique())],
#                 options=[{'label': str(y), 'value': str(y)} for y in range(2025, 2031)],
#                 placeholder='Select year'
#             ),
#             html.Button('Save', id='save-button', n_clicks=0, style={'marginTop': '10px'})
#         ], style={'marginTop': '20px'})
#     ])
#
#     @app.callback(
#         Output('line-chart', 'figure'),
#         Output('editable-table', 'data'),
#         Input('editable-table', 'data'),
#         Input('editable-table', 'columns'),
#         Input('metric-selector', 'value')
#     )
#     def update_graph_and_table(rows, columns, selected_metrics):
#         df = pd.DataFrame(rows)
#         df.set_index('Metric', inplace=True)
#         df = df.T
#         df.index.name = 'Year-Month'
#         df.reset_index(inplace=True)
#
#         for col in df.columns[1:]:
#             df[col] = pd.to_numeric(df[col], errors='coerce')
#
#         if all(metric in df.columns for metric in metrics):
#             df = update_data(df)
#
#         save_dataframe(df, product_name, path)
#
#         fig = go.Figure()
#         for col in selected_metrics:
#             fig.add_trace(go.Scatter(x=df['Year-Month'], y=df[col], mode='lines+markers', name=col))
#
#         fig.update_layout(title='Metrics Over Time', xaxis_title='Year-Month', yaxis_title='Value')
#
#         updated_transposed = df.set_index('Year-Month').T
#         updated_transposed.reset_index(inplace=True)
#         updated_transposed.rename(columns={'index': 'Metric'}, inplace=True)
#
#         return fig, updated_transposed.to_dict('records')
#
#     @app.callback(
#         Output('editable-table', 'data', allow_duplicate=True),
#         Input('save-button', 'n_clicks'),
#         State('editable-table', 'data'),
#         State('month-dropdown', 'value'),
#         State('year-dropdown', 'value'),
#         prevent_initial_call=True
#     )
#     def handle_save(n_clicks, table_data, selected_month, selected_year):
#         if not selected_month or not selected_year:
#             raise Dash.exceptions.PreventUpdate
#
#         df = pd.DataFrame(table_data)
#         write_consensus_report(df, product_name, path, selected_month, selected_year)
#
#         return table_data
#
#     # app.run(debug=False)
#     app.run(debug=False, use_reloader=False)









