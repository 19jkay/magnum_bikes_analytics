import pandas as pd
import os
import webbrowser
from datetime import datetime

from DASH.DASH_Helper import DASH_Helper_get_product_info


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

def write_consensus_report_old(df, product_name, path, selected_month, selected_year):
    df.set_index('Metric', inplace=True)
    df = df.T
    df.index.name = 'Year-Month'
    df.reset_index(inplace=True)
    df = df[['Year-Month', 'Final Consensus']]

    product_guid, avg_cost = DASH_Helper_get_product_info(product_name)
    df['Final Consensus'] = (avg_cost * df['Final Consensus']).round(2)
    product_name = product_name + " ($)"


    extension = "xlsx"
    file_name = 'Forecast_Summary'
    target_period = f"{selected_year}-{selected_month}"
    print("Saving path: ", path)
    output_dir = os.path.join(r"C:\Users\joshu\Documents\DASH", path)
    os.makedirs(output_dir, exist_ok=True)  # Ensure directory exists
    filename = f"{file_name}_{target_period}.{extension}"
    full_path = os.path.join(output_dir, filename)




    row1 = ['Year-Month'] + df['Year-Month'].tolist()
    row2 = [product_name] + df['Final Consensus'].tolist()
    row3 = [product_name] + df['Purchases'].tolist()
    new_data = pd.DataFrame([row1, row2, row3])
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




def write_consensus_report(df, product_name, path, selected_month, selected_year):
    # df.set_index('Metric', inplace=True)
    # df = df.T
    # df.index.name = 'Year-Month'
    # df.reset_index(inplace=True)

    product_guid, avg_cost = DASH_Helper_get_product_info(product_name)
    # df['Final Consensus'] = (avg_cost * df['Final Consensus']).round(2)
    # product_name = product_name + " ($)"


    extension = "xlsx"
    file_name = 'Forecast_Summary'
    target_period = f"{selected_year}-{selected_month}"
    print("Saving path: ", path)
    output_dir = os.path.join(r"C:\Users\joshu\Documents\DASH", path)
    os.makedirs(output_dir, exist_ok=True)  # Ensure directory exists
    filename = f"{file_name}_{target_period}.{extension}"
    full_path = os.path.join(output_dir, filename)

    # # Second folder that saves everything
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamp_month = datetime.now().strftime("%Y%m")
    filename_folder_save = 'Umbrella_Save_'+timestamp_month

    timestamped_dir = os.path.join(r"C:\Users\joshu\Documents\DASH", filename_folder_save)
    os.makedirs(timestamped_dir, exist_ok=True)

    safe_product_name = product_name.replace("/", "_").replace(":", "_")

    filename_umbrella_save = f"{safe_product_name}_{timestamp}.{extension}"
    timestamped_path = os.path.join(timestamped_dir, filename_umbrella_save)
    # print("Product Name: ", safe_product_name)
    # print("Umbrella save path:", timestamped_path)
    df.to_excel(timestamped_path, index=False, header=True)

    df.set_index('Metric', inplace=True)
    df = df.T
    df.index.name = 'Year-Month'
    df.reset_index(inplace=True)

    df = df[['Year-Month', 'Final Consensus', 'Purchases']]


    row1 = ['Year-Month'] + df['Year-Month'].tolist()
    row2 = [product_name + ' Final Consensus'] + df['Final Consensus'].tolist()
    row3 = [product_name + ' Purchases'] + df['Purchases'].tolist()
    new_data = pd.DataFrame([row1, row2, row3])

    product_metric_one = product_name + ' Final Consensus'
    product_metric_two = product_name + ' Purchases'
    new_data.index = ['Year-Month', product_metric_one, product_metric_two]

    if not os.path.exists(full_path):
        # File doesn't exist — create new
        new_data.to_excel(full_path, index=False, header=False)

    else:
        # File exists — load and update
        existing_data = pd.read_excel(full_path, header=None)

        # Check if product_name already exists in first column
        product_rows = existing_data.iloc[:, 0].astype(str)
        lookup = product_name + ' Final Consensus'
        if lookup in product_rows.values:
            # Update existing row
            row_index = product_rows[product_rows == lookup].index[0]
            existing_data.iloc[row_index, 1:] = row2[1:]
            existing_data.iloc[row_index + 1, 1:] = row3[1:]
        else:
            # Append new row
            existing_data = pd.concat([existing_data, pd.DataFrame([row2, row3])], ignore_index=True)

        # Save updated file
        existing_data.to_excel(full_path, index=False, header=False)








def dash_app(data, product_name, path):
    import pandas as pd
    import plotly.graph_objects as go
    from dash import Dash, html, dcc, dash_table, Input, Output, State, ctx
    import matplotlib
    from flask import request
    import os
    import time



    matplotlib.use('Agg')

    # bottom_table_df = pd.DataFrame([['' for _ in range(2)] for _ in range(7)],
    #                                columns=['Metric', 'Value'])

    # Preprocess date
    data['Year-Month'] = pd.to_datetime(data['Year-Month'])
    data['Year-Month'] = data['Year-Month'].dt.to_period('M').dt.to_timestamp().dt.strftime('%Y-%m')

    data['Comments'] = ''

    inventory_name = [col for col in data.columns if col.startswith('Inventory')][0]
    print("in app: ", inventory_name)

    data = update_data(data, inventory_name)

    # metrics = ['Analytical Forecast (Kay)', 'Financial Forecast (Poll)', 'Final Consensus', inventory_name, 'Ending Inventory', 'Purchases']
    metrics = ['Analytical Forecast (Kay)', 'Financial Forecast (Poll)', 'Final Consensus', inventory_name, 'Ending Inventory', 'Purchases', 'Comments']
    data = data[['Year-Month'] + metrics]

    transposed = data.set_index('Year-Month').T
    transposed.reset_index(inplace=True)
    transposed.rename(columns={'index': 'Metric'}, inplace=True)

    start = time.time()
    # app = Dash(__name__)
    app = Dash(__name__, serve_locally=True)
    server = app.server  # Access Flask server
    print("Layout build time:", time.time() - start)

    # Add shutdown route
    @server.route('/shutdown', methods=['POST'])
    def shutdown():
        shutdown_func = request.environ.get('werkzeug.server.shutdown')
        if shutdown_func:
            shutdown_func()
        return 'Server shutting down...'

    # app.layout = html.Div([
    #     html.H1(product_name + " Forecast Dashboard", style={'textAlign': 'center', 'marginBottom': '20px'}),
    #
    #     dash_table.DataTable(
    #         id='editable-table',
    #         data=transposed.to_dict('records'),
    #         columns=[{'name': col, 'id': col, 'editable': True} for col in transposed.columns],
    #         style_table={'overflowX': 'auto'}
    #     ),
    #
    #     html.Label("Select metrics to display:"),
    #     dcc.Checklist(
    #         id='metric-selector',
    #         options=[{'label': m, 'value': m} for m in metrics],
    #         value=['Analytical Forecast (Kay)', 'Financial Forecast (Poll)', 'Final Consensus', inventory_name],
    #         inline=True
    #     ),
    #
    #     dcc.Graph(id='line-chart'),
    #
    #     html.Div([
    #         html.Label("Select Month:"),
    #         dcc.Dropdown(
    #             id='month-dropdown',
    #             options=[{'label': f'{m:02d}', 'value': f'{m:02d}'} for m in range(1, 13)],
    #             placeholder='Select month'
    #         ),
    #         html.Label("Select Year:"),
    #         dcc.Dropdown(
    #             id='year-dropdown',
    #             options=[{'label': str(y), 'value': str(y)} for y in range(2025, 2031)],
    #             placeholder='Select year'
    #         ),
    #         html.Button('Save', id='save-button', n_clicks=0, style={'marginTop': '10px'}),
    #         html.Button("Quit App", id="quit-btn"),
    #         html.Div(id="status")
    #     ], style={'marginTop': '20px'})
    # ])
    #
    # @app.callback(
    #     Output('line-chart', 'figure'),
    #     Output('editable-table', 'data'),
    #     Input('editable-table', 'data'),
    #     Input('editable-table', 'columns'),
    #     Input('metric-selector', 'value')
    # )







    # app.layout = html.Div([
    #     html.H1(product_name + " Forecast Dashboard", style={'textAlign': 'center', 'marginBottom': '20px'}),
    #
    #     dash_table.DataTable(
    #         id='editable-table',
    #         data=transposed.to_dict('records'),
    #         columns=[{'name': col, 'id': col, 'editable': True} for col in transposed.columns],
    #         style_table={'overflowX': 'auto'}
    #     ),
    #
    #
    #     # Graph and bottom table side by side
    #     html.Div([
    #         html.Div([
    #             dcc.Graph(id='line-chart', style={'height': '600px', 'width': '100%'})
    #         ], style={'flex': '3'}),
    #
    #         html.Div([
    #             # html.H4("Editable Table (Bottom Right)", style={'textAlign': 'center'}),
    #             html.H4("Notes", style={'textAlign': 'center'}),
    #             dash_table.DataTable(
    #                 id='bottom-table',
    #                 data=bottom_table_df.to_dict('records'),
    #                 columns=[{'name': col, 'id': col, 'editable': True} for col in bottom_table_df.columns],
    #                 style_table={'width': '300px', 'margin': 'auto'},
    #                 style_cell={'textAlign': 'center'}
    #             )
    #         ], style={'flex': '1', 'paddingLeft': '20px', 'alignSelf': 'flex-start'})
    #     ], style={'display': 'flex', 'marginTop': '30px'}),
    #
    #
    #     html.Label("Select metrics to display:"),
    #     dcc.Checklist(
    #         id='metric-selector',
    #         options=[{'label': m, 'value': m} for m in metrics],
    #         value=['Analytical Forecast (Kay)', 'Financial Forecast (Poll)', 'Final Consensus', inventory_name],
    #         inline=True
    #     ),
    #
    #
    #     html.Div([
    #         html.Label("Select Month:"),
    #         dcc.Dropdown(
    #             id='month-dropdown',
    #             options=[{'label': f'{m:02d}', 'value': f'{m:02d}'} for m in range(1, 13)],
    #             placeholder='Select month'
    #         ),
    #         html.Label("Select Year:"),
    #         dcc.Dropdown(
    #             id='year-dropdown',
    #             options=[{'label': str(y), 'value': str(y)} for y in range(2025, 2031)],
    #             placeholder='Select year'
    #         ),
    #         html.Button('Save', id='save-button', n_clicks=0, style={'marginTop': '10px'}),
    #         html.Button("Quit App", id="quit-btn"),
    #         html.Div(id="status")
    #     ], style={'marginTop': '20px'})
    # ])





    # app.layout = html.Div([
    #     html.H1(product_name + " Forecast Dashboard", style={'textAlign': 'center', 'marginBottom': '20px'}),
    #
    #     # Notes (left) and Editable table (right) side-by-side
    #     html.Div([
    #         html.Div([
    #             html.H4("Notes", style={'textAlign': 'center', 'marginBottom': '8px'}),
    #             dash_table.DataTable(
    #                 id='bottom-table',
    #                 data=bottom_table_df.to_dict('records'),
    #                 columns=[{'name': col, 'id': col, 'editable': True} for col in bottom_table_df.columns],
    #                 style_table={'width': '320px', 'margin': '0'},
    #                 style_cell={'textAlign': 'left', 'whiteSpace': 'normal', 'height': 'auto'},
    #                 tooltip_header={col: 'Notes / comments' for col in bottom_table_df.columns}
    #             )
    #         ], style={'flex': '1', 'paddingRight': '16px', 'minWidth': '260px'}),  # left column
    #
    #         html.Div([
    #             dash_table.DataTable(
    #                 id='editable-table',
    #                 data=transposed.to_dict('records'),
    #                 columns=[{'name': col, 'id': col, 'editable': True} for col in transposed.columns],
    #                 style_table={'overflowX': 'auto'},
    #                 style_cell={'textAlign': 'center', 'minWidth': '80px'},
    #             )
    #         ], style={'flex': '3', 'minWidth': '400px'})  # right column (editable table)
    #     ], style={'display': 'flex', 'alignItems': 'flex-start', 'marginTop': '10px'}),
    #
    #     # Graph and Notes (bottom-right) side by side
    #     html.Div([
    #         html.Div([
    #             dcc.Graph(id='line-chart', style={'height': '600px', 'width': '100%'})
    #         ], style={'flex': '3'}),
    #     ], style={'display': 'flex', 'marginTop': '30px'}),
    #
    #     html.Label("Select metrics to display:"),
    #     dcc.Checklist(
    #         id='metric-selector',
    #         options=[{'label': m, 'value': m} for m in metrics],
    #         value=['Analytical Forecast (Kay)', 'Financial Forecast (Poll)', 'Final Consensus', inventory_name],
    #         inline=True
    #     ),
    #
    #     html.Div([
    #         html.Label("Select Month:"),
    #         dcc.Dropdown(
    #             id='month-dropdown',
    #             options=[{'label': f'{m:02d}', 'value': f'{m:02d}'} for m in range(1, 13)],
    #             placeholder='Select month'
    #         ),
    #         html.Label("Select Year:"),
    #         dcc.Dropdown(
    #             id='year-dropdown',
    #             options=[{'label': str(y), 'value': str(y)} for y in range(2025, 2031)],
    #             placeholder='Select year'
    #         ),
    #         html.Button('Save', id='save-button', n_clicks=0, style={'marginTop': '10px'}),
    #         html.Button("Quit App", id="quit-btn"),
    #         html.Div(id="status")
    #     ], style={'marginTop': '20px'})
    # ])

    app.layout = html.Div([
        html.H1(product_name + " Forecast Dashboard", style={'textAlign': 'center', 'marginBottom': '20px'}),

        # Notes (left) and Editable table (right) side-by-side — flush top alignment
        html.Div([
            # Left: Notes column (fixed width)
            # html.Div([
            #     html.Div("KPIs", style={'textAlign': 'center', 'margin': '0 0 6px 0', 'fontWeight': '600'}),
            #     dash_table.DataTable(
            #         id='bottom-table',
            #         data=bottom_table_df.to_dict('records'),
            #         columns=[{'name': col, 'id': col, 'editable': True} for col in bottom_table_df.columns],
            #         style_table={'width': '280px', 'margin': '0'},
            #         style_cell={'textAlign': 'center', 'whiteSpace': 'normal', 'height': 'auto', 'padding': '6px'},
            #         tooltip_header={col: 'Notes / comments' for col in bottom_table_df.columns}
            #     )
            # ], style={'flex': '0 0 300px', 'paddingRight': '8px', 'boxSizing': 'border-box'}),

            # Right: Forecast editable table (flexible width)
            html.Div([
                html.Div(product_name + " Forecast",
                         style={'textAlign': 'center', 'margin': '0 0 6px 0', 'fontWeight': '600'}),
                dash_table.DataTable(
                    id='editable-table',
                    data=transposed.to_dict('records'),
                    columns=[{'name': col, 'id': col, 'editable': True} for col in transposed.columns],
                    style_table={'overflowX': 'auto', 'margin': '0'},
                    style_cell={'textAlign': 'center', 'minWidth': '80px', 'padding': '6px'},
                )
            ], style={'flex': '1 1 0px', 'minWidth': '400px', 'boxSizing': 'border-box'})
        ], style={
            'display': 'flex',
            'alignItems': 'flex-start',  # ensures top edges are aligned
            'gap': '8px',  # small horizontal gap so the two tables are close but not touching
            'marginTop': '10px'
        }),

        # Graph (below)
        html.Div([
            dcc.Graph(id='line-chart', style={'height': '600px', 'width': '100%'})
        ], style={'display': 'flex', 'marginTop': '30px'}),

        html.Label("Select metrics to display:"),
        dcc.Checklist(
            id='metric-selector',
            options=[{'label': m, 'value': m} for m in metrics],
            # value=['Analytical Forecast (Kay)', 'Financial Forecast (Poll)', 'Final Consensus', inventory_name],
            value=['Final Consensus', inventory_name],
            inline=True
        ),

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

        # for col in df.columns[1:]:
        for col in df.columns[1:-1]:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        if all(metric in df.columns for metric in metrics):
            df = update_data(df, inventory_name)

        save_dataframe(df, product_name, path, filename_suffix="consensus_data")

        # save_dataframe(bottom_table_df, product_name, path, filename_suffix='additional_data')

        fig = go.Figure()
        for col in selected_metrics:
            fig.add_trace(go.Scatter(x=df['Year-Month'], y=df[col], mode='lines+markers', name=col))

        # fig.update_layout(title='Metrics Over Time', xaxis_title='Year-Month', yaxis_title='Value')
        fig.update_layout(xaxis_title='Year-Month', yaxis_title='Value')

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












#
# def dash_app(data, product_name, path):
#     import pandas as pd
#     import plotly.graph_objects as go
#     from dash import Dash, html, dcc, dash_table, Input, Output, State, ctx
#     import matplotlib
#     from flask import request
#     import os
#     import time
#
#
#
#     matplotlib.use('Agg')
#
#     bottom_table_df = pd.DataFrame([['' for _ in range(2)] for _ in range(7)],
#                                    columns=['Metric', 'Value'])
#
#     # Preprocess date
#     data['Year-Month'] = pd.to_datetime(data['Year-Month'])
#     data['Year-Month'] = data['Year-Month'].dt.to_period('M').dt.to_timestamp().dt.strftime('%Y-%m')
#
#     data['Comments'] = ''
#
#     inventory_name = [col for col in data.columns if col.startswith('Inventory')][0]
#     print("in app: ", inventory_name)
#
#     data = update_data(data, inventory_name)
#
#     # metrics = ['Analytical Forecast (Kay)', 'Financial Forecast (Poll)', 'Final Consensus', inventory_name, 'Ending Inventory', 'Purchases']
#     metrics = ['Analytical Forecast (Kay)', 'Financial Forecast (Poll)', 'Final Consensus', inventory_name, 'Ending Inventory', 'Purchases', 'Comments']
#     data = data[['Year-Month'] + metrics]
#
#     transposed = data.set_index('Year-Month').T
#     transposed.reset_index(inplace=True)
#     transposed.rename(columns={'index': 'Metric'}, inplace=True)
#
#     start = time.time()
#     # app = Dash(__name__)
#     app = Dash(__name__, serve_locally=True)
#     server = app.server  # Access Flask server
#     print("Layout build time:", time.time() - start)
#
#     # Add shutdown route
#     @server.route('/shutdown', methods=['POST'])
#     def shutdown():
#         shutdown_func = request.environ.get('werkzeug.server.shutdown')
#         if shutdown_func:
#             shutdown_func()
#         return 'Server shutting down...'
#
#     # app.layout = html.Div([
#     #     html.H1(product_name + " Forecast Dashboard", style={'textAlign': 'center', 'marginBottom': '20px'}),
#     #
#     #     dash_table.DataTable(
#     #         id='editable-table',
#     #         data=transposed.to_dict('records'),
#     #         columns=[{'name': col, 'id': col, 'editable': True} for col in transposed.columns],
#     #         style_table={'overflowX': 'auto'}
#     #     ),
#     #
#     #     html.Label("Select metrics to display:"),
#     #     dcc.Checklist(
#     #         id='metric-selector',
#     #         options=[{'label': m, 'value': m} for m in metrics],
#     #         value=['Analytical Forecast (Kay)', 'Financial Forecast (Poll)', 'Final Consensus', inventory_name],
#     #         inline=True
#     #     ),
#     #
#     #     dcc.Graph(id='line-chart'),
#     #
#     #     html.Div([
#     #         html.Label("Select Month:"),
#     #         dcc.Dropdown(
#     #             id='month-dropdown',
#     #             options=[{'label': f'{m:02d}', 'value': f'{m:02d}'} for m in range(1, 13)],
#     #             placeholder='Select month'
#     #         ),
#     #         html.Label("Select Year:"),
#     #         dcc.Dropdown(
#     #             id='year-dropdown',
#     #             options=[{'label': str(y), 'value': str(y)} for y in range(2025, 2031)],
#     #             placeholder='Select year'
#     #         ),
#     #         html.Button('Save', id='save-button', n_clicks=0, style={'marginTop': '10px'}),
#     #         html.Button("Quit App", id="quit-btn"),
#     #         html.Div(id="status")
#     #     ], style={'marginTop': '20px'})
#     # ])
#     #
#     # @app.callback(
#     #     Output('line-chart', 'figure'),
#     #     Output('editable-table', 'data'),
#     #     Input('editable-table', 'data'),
#     #     Input('editable-table', 'columns'),
#     #     Input('metric-selector', 'value')
#     # )
#
#
#
#
#
#
#
#     # app.layout = html.Div([
#     #     html.H1(product_name + " Forecast Dashboard", style={'textAlign': 'center', 'marginBottom': '20px'}),
#     #
#     #     dash_table.DataTable(
#     #         id='editable-table',
#     #         data=transposed.to_dict('records'),
#     #         columns=[{'name': col, 'id': col, 'editable': True} for col in transposed.columns],
#     #         style_table={'overflowX': 'auto'}
#     #     ),
#     #
#     #
#     #     # Graph and bottom table side by side
#     #     html.Div([
#     #         html.Div([
#     #             dcc.Graph(id='line-chart', style={'height': '600px', 'width': '100%'})
#     #         ], style={'flex': '3'}),
#     #
#     #         html.Div([
#     #             # html.H4("Editable Table (Bottom Right)", style={'textAlign': 'center'}),
#     #             html.H4("Notes", style={'textAlign': 'center'}),
#     #             dash_table.DataTable(
#     #                 id='bottom-table',
#     #                 data=bottom_table_df.to_dict('records'),
#     #                 columns=[{'name': col, 'id': col, 'editable': True} for col in bottom_table_df.columns],
#     #                 style_table={'width': '300px', 'margin': 'auto'},
#     #                 style_cell={'textAlign': 'center'}
#     #             )
#     #         ], style={'flex': '1', 'paddingLeft': '20px', 'alignSelf': 'flex-start'})
#     #     ], style={'display': 'flex', 'marginTop': '30px'}),
#     #
#     #
#     #     html.Label("Select metrics to display:"),
#     #     dcc.Checklist(
#     #         id='metric-selector',
#     #         options=[{'label': m, 'value': m} for m in metrics],
#     #         value=['Analytical Forecast (Kay)', 'Financial Forecast (Poll)', 'Final Consensus', inventory_name],
#     #         inline=True
#     #     ),
#     #
#     #
#     #     html.Div([
#     #         html.Label("Select Month:"),
#     #         dcc.Dropdown(
#     #             id='month-dropdown',
#     #             options=[{'label': f'{m:02d}', 'value': f'{m:02d}'} for m in range(1, 13)],
#     #             placeholder='Select month'
#     #         ),
#     #         html.Label("Select Year:"),
#     #         dcc.Dropdown(
#     #             id='year-dropdown',
#     #             options=[{'label': str(y), 'value': str(y)} for y in range(2025, 2031)],
#     #             placeholder='Select year'
#     #         ),
#     #         html.Button('Save', id='save-button', n_clicks=0, style={'marginTop': '10px'}),
#     #         html.Button("Quit App", id="quit-btn"),
#     #         html.Div(id="status")
#     #     ], style={'marginTop': '20px'})
#     # ])
#
#
#
#
#
#     # app.layout = html.Div([
#     #     html.H1(product_name + " Forecast Dashboard", style={'textAlign': 'center', 'marginBottom': '20px'}),
#     #
#     #     # Notes (left) and Editable table (right) side-by-side
#     #     html.Div([
#     #         html.Div([
#     #             html.H4("Notes", style={'textAlign': 'center', 'marginBottom': '8px'}),
#     #             dash_table.DataTable(
#     #                 id='bottom-table',
#     #                 data=bottom_table_df.to_dict('records'),
#     #                 columns=[{'name': col, 'id': col, 'editable': True} for col in bottom_table_df.columns],
#     #                 style_table={'width': '320px', 'margin': '0'},
#     #                 style_cell={'textAlign': 'left', 'whiteSpace': 'normal', 'height': 'auto'},
#     #                 tooltip_header={col: 'Notes / comments' for col in bottom_table_df.columns}
#     #             )
#     #         ], style={'flex': '1', 'paddingRight': '16px', 'minWidth': '260px'}),  # left column
#     #
#     #         html.Div([
#     #             dash_table.DataTable(
#     #                 id='editable-table',
#     #                 data=transposed.to_dict('records'),
#     #                 columns=[{'name': col, 'id': col, 'editable': True} for col in transposed.columns],
#     #                 style_table={'overflowX': 'auto'},
#     #                 style_cell={'textAlign': 'center', 'minWidth': '80px'},
#     #             )
#     #         ], style={'flex': '3', 'minWidth': '400px'})  # right column (editable table)
#     #     ], style={'display': 'flex', 'alignItems': 'flex-start', 'marginTop': '10px'}),
#     #
#     #     # Graph and Notes (bottom-right) side by side
#     #     html.Div([
#     #         html.Div([
#     #             dcc.Graph(id='line-chart', style={'height': '600px', 'width': '100%'})
#     #         ], style={'flex': '3'}),
#     #     ], style={'display': 'flex', 'marginTop': '30px'}),
#     #
#     #     html.Label("Select metrics to display:"),
#     #     dcc.Checklist(
#     #         id='metric-selector',
#     #         options=[{'label': m, 'value': m} for m in metrics],
#     #         value=['Analytical Forecast (Kay)', 'Financial Forecast (Poll)', 'Final Consensus', inventory_name],
#     #         inline=True
#     #     ),
#     #
#     #     html.Div([
#     #         html.Label("Select Month:"),
#     #         dcc.Dropdown(
#     #             id='month-dropdown',
#     #             options=[{'label': f'{m:02d}', 'value': f'{m:02d}'} for m in range(1, 13)],
#     #             placeholder='Select month'
#     #         ),
#     #         html.Label("Select Year:"),
#     #         dcc.Dropdown(
#     #             id='year-dropdown',
#     #             options=[{'label': str(y), 'value': str(y)} for y in range(2025, 2031)],
#     #             placeholder='Select year'
#     #         ),
#     #         html.Button('Save', id='save-button', n_clicks=0, style={'marginTop': '10px'}),
#     #         html.Button("Quit App", id="quit-btn"),
#     #         html.Div(id="status")
#     #     ], style={'marginTop': '20px'})
#     # ])
#
#     app.layout = html.Div([
#         html.H1(product_name + " Forecast Dashboard", style={'textAlign': 'center', 'marginBottom': '20px'}),
#
#         # Notes (left) and Editable table (right) side-by-side — flush top alignment
#         html.Div([
#             # Left: Notes column (fixed width)
#             html.Div([
#                 html.Div("KPIs", style={'textAlign': 'center', 'margin': '0 0 6px 0', 'fontWeight': '600'}),
#                 dash_table.DataTable(
#                     id='bottom-table',
#                     data=bottom_table_df.to_dict('records'),
#                     columns=[{'name': col, 'id': col, 'editable': True} for col in bottom_table_df.columns],
#                     style_table={'width': '280px', 'margin': '0'},
#                     style_cell={'textAlign': 'center', 'whiteSpace': 'normal', 'height': 'auto', 'padding': '6px'},
#                     tooltip_header={col: 'Notes / comments' for col in bottom_table_df.columns}
#                 )
#             ], style={'flex': '0 0 300px', 'paddingRight': '8px', 'boxSizing': 'border-box'}),
#
#             # Right: Forecast editable table (flexible width)
#             html.Div([
#                 html.Div(product_name + " Forecast",
#                          style={'textAlign': 'center', 'margin': '0 0 6px 0', 'fontWeight': '600'}),
#                 dash_table.DataTable(
#                     id='editable-table',
#                     data=transposed.to_dict('records'),
#                     columns=[{'name': col, 'id': col, 'editable': True} for col in transposed.columns],
#                     style_table={'overflowX': 'auto', 'margin': '0'},
#                     style_cell={'textAlign': 'center', 'minWidth': '80px', 'padding': '6px'},
#                 )
#             ], style={'flex': '1 1 0px', 'minWidth': '400px', 'boxSizing': 'border-box'})
#         ], style={
#             'display': 'flex',
#             'alignItems': 'flex-start',  # ensures top edges are aligned
#             'gap': '8px',  # small horizontal gap so the two tables are close but not touching
#             'marginTop': '10px'
#         }),
#
#         # Graph (below)
#         html.Div([
#             dcc.Graph(id='line-chart', style={'height': '600px', 'width': '100%'})
#         ], style={'display': 'flex', 'marginTop': '30px'}),
#
#         html.Label("Select metrics to display:"),
#         dcc.Checklist(
#             id='metric-selector',
#             options=[{'label': m, 'value': m} for m in metrics],
#             # value=['Analytical Forecast (Kay)', 'Financial Forecast (Poll)', 'Final Consensus', inventory_name],
#             value=['Final Consensus', inventory_name],
#             inline=True
#         ),
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
#                 options=[{'label': str(y), 'value': str(y)} for y in range(2025, 2031)],
#                 placeholder='Select year'
#             ),
#             html.Button('Save', id='save-button', n_clicks=0, style={'marginTop': '10px'}),
#             html.Button("Quit App", id="quit-btn"),
#             html.Div(id="status")
#         ], style={'marginTop': '20px'})
#     ])
#
#
#
#
#
#     @app.callback(
#         Output('line-chart', 'figure'),
#         Output('editable-table', 'data'),
#         Input('editable-table', 'data'),
#         Input('editable-table', 'columns'),
#         Input('metric-selector', 'value')
#     )
#
#     def update_graph_and_table(rows, columns, selected_metrics):
#         df = pd.DataFrame(rows)
#         df.set_index('Metric', inplace=True)
#         df = df.T
#         df.index.name = 'Year-Month'
#         df.reset_index(inplace=True)
#
#         # for col in df.columns[1:]:
#         for col in df.columns[1:-1]:
#             df[col] = pd.to_numeric(df[col], errors='coerce')
#
#         if all(metric in df.columns for metric in metrics):
#             df = update_data(df, inventory_name)
#
#         save_dataframe(df, product_name, path, filename_suffix="consensus_data")
#
#         save_dataframe(bottom_table_df, product_name, path, filename_suffix='additional_data')
#
#         fig = go.Figure()
#         for col in selected_metrics:
#             fig.add_trace(go.Scatter(x=df['Year-Month'], y=df[col], mode='lines+markers', name=col))
#
#         # fig.update_layout(title='Metrics Over Time', xaxis_title='Year-Month', yaxis_title='Value')
#         fig.update_layout(xaxis_title='Year-Month', yaxis_title='Value')
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
#
#     @app.callback(
#         Output("status", "children"),
#         Input("quit-btn", "n_clicks"),
#         prevent_initial_call=True
#     )
#     def shutdown(n_clicks):
#         os._exit(0)
#
#     webbrowser.open("http://127.0.0.1:8050")
#     app.run(debug=False, use_reloader=False)













#
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









