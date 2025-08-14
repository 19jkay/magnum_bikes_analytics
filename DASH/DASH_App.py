import pandas as pd
from dash import Dash, dash_table, dcc, html, Input, Output
import plotly.graph_objs as go
import re
import os



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

def update_data(data):
    # data['Final Consensus'] = 1 / 2 * (data['Analytical Forecast (Kay)'] + data['Financial Forecast (Poll)'])
    # current inventory is last months Ending Inventory
    for i in range(1, len(data)):
        data.loc[i - 1, 'Ending Inventory'] = data.loc[i - 1, 'Inventory'] - data.loc[i - 1, 'Final Consensus'] + \
                                              data.loc[i - 1, 'Purchases']
        data.loc[i, 'Inventory'] = data.loc[i - 1, 'Ending Inventory']
    # do last column in table
    data.loc[len(data) - 1, 'Ending Inventory'] = data.loc[len(data) - 1, 'Inventory'] - data.loc[
        len(data) - 1, 'Final Consensus'] + data.loc[len(data) - 1, 'Purchases']

    #round to 2 decimal places
    numeric_cols = data.select_dtypes(include='number').columns
    data[numeric_cols] = data[numeric_cols].round(0)

    return data


def dash_app(data, product_name, path):
    import matplotlib
    matplotlib.use('Agg')

    data['Year-Month'] = pd.to_datetime(data['Year-Month'])
    data['Year-Month'] = data['Year-Month'].dt.to_period('M')
    data['Year-Month'] = data['Year-Month'].dt.to_timestamp()
    data['Year-Month'] = data['Year-Month'].dt.strftime('%Y-%m-%d')

    data = update_data(data)

    metrics = ['Analytical Forecast (Kay)', 'Financial Forecast (Poll)', 'Final Consensus', 'Inventory', 'Ending Inventory', 'Purchases']
    data = data[['Year-Month'] + metrics]

    transposed = data.set_index('Year-Month').T
    transposed.reset_index(inplace=True)
    transposed.rename(columns={'index': 'Metric'}, inplace=True)

    app = Dash(__name__)

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
            value=['Analytical Forecast (Kay)', 'Financial Forecast (Poll)', 'Final Consensus', 'Inventory'],
            inline=True
        ),

        dcc.Graph(id='line-chart')
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
            df = update_data(df)

        save_dataframe(df, product_name, path)

        fig = go.Figure()
        for col in selected_metrics:
            fig.add_trace(go.Scatter(x=df['Year-Month'], y=df[col], mode='lines+markers', name=col))

        fig.update_layout(title='Metrics Over Time', xaxis_title='Year-Month', yaxis_title='Value')

        updated_transposed = df.set_index('Year-Month').T
        updated_transposed.reset_index(inplace=True)
        updated_transposed.rename(columns={'index': 'Metric'}, inplace=True)

        return fig, updated_transposed.to_dict('records')

    app.run(debug=False)


# def dash_app(data, product_name):
#     import matplotlib
#     matplotlib.use('Agg')
#
#     # data['Month'] = data['Year-Month'].dt.strftime('%Y-%m-%d')
#     # data.drop(columns=['Year-Month'], inplace=True)
#     data['Year-Month'] = pd.to_datetime(data['Year-Month'])
#     data['Year-Month'] = data['Year-Month'].dt.to_period('M')
#     data['Year-Month'] = data['Year-Month'].dt.to_timestamp()
#     data['Year-Month'] = data['Year-Month'].dt.strftime('%Y-%m-%d')
#
#     #update data once to start
#     data = update_data(data)
#
#     data = data[['Year-Month', 'Analytical Forecast (Kay)', 'Financial Forecast (Poll)', 'Final Consensus', 'Inventory', 'Ending Inventory', 'Purchases']]
#     # for col in data.columns[1:]:
#     #     data[col] = pd.to_numeric(data[col], errors='coerce')
#
#     # Transpose the data
#     transposed = data.set_index('Year-Month').T
#     transposed.reset_index(inplace=True)
#     transposed.rename(columns={'index': 'Metric'}, inplace=True)
#     data=transposed.to_dict('records')
#
#
#     app = Dash(__name__)
#
#
#     # Layout with editable table and graph
#     app.layout = html.Div([
#         html.H1(product_name + " Forecast Dashboard",
#                 style={'textAlign': 'center', 'marginBottom': '20px'}),
#         dash_table.DataTable(
#             id='editable-table',
#             data=transposed.to_dict('records'),
#             columns=[{'name': col, 'id': col, 'editable': True} for col in transposed.columns],
#             style_table={'overflowX': 'auto'}
#         ),
#         dcc.Graph(id='line-chart')
#     ])
#
#
#     # Callback to update the graph and recalculate Final Consensus
#     @app.callback(
#         Output('line-chart', 'figure'),
#         Output('editable-table', 'data'),
#         Input('editable-table', 'data'),
#         Input('editable-table', 'columns')
#     )
#     def update_graph_and_table(rows, columns):
#         df = pd.DataFrame(rows)
#         df.set_index('Metric', inplace=True)
#         df = df.T
#         df.index.name = 'Year-Month'
#         df.reset_index(inplace=True)
#
#         # Convert numeric columns
#         for col in df.columns[1:]:
#             df[col] = pd.to_numeric(df[col], errors='coerce')
#
#         if 'Analytical Forecast (Kay)' in df.columns and 'Financial Forecast (Poll)' in df.columns and 'Final Consensus' in df.columns and 'Inventory' in df.columns and 'Ending Inventory' in df.columns and 'Purchases' in df.columns:
#             df = update_data(df)
#             # df['Final Consensus'] = 0.5 * (
#             #         df['Unit Forecast (Kay)'].fillna(0) + df['Unit Forecast (Poll)'].fillna(0)
#             # )
#
#         #save updated data from interactive plot changes
#         save_dataframe(df, product_name)
#
#         # Create line chart
#         fig = go.Figure()
#         for col in ['Analytical Forecast (Kay)', 'Financial Forecast (Poll)', 'Final Consensus', 'Inventory']:
#             fig.add_trace(go.Scatter(x=df['Year-Month'], y=df[col], mode='lines+markers', name=col))
#         # for col in df.columns[1:]:
#         #     fig.add_trace(go.Scatter(x=df['Month'], y=df[col], mode='lines+markers', name=col))
#
#         fig.update_layout(title='Metrics Over Time', xaxis_title='Year-Month', yaxis_title='Value')
#
#         # Transpose back for table display
#         updated_transposed = df.set_index('Year-Month').T
#         updated_transposed.reset_index(inplace=True)
#         updated_transposed.rename(columns={'index': 'Metric'}, inplace=True)
#
#         return fig, updated_transposed.to_dict('records')
#
#     app.run(debug=False)
