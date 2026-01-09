import dash
from dash import html, dcc, Output, Input, dash_table
from dash.dependencies import State
import subprocess
import re
import queue
import threading
import pandas as pd
import os
from pathlib import Path

# Initialize Dash app
app = dash.Dash(__name__, 
                external_stylesheets=["https://cdnjs.cloudflare.com/ajax/libs/normalize/8.0.1/normalize.min.css"],
                suppress_callback_exceptions=True)
app.title = "SQLite to MongoDB Migration Tool"

# Global queue for output
output_queue = queue.Queue()
current_process = None

# Paths for CSV results
SQL_RESULTS_DIR = "requetes_sql/resultat_requetes_sql"
MONGODB_RESULTS_DIR = "requetes_mongodb/resultat_requetes_mongodb"

# Layout of the Dash app
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div([
        html.Button("🏠 Home", id="nav-home", n_clicks=0, style={
            "marginRight": "10px",
            "padding": "10px 20px",
            "backgroundColor": "#6C757D",
            "color": "white",
            "border": "none",
            "borderRadius": "5px",
            "cursor": "pointer",
            "fontSize": "16px"
        }),
        html.Button("📊 View Results", id="nav-results", n_clicks=0, style={
            "marginRight": "10px",
            "padding": "10px 20px",
            "backgroundColor": "#17A2B8",
            "color": "white",
            "border": "none",
            "borderRadius": "5px",
            "cursor": "pointer",
            "fontSize": "16px"
        }),
        html.Button("📈 Dashboard", id="nav-dashboard", n_clicks=0, style={
            "padding": "10px 20px",
            "backgroundColor": "#28A745",
            "color": "white",
            "border": "none",
            "borderRadius": "5px",
            "cursor": "pointer",
            "fontSize": "16px"
        })
    ], style={
        "textAlign": "center",
        "marginBottom": "20px",
        "padding": "10px",
        "backgroundColor": "#f0f0f0"
    }),
    html.Div(id='page-content')
])

# Home page layout
home_layout = html.Div([
    html.H1("SQLite to MongoDB Migration Tool", style={
        "textAlign": "center",
        "color": "#333",
        "fontFamily": "Arial, sans-serif",
        "marginBottom": "20px"
    }),
    html.Div([
        html.Button("Run Migration", id="btn-migration", n_clicks=0, style={
            "marginRight": "10px",
            "padding": "10px 20px",
            "backgroundColor": "#007BFF",
            "color": "white",
            "border": "none",
            "borderRadius": "5px",
            "cursor": "pointer",
            "fontSize": "16px"
        }),
        html.Button("Run SQL Queries", id="btn-queries", n_clicks=0, style={
            "marginRight": "10px",
            "padding": "10px 20px",
            "backgroundColor": "#28A745",
            "color": "white",
            "border": "none",
            "borderRadius": "5px",
            "cursor": "pointer",
            "fontSize": "16px"
        }),
        html.Button("Run MongoDB Queries", id="btn-mongodb-queries", n_clicks=0, style={
            "padding": "10px 20px",
            "backgroundColor": "#FFC107",
            "color": "white",
            "border": "none",
            "borderRadius": "5px",
            "cursor": "pointer",
            "fontSize": "16px"
        })
    ], style={
        "textAlign": "center",
        "marginBottom": "20px"
    }),
    dcc.Interval(id="interval", interval=1000, n_intervals=0),
    dcc.Store(id="store-terminal-output", data=[]),
    html.Div(id="terminal-output", style={
        "whiteSpace": "pre-wrap",
        "backgroundColor": "#f9f9f9",
        "padding": "15px",
        "border": "1px solid #ddd",
        "borderRadius": "5px",
        "height": "400px",
        "overflowY": "scroll",
        "fontFamily": "Courier New, monospace",
        "fontSize": "14px",
        "boxShadow": "0 2px 5px rgba(0, 0, 0, 0.1)"
    })
])

# Results page layout
results_layout = html.Div([
    html.H1("Query Results Viewer", style={
        "textAlign": "center",
        "color": "#333",
        "fontFamily": "Arial, sans-serif",
        "marginBottom": "20px"
    }),
    html.Div([
        html.Div([
            html.H3("SQL Query Results", style={"color": "#28A745"}),
            html.Div(id="sql-file-list", style={
                "maxHeight": "300px",
                "overflowY": "auto",
                "border": "1px solid #ddd",
                "padding": "10px",
                "borderRadius": "5px"
            })
        ], style={"width": "48%", "display": "inline-block", "verticalAlign": "top"}),
        html.Div([
            html.H3("MongoDB Query Results", style={"color": "#FFC107"}),
            html.Div(id="mongodb-file-list", style={
                "maxHeight": "300px",
                "overflowY": "auto",
                "border": "1px solid #ddd",
                "padding": "10px",
                "borderRadius": "5px"
            })
        ], style={"width": "48%", "display": "inline-block", "verticalAlign": "top", "marginLeft": "4%"})
    ]),
    html.Hr(),
    html.Div(id="selected-file-info", style={
        "textAlign": "center",
        "fontSize": "18px",
        "fontWeight": "bold",
        "marginBottom": "10px",
        "color": "#007BFF"
    }),
    html.Div(id="csv-viewer", style={
        "marginTop": "20px"
    }),
    dcc.Store(id="selected-csv-path")
])

# Navigation callbacks
@app.callback(
    Output('url', 'pathname'),
    [Input('nav-home', 'n_clicks'),
     Input('nav-results', 'n_clicks'),
     Input('nav-dashboard', 'n_clicks')],
    prevent_initial_call=True
)
def navigate(home_clicks, results_clicks, dashboard_clicks):
    ctx = dash.callback_context
    if not ctx.triggered:
        return '/'
    
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    if button_id == 'nav-home':
        return '/'
    elif button_id == 'nav-results':
        return '/results'
    elif button_id == 'nav-dashboard':
        return '/dashboard'
    
    return '/'

@app.callback(
    Output('page-content', 'children'),
    Input('url', 'pathname')
)
def display_page(pathname):
    if pathname == '/results':
        return results_layout
    elif pathname == '/dashboard':
        return html.Div([
            html.H1("Dashboard", style={"textAlign": "center", "marginBottom": "20px"}),
            html.Iframe(
                src="http://127.0.0.1:8051",  # Port du dashboard séparé
                style={"width": "100%", "height": "800px", "border": "none"}
            )
        ])
    else:
        return home_layout

# Function to run a script and capture its output
def run_script(script_path):
    global current_process
    try:
        current_process = subprocess.Popen(
            ["python", script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )
        
        # Read stdout
        for line in current_process.stdout:
            output_queue.put(line.rstrip('\n'))
        
        # Read stderr
        for line in current_process.stderr:
            if not re.match(r"^\s*\d+%|^\s*\|", line):
                output_queue.put(f"ERROR: {line.rstrip()}")
        
        current_process.wait()
        output_queue.put(f"\n✅ Finished running {script_path}\n")
    except Exception as e:
        output_queue.put(f"\n❌ Error: {str(e)}\n")
    finally:
        current_process = None

# Callback to handle button clicks
@app.callback(
    Output("store-terminal-output", "data"),
    [Input("btn-migration", "n_clicks"),
     Input("btn-queries", "n_clicks"),
     Input("btn-mongodb-queries", "n_clicks"),
     Input("interval", "n_intervals")],
    [State("store-terminal-output", "data")]
)
def handle_updates(btn_migration_clicks, btn_queries_clicks, btn_mongodb_clicks, n_intervals, current_output):
    ctx = dash.callback_context
    if not ctx.triggered:
        return current_output
    
    triggered_id = ctx.triggered[0]["prop_id"].split(".")[0]
    
    # Handle button clicks - start new process
    if triggered_id in ["btn-migration", "btn-queries", "btn-mongodb-queries"]:
        # Clear queue and output
        while not output_queue.empty():
            output_queue.get()
        
        script_map = {
            "btn-migration": "migration/migration.py",
            "btn-queries": "requetes_sql/requete_sql.py",
            "btn-mongodb-queries": "requetes_mongodb/requete_mongo.py"
        }
        
        script_path = script_map.get(triggered_id)
        if script_path:
            output_queue.put(f"🚀 Starting {script_path}...\n")
            threading.Thread(target=run_script, args=(script_path,), daemon=True).start()
            return [f"🚀 Starting {script_path}...\n"]
    
    # Handle interval updates - read from queue
    elif triggered_id == "interval":
        new_output = current_output.copy() if current_output else []
        
        # Read all available output from queue
        while not output_queue.empty():
            try:
                line = output_queue.get_nowait()
                new_output.append(line)
            except queue.Empty:
                break
        
        return new_output
    
    return current_output

# Callback to display terminal output
@app.callback(
    Output("terminal-output", "children"),
    [Input("store-terminal-output", "data")]
)
def update_terminal_output(data):
    return "\n".join(data)

# Function to get CSV files from directory
def get_csv_files(directory):
    if not os.path.exists(directory):
        return []
    return sorted([f for f in os.listdir(directory) if f.endswith('.csv')])

# Callback to populate file lists
@app.callback(
    [Output('sql-file-list', 'children'),
     Output('mongodb-file-list', 'children')],
    Input('url', 'pathname')
)
def populate_file_lists(pathname):
    if pathname != '/results':
        return [], []
    
    sql_files = get_csv_files(SQL_RESULTS_DIR)
    mongodb_files = get_csv_files(MONGODB_RESULTS_DIR)
    
    sql_buttons = [
        html.Button(
            f"📄 {file}",
            id={'type': 'sql-file-btn', 'index': file},
            n_clicks=0,
            style={
                "width": "100%",
                "marginBottom": "5px",
                "padding": "8px",
                "backgroundColor": "#28A745",
                "color": "white",
                "border": "none",
                "borderRadius": "3px",
                "cursor": "pointer",
                "textAlign": "left"
            }
        ) for file in sql_files
    ]
    
    mongodb_buttons = [
        html.Button(
            f"📄 {file}",
            id={'type': 'mongodb-file-btn', 'index': file},
            n_clicks=0,
            style={
                "width": "100%",
                "marginBottom": "5px",
                "padding": "8px",
                "backgroundColor": "#FFC107",
                "color": "white",
                "border": "none",
                "borderRadius": "3px",
                "cursor": "pointer",
                "textAlign": "left"
            }
        ) for file in mongodb_files
    ]
    
    return (sql_buttons if sql_buttons else html.P("No SQL results found", style={"color": "#999"}),
            mongodb_buttons if mongodb_buttons else html.P("No MongoDB results found", style={"color": "#999"}))

# Callback to handle file selection and display
# Callback to handle file selection and display
@app.callback(
    [Output('csv-viewer', 'children'),
     Output('selected-file-info', 'children'),
     Output('selected-csv-path', 'data')],
    [Input({'type': 'sql-file-btn', 'index': dash.dependencies.ALL}, 'n_clicks'),
     Input({'type': 'mongodb-file-btn', 'index': dash.dependencies.ALL}, 'n_clicks')],
    prevent_initial_call=True
)
def display_csv(sql_clicks, mongodb_clicks):
    ctx = dash.callback_context
    
    # Si rien n'a déclenché le callback ou si triggered_id est vide
    if not ctx.triggered or not ctx.triggered_id:
        return html.P("Select a file to view", style={"textAlign": "center", "color": "#999"}), "", None
    
    # Récupération directe de l'ID du bouton cliqué (c'est un dictionnaire)
    triggered_id = ctx.triggered_id
    
    # On vérifie que triggered_id est bien un dictionnaire (cas des pattern matching)
    if not isinstance(triggered_id, dict):
        return dash.no_update

    file_name = triggered_id['index']
    button_type = triggered_id['type']
    
    file_path = None
    source = None
    
    # Détermination du chemin et de la source en fonction du type de bouton
    if button_type == 'sql-file-btn':
        file_path = os.path.join(SQL_RESULTS_DIR, file_name)
        source = "SQL"
    elif button_type == 'mongodb-file-btn':
        file_path = os.path.join(MONGODB_RESULTS_DIR, file_name)
        source = "MongoDB"
    
    # Vérification de l'existence du fichier
    if not file_path or not os.path.exists(file_path):
        return html.P("File not found", style={"textAlign": "center", "color": "red"}), "", None
    
    try:
        df = pd.read_csv(file_path)
        
        table = dash_table.DataTable(
            data=df.to_dict('records'),
            columns=[{'name': col, 'id': col} for col in df.columns],
            style_table={'overflowX': 'auto'},
            style_cell={
                'textAlign': 'left',
                'padding': '10px',
                'fontFamily': 'Arial, sans-serif'
            },
            style_header={
                'backgroundColor': '#007BFF',
                'color': 'white',
                'fontWeight': 'bold'
            },
            style_data={
                'backgroundColor': '#f9f9f9',
                'border': '1px solid #ddd'
            },
            page_size=20
        )
        
        info_text = f"📊 {source} Query: {file_name} ({len(df)} rows)"
        
        return table, info_text, file_path
        
    except Exception as e:
        return html.P(f"Error reading file: {str(e)}", style={"textAlign": "center", "color": "red"}), "", None

# Run the Dash app
if __name__ == "__main__":
    app.run(debug=False, port=8050)  # Main app sur le port 8050
