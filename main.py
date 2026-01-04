import dash
from dash import html, dcc, Output, Input
import subprocess
import threading
import re

# Initialize Dash app
app = dash.Dash(__name__, external_stylesheets=["https://cdnjs.cloudflare.com/ajax/libs/normalize/8.0.1/normalize.min.css"])
app.title = "SQLite to MongoDB Migration Tool"

# Global variable to store terminal output
terminal_output = []

# Function to run a script and capture its output
def run_script(script_path):
    def task():
        process = subprocess.Popen(
            ["python", script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        for line in process.stdout:
            terminal_output.append(line)
        for line in process.stderr:
            # Filter out tqdm progress bars or other non-critical messages
            if not re.match(r"^\s*\d+%|^\s*\|", line):  # Example filter for tqdm
                terminal_output.append(f"ERROR: {line}")
        process.wait()
        terminal_output.append(f"\nFinished running {script_path}\n")

    threading.Thread(target=task).start()

# Layout of the Dash app
app.layout = html.Div([
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
        html.Button("Run Queries", id="btn-queries", n_clicks=0, style={
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
        "marginBottom": "20px"
    }),
    dcc.Interval(id="interval", interval=1000, n_intervals=0),  # Refresh every second
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

# Callback to handle button clicks and update terminal output
@app.callback(
    Output("terminal-output", "children"),
    [Input("btn-migration", "n_clicks"),
     Input("btn-queries", "n_clicks"),
     Input("interval", "n_intervals")]
)
def update_output(btn_migration_clicks, btn_queries_clicks, _):
    ctx = dash.callback_context
    if not ctx.triggered:
        return "\n".join(terminal_output)
    
    triggered_id = ctx.triggered[0]["prop_id"].split(".")[0]
    if triggered_id == "btn-migration":
        run_script("migration/migration.py")
    elif triggered_id == "btn-queries":
        run_script("requetes_sql/requete_sql.py")
    
    return "\n".join(terminal_output)

# Run the Dash app
if __name__ == "__main__":
    app.run(debug=False)
