import streamlit as st
import pandas as pd
import re
import pyodbc
def modify_column_names(df):
  """Modifies column names based on specific prefixes."""
  new_columns = []
  for col in df.columns:
    if col.startswith('Training record'):
      new_col = 'Training record - ' + col
    elif col.startswith('Training'):
      new_col = 'Training - ' + col
    else:
      new_col = 'User - ' + col
    new_columns.append(new_col)
  df.columns = new_columns

def remove_special_characters(df):
  """Removes special characters from the 'User - User full name' column."""
  df['User - User full name'] = df['User - User full name'].astype(str).apply(
      lambda x: re.sub(r'[^\w\s]', '', x).strip().upper())
  return df

def filter_data(df):
  """Filters data based on division and training record status."""
  df['User - Division'] = df['User - Division'].astype(str).str.lower()
  df['Training record - Training record status'] = df['Training record - Training record status'].astype(str).str.lower()
  return df[(df['User - Division'] == 'indec') & ~(df['Training record - Training record status'].astype(str).str.startswith('completed'))]

def process_dataframe(df):
  """Applies cleaning functions to a DataFrame."""
  df = remove_special_characters(df)
  return df

def handle_missing_cadre(df):
  """Drops rows with missing 'User - Cadre Indicator' (optional)."""
  if 'User - Cadre Indicator' in df.columns:
    df = df.dropna(subset=['User - Cadre Indicator'])
  return df

def stack_dataframes(dataframes):
  """Filters and stacks multiple DataFrames."""
  filtered_dataframes = {}
  for i, df in enumerate(dataframes):
    filtered_df = filter_data(df.copy())  # Filter on a copy
    filtered_dataframes[f"filtered_df_{i+1}"] = filtered_df
  stacked_df = pd.concat(filtered_dataframes.values(), ignore_index=True)
  return stacked_df

def process_stacked_df(stacked_df, table_header_style, table_cell_style):
  """Creates a nested table structure for processed data."""
  grouped_df = stacked_df.groupby('User - User ID')
  result_dfs = []
  for _, group in grouped_df:
    if len(group) > 1:
      # Multiple records for the same user
      training_titles = group['Training - Training title'].tolist()
      training_statuses = group['Training record - Training record status'].tolist()

      # Create nested table structure with SGID and Employee Name (first occurrence)
      nested_table = f"""
<table style="border-collapse: collapse; border-spacing: 0px; margin: 10px; border: 1px solid #333;">
<thead>
<tr>
<th style="{table_header_style}">SGID</th>
<th style="{table_header_style}">Employee Name</th>
<th style="{table_header_style}">Course</th>
<th style="{table_header_style}">Status</th>
</tr>
</thead>
<tbody>
<tr>
<td rowspan="{len(training_titles) }"style="{table_cell_style}">{group.iloc[0]['User - User ID']}</td>
<td rowspan="{len(training_titles) }"style="{table_cell_style}">{group.iloc[0]['User - User full name']}</td>
<td style="{table_cell_style}">{training_titles[0]}</td>
<td style="{table_cell_style}">{training_statuses[0]}</td>
</tr>

{''.join([f'<td style="{table_cell_style}">{title}</td><td style="{table_cell_style}">{status}</td></tr>' for title, status in zip(training_titles[1:], training_statuses[1:])])}
</tbody>
</table>

"""

      result_df = group.iloc[0].to_frame().T
      result_df['Training Details'] = nested_table

    else:
      # Single record for the user
      result_df = group.copy()  # Make a copy to avoid modifying the original DataFrame
      result_df['Training Details'] = f"""
<table style="border-collapse: collapse; border-spacing: 0px; margin: 10px; border: 1px solid #333;">
<thead>
<tr>
<th style="{table_header_style}">SGID</th>
<th style="{table_header_style}">Employee Name</th>
<th style="{table_header_style}">Course</th>
<th style="{table_header_style}">Status</th>
</tr>
</thead>
<tbody>
<tr>
<td style="{table_cell_style}">{result_df.iloc[0]['User - User ID']}</td>
<td style="{table_cell_style}">{result_df.iloc[0]['User - User full name']}</td>
<td style="{table_cell_style}">{result_df.iloc[0]['Training - Training title']}</td>
<td style="{table_cell_style}">{result_df.iloc[0]['Training record - Training record status']}</td>
</tr>
</tbody>
</table>

"""

    result_dfs.append(result_df)

  final_df = pd.concat(result_dfs, ignore_index=True)
  return final_df

def send_email_using_stored_procedure(processed_df, server, database, username, password):
  # Connect to MS SQL Server
  conn = None
  try:
    conn_str = (
        f"Driver={{SQL Server}};"
        f"Server={server};"
        f"Database={database};"
        f"UID={username};"
        f"PWD={password};"
    )
    conn = pyodbc.connect(conn_str)
  except pyodbc.Error as ex:
    print(f"Error connecting to MS SQL Server: {ex}")
    return

  # Iterate through processed DataFrame
  for index, row in processed_df.tail(10).iterrows():
    # Extract data from the row
    user_id = row['User - User ID']
    emp_name = row['User - User full name']
    email = row['User - User e-mail']
    training_details = row['Training Details']
    recipent='sanika.rane@saint-gobain.com'

    # Construct email body with formatted table
    email_body = f"""
<b>THIS IS A TEST EMAIL</b><br><br>

Dear Team,<br><br> Just a quick reminder to complete your Boost Training sessions at earliest convenience.<br>Link and pathway is as below:<br> https://saint-gobain.csod.com/client/saint-gobain/default.aspx <br>If you encounter any difficulties or have questions, please don't hesitate to reach out to us.<br>Ignore if already completed!!<br><br>


{training_details}

"""

    # Call stored procedure to send email
    try:
      cursor = conn.cursor()
      cursor.execute(f"EXEC Boost_Training ?, ?", (recipent, email_body))
      conn.commit()
      print(f"Email sent to {recipent}")
    except pyodbc.Error as ex:
      print(f"Error calling stored procedure: {ex}")

  # Close connection
  if conn:
    conn.close()

def main():
  st.title("Multiple Excel File Processor")

  # Upload multiple Excel files
  uploaded_files = st.file_uploader("Upload multiple Excel files", accept_multiple_files=True)

  if uploaded_files:
    dataframes = []
    for uploaded_file in uploaded_files:
      df = pd.read_excel(uploaded_file, skiprows=13)
      dataframes.append(df)

    # Process DataFrames, stack, and process further
    stacked_df = stack_dataframes([process_dataframe(df) for df in dataframes])
    processed_df = process_stacked_df(
        stacked_df,
        table_header_style="background-color: #f2f2f2; text-align: center; padding: 10px; border: 2px solid #333; width: 150px;",
        table_cell_style="text-align: center; padding: 10px; border: 2px solid #333;"
    )

    # Send emails using the improved function
    send_email_using_stored_procedure(processed_df, '10.87.10.91', 'Boost_training', 'Boost_training', 'Boost_training')

if __name__ == '__main__':
  main()