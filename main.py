"""
library is camlot 

pip install camelot-py[cv]

"""

import uvicorn
from fastapi import FastAPI, File, UploadFile
import camelot, os
import pandas as pd

app = FastAPI()


def process_csv(df):
    # Find rows where the first column name is "Rank"
    rank_rows = df[df.iloc[:, 0] == "Rank"]

    # Scenario 1: Delete rows above the first occurrence of "Rank"
    first_rank_index = rank_rows.index[0]
    if first_rank_index != 0:
        df = df.drop(range(first_rank_index), axis=0)

    # Scenario 2: Delete rows containing only "Rank" and their immediate preceding rows
    for index in rank_rows.index[1:]:
        if index > 0:
            if index - 1 in df.index:  # Check if the row exists before dropping
                df = df.drop(index - 1, axis=0)  # Delete the row above "Rank"
            if index in df.index:  # Check if the row exists before dropping
                df = df.drop(index, axis=0)  # Delete the "Rank" row
    # Reset the index so that the second row becomes the first row
    df = df.reset_index(drop=True)

    # Strip leading and trailing spaces from column names in the first row

    # Make the second row as the new header row
    new_header = df.iloc[0]
    df = df[1:]
    df.columns = new_header
    # Strip leading and trailing spaces, as well as newline characters, from column names
    df.columns = df.columns.str.strip().str.replace('\n', '')
    df.columns = df.columns.str.strip()
    df.columns = [col.strip() for col in df.columns]
    
    # Return the modified DataFrame
    return df



def process_data_frame(df):
    # Read the CSV file into a DataFrame
    tier_list = ['Fantastic', 'Great', 'Fair', 'Poor', 'Coming Soon']

    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        # Check if the value in 'DA Name' column is NaN
        if isinstance(row['da_name'], str):
            # Check if any word from the tier_list is in the 'DA Name' column
            for tier in tier_list:
                if tier in row['da_name']:
                    df.at[index, 'da_name'] = row['da_name'].replace(tier, "").strip()
                    # Split the 'DA Name' and assign the matched tier to 'DA Tier' column
                    df.at[index, 'da_tier'] = tier

    for index, row in df.iterrows():
        if isinstance(row['da_tier'], str):
            for tier in tier_list:
                if tier in row['da_tier']:
                    # Split the text based on the tier name
                    da_name, _, cdf_score = row['da_tier'].partition(tier)

                    df.at[index, 'da_tier'] = tier
                    
                    # Update DA Name only if data exists before tier
                    if da_name.strip():
                        df.at[index, 'da_name'] = da_name.strip()
                    
                    # Update CDF Score only if data exists after tier
                    if cdf_score.strip():
                        df.at[index, 'cdf_score'] = cdf_score.strip()

    for index, row in df.iterrows():
        if isinstance(row['cdf_score'], str):
            text= row['cdf_score'].strip()
            if "%" in text:
                split_text = [part.strip() for part in text.split("%") if part.strip()]
                if len(split_text) > 1:
                    df.at[index, 'cdf_score']= split_text[0].strip()+"%"
                    df.at[index, 'no_feedack']= split_text[1].strip()

            for tier in tier_list:
                if tier in row['cdf_score']:
                    df.at[index, 'da_tier']= tier
                    text= row['cdf_score'].strip()
                    text= text.replace(tier, "").strip()
                    if "%" in text:
                        split_text = [part.strip() for part in text.split("%") if part.strip()]
                        if len(split_text) > 1:
                            df.at[index, 'cdf_score']= split_text[0].strip()+"%"
                            df.at[index, 'no_feedack']= split_text[1].strip()
                            break
        
    return df

#################################################################################################


@app.post("/upload-pdf/")
async def process_pdf(file: UploadFile = File(...)):
    if file.filename.endswith('.pdf'):
        # Define the directory to save the PDF files
        save_directory = "pdfs"
        os.makedirs(save_directory, exist_ok=True)  # Create the directory if it doesn't exist

        # Specify the complete path to save the PDF file
        save_path = os.path.join(save_directory, file.filename)

        # Save the uploaded PDF file to the specified directory
        with open(save_path, "wb") as buffer:
            buffer.write(await file.read())

        # Continue with your PDF processing logic
        abc = camelot.read_pdf(save_path, pages="all")
        dfs = []
        for table in abc:
            dfs.append(table.df)
        combined_df = pd.concat(dfs, ignore_index=True)
        processed_df = process_csv(combined_df)
        # Remove spaces from column names
        processed_df.rename(columns=lambda x: x.lower().replace(' ', '_').replace('-',''), inplace=True)
        more_processed_df = process_data_frame(processed_df)
        json_data = more_processed_df.to_dict(orient='records')
        
        return {"status": True, "filename": file.filename, "data": json_data}
    
    return {'status': False, "message": "Uploaded file is not a PDF"}



#################################################################################################


def process_pod_report(save_path):
    # Extract all the tables in the PDF file
    abc = camelot.read_pdf(save_path, pages="all")  # Replace with your file location

    # Initialize an empty dictionary to store DataFrames
    table_dict = {}

    # Iterate through each table extracted by Camelot
    for index, table in enumerate(abc, start=1):
        # Convert the table to a DataFrame
        df = table.df
        # # Use the second row as column names
        df.columns = df.iloc[0]
        # # # Skip the first row as it contains unnecessary data
        df = df.iloc[1:]
        # # Get the column names of the DataFrame
        columns = tuple(df.columns)
        # Check if a DataFrame with the same columns already exists in the dictionary
        if columns in table_dict:
            # skip columns name 
            df = df.iloc[1:]
            # If it does, append the current DataFrame to the existing one
            table_dict[columns] = pd.concat([table_dict[columns], df], ignore_index=True)
        else:
            # If it doesn't, create a new entry in the dictionary
            table_dict[columns] = df

    all_tables= {}
    for index, (columns, df) in enumerate(table_dict.items(), start=1):
        if all(column == '' for column in columns) and all(column == '' for column in df.columns) and all(all(value == '' for value in row) for _, row in df.iterrows()):
            continue  # Skip this DataFrame if both column names and data are empty

        columns= [col.lower().replace(" ", "_").replace("-", "").replace("%","percent") for col in columns]

        # Check if "Employee Name" and "Transporter Id" are not in the column names
        if "employee_name" not in columns and "transporter_id" not in columns:
            new_columns = []
            for col in columns:
                # Split column names by "\n" and remove empty strings
                col_parts = [part for part in col.split("\n") if part]
                new_columns.extend(col_parts)
            # Ensure the length of the new columns list is the same as the original columns list
            if len(new_columns) == len(columns):
                # Replace the column names in the DataFrame
                df.columns = new_columns
                print(new_columns)
            else:
                print("Unable to process column names due to inconsistency.")

        if "employee_name" in columns and "transporter_id" in columns and "pod_summary" in columns:
            # df.columns= columns
            df.columns = df.iloc[0]
            # # # Skip the first row as it contains unnecessary data
            df = df.iloc[1:]
            df.columns = ["employee_name", "transporter_id"] + list(df.columns[2:])

            new_column_names = df.columns[2].split("\n")

            # Shift the data in the DataFrame to match the new column names
            df[new_column_names[0]] = df[df.columns[2]].apply(lambda x: x.split("\n")[0])
            df[new_column_names[1]] = df[df.columns[2]].apply(lambda x: x.split("\n")[1])

            # Drop the original column "Opportunities\nSuccess" from the DataFrame
            df.drop(columns=df.columns[2], inplace=True)

            # Reorder the columns with the new column names
            df = df[['employee_name', 'transporter_id', new_column_names[0], new_column_names[1]] + list(df.columns[3:])]

        df.columns= [col.lower().replace(" ", "_").replace("-", "").replace("%","percent").replace("\n", "_") for col in df.columns]

        if index== 1:
            all_tables['POD_Summary']= df.to_dict(orient='records')
        
        if index==2:
            all_tables['Rejects_Category_Breakdown']= df.to_dict(orient='records')
        
        if index== 3:
            all_tables['Delivery_Acceptance_Report']= df.to_dict(orient='records')
        
        if index > 3:
            all_tables[f'other_table_{index}']= df.to_dict(orient='records')

    return all_tables



@app.post("/upload-pod-report/")
async def process_pdf_pod(file: UploadFile = File(...)):
    if file.filename.endswith('.pdf'):
        # Define the directory to save the PDF files
        save_directory = "pdfs"
        os.makedirs(save_directory, exist_ok=True)  # Create the directory if it doesn't exist

        # Specify the complete path to save the PDF file
        save_path = os.path.join(save_directory, file.filename)

        # Save the uploaded PDF file to the specified directory
        with open(save_path, "wb") as buffer:
            buffer.write(await file.read())

        json_data= process_pod_report(save_path)
        
        return {"status": True,"filename": file.filename, "data": json_data}
    
    return {'status': False, "message": "Uploaded file is not a PDF"}



#################################################################################################


def process_score_card(save_path):
    abc = camelot.read_pdf(save_path, pages="all")  # Replace with your file location
    # Initialize an empty dictionary to store DataFrames
    table_dict = {}

    # Iterate through each table extracted by Camelot
    for index, table in enumerate(abc, start=1):
        # Convert the table to a DataFrame
        df = table.df
        # # Use the second row as column names
        df.columns = df.iloc[1]
        # # # Skip the first row as it contains unnecessary data
        df = df.iloc[2:]
        # # Get the column names of the DataFrame
        columns = tuple(df.columns)
        
        # Check if a DataFrame with the same columns already exists in the dictionary
        if columns in table_dict:
            # skip columns name 
            df = df.iloc[1:]
            # If it does, append the current DataFrame to the existing one
            table_dict[columns] = pd.concat([table_dict[columns], df], ignore_index=True)
        else:
            # If it doesn't, create a new entry in the dictionary
            table_dict[columns] = df

    all_tables= {}

    # Print or process the resulting DataFrames
    for index, (columns, df) in enumerate(table_dict.items(), start=1):
        if all(column == '' for column in columns) and all(column == '' for column in df.columns) and all(all(value == '' for value in row) for _, row in df.iterrows()):
            continue  # Skip this DataFrame if both column names and data are empty

        df.columns= [col.lower().replace(" ", "_").replace("/", "_").replace("-", "").replace("%","percent").replace("\n", "") for col in df.columns]

        if index== 1:
            all_tables['DA_Current_Week_Performance']= df.to_dict(orient='records')
        
        if index==2:
            all_tables['DA_Trailing_Week_Performance']= df.to_dict(orient='records')

        if index > 2:
            all_tables[f'other_table_{index}']= df.to_dict(orient='records')

    return all_tables



@app.post("/upload-score-card/")
async def process_pdf_scorecard(file: UploadFile = File(...)):
    if file.filename.endswith('.pdf'):
        # Define the directory to save the PDF files
        save_directory = "pdfs"
        os.makedirs(save_directory, exist_ok=True)  # Create the directory if it doesn't exist

        # Specify the complete path to save the PDF file
        save_path = os.path.join(save_directory, file.filename)

        # Save the uploaded PDF file to the specified directory
        with open(save_path, "wb") as buffer:
            buffer.write(await file.read())

        json_data= process_score_card(save_path)
        
        return {"status": True,"filename": file.filename, "data": json_data}
    
    return {'status': False, "message": "Uploaded file is not a PDF"}







if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8009, reload=True)
