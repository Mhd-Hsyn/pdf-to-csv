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
        processed_df.rename(columns=lambda x: x.lower().replace(' ', '_'), inplace=True)

        more_processed_df = process_data_frame(processed_df)
        # more_processed_df.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
        json_data = more_processed_df.to_dict(orient='records')
        
        return {"filename": file.filename, "data": json_data}
    
    return {'status': False, "message": "Uploaded file is not a PDF"}





if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)