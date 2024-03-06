import pandas as pd
import os


def extract_month_list(df):
    """Extracts a list of month names from the second row of the DataFrame.

    Args:
        df: The input pandas DataFrame.

    Returns:
        A list of month names.
    """
    # Reshape the DataFrame to facilitate the extraction
    df.reset_index(drop=True, inplace=True)

    return df.iloc[1, 4:16].tolist()


def extract_lead_data(df):
    """Extracts all LEAD dataframes and their corresponding LEAD values.

    Args:
        df: The input pandas DataFrame.

    Returns:
        A list of tuples, where each tuple contains:
            * lead_df: A DataFrame representing a single LEAD block.
            * lead_value: The LEAD value for that block.
    """

    result = []  
    current_lead = []

    df.reset_index(drop=True, inplace=True)

    for i, row in df.iterrows():
        first_column_value = row[1]

        # Condition 1: First column is non-null, others are all null
        if pd.notna(first_column_value) and row[2:].isna().all():

            # Condition 2: Value in the first column appears less than 9 times
            if df[df[1] == first_column_value].count()[1] < 9: 

                # Condition 3: Value is different from previous row (optional, keep if needed)
                if (i == 0 or row[1] != df.iloc[i - 1, 1]):  
                    if current_lead: 
                        lead_df = pd.DataFrame(current_lead)
                        lead_df.reset_index(inplace=True, drop=True)
                        lead_value = lead_df.iloc[0, 0]
                        # print(lead_value)
                        result.append((lead_df, lead_value))
                        current_lead = []  
                    current_lead.append(row)

        elif current_lead:
            current_lead.append(row)

    # Process the last LEAD block (if any)
    if current_lead: 
        lead_df = pd.DataFrame(current_lead)
        lead_df.reset_index(inplace=True, drop=True)
        lead_value = lead_df.iloc[0, 0]
        result.append((lead_df, lead_value))

    return result 



def extract_product_data(lead_df):
    """Extracts all PRODUCT dataframes and their corresponding PRODUCT values.

    Args:
        df: The input pandas DataFrame Lead df.

    Returns:
        A list of tuples, where each tuple contains:
            * product_df: A DataFrame representing a single PRODUCT block.
            * product_value: The PRODUCT value for that block.
    """

    result = []  # Store (product_df, product_value) pairs
    current_product = []

    # Reshape the DataFrame to facilitate the extraction
    lead_df.reset_index(drop=True, inplace=True)  

    for i, row in lead_df.iterrows():
        if pd.notna(row[2]) and lead_df.iloc[i, 3:].isna().all() and (i == 0 or row[2] != lead_df.iloc[i-1, 2]):
            if current_product:  # End of the previous PRODUCT
                product_df = pd.DataFrame(current_product)
                product_df.reset_index(inplace=True, drop=True)
                product_value = product_df.iloc[0, 2]
                result.append((product_df, product_value))
                current_product = []  # Start a new PRODUCT
            current_product.append(row)
        elif current_product:
            current_product.append(row)

    # Process the last PRODUCT (if any)
    if current_product:
        product_df = pd.DataFrame(current_product)
        product_df.reset_index(inplace=True, drop=True)
        product_value = product_df.iloc[0, 2]
        result.append((product_df, product_value))

    return result


def transform_product_data(product_df, lead_value, product_value, month):
    """Transforms product data into the desired format.

    Args:
        product_df: The DataFrame containing the raw product data. 
        lead_value: The LEAD value associated with the product.
        product_value: The PRODUCT value associated with the product.
        month: A list of month names.

    Returns:
        The reshaped and transformed product DataFrame.
    """

    # Reshape the DataFrame to facilitate the extraction
    product_df.reset_index(drop=True, inplace=True)  

    # Extract new column names
    new_columns = product_df.iloc[:, 3].dropna().str.strip().tolist() 
    
    # Extract target data and reshape
    data = product_df.iloc[1:10, 4:16].values
    reshaped_data = pd.DataFrame(data).T
    reshaped_data.columns = new_columns

    # Add additional columns
    reshaped_data['PRODUCT GROUP'] = product_df.iloc[0, 0]
    reshaped_data['TYPE'] = product_df.iloc[0, 1]
    reshaped_data['Month'] = month
    reshaped_data['LEAD'] = lead_value
    reshaped_data['PRODUCT'] = product_value

    # Reorder columns
    new_order = ['LEAD', 'PRODUCT GROUP', 'TYPE', 'PRODUCT', 'Month'] + new_columns
    reshaped_data = reshaped_data[new_order]
    # print(reshaped_data)

    return reshaped_data


def process_file(file_path):
    """Processes a single Excel file."""
    df = pd.read_excel(file_path, usecols='B:Q', header=None)
    df.reset_index(drop=True, inplace=True)

    month = extract_month_list(df)

    result_data = []
    for lead_df, lead_value in extract_lead_data(df):
        product_data = extract_product_data(lead_df)
        for product_df, product_value in product_data:
            transformed_df = transform_product_data(product_df, lead_value, product_value, month)
            result_data.append(transformed_df)

    return result_data


def main():
    folder_path = r"C:\Users\Woody\Documents\SWG\temp\input"  # Replace with your folder path 
    result_data = []

    for filename in os.listdir(folder_path):
        if filename.endswith(".xlsx"):
            file_path = os.path.join(folder_path, filename)
            file_results = process_file(file_path)
            result_data.extend(file_results)  

    final_df = pd.concat(result_data, ignore_index=True)
    final_df.to_excel('result.xlsx', index=False)

if __name__ == "__main__":
    main()