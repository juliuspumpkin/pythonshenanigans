import pandas as pd

# Read the Excel file into a DataFrame
df = pd.read_excel('input.xlsx')

# Create a new DataFrame with the desired columns and data
new_df = pd.DataFrame({
    'ROW NO': range(1, len(df) + 1),
    'Names': 'OMNIUBUS',
    'Code': '7831505',
    'Debit Amount': df['AMOUNT'],
    'Credit Amount': '',
    'Pages': range(1, len(df) + 1)
})

# Add additional columns to new_df
new_df['Names'] += ' ' + df['Client Name'] + ' ON BEHALF OF BESTBANK'
new_df['Credit Amount'] = df['AMOUNT']

# Save the new DataFrame to a new Excel file
new_df.to_excel('output.xlsx', index=False)
