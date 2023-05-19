import pandas as pd

# Read the Excel file into a DataFrame
df = pd.read_excel('input.xlsx')

# Create a new DataFrame to store the transformed data
new_df = pd.DataFrame(columns=['ROW NO', 'Names', 'Code', 'Debit Amount', 'Credit Amount', 'Pages'])

# Iterate over each row in the source DataFrame
for index, row in df.iterrows():
    # Empty row
    new_df = new_df.append(pd.Series(dtype='object'), ignore_index=True)

    # Second row
    new_df = new_df.append({
        'Names': 'OMNIUBUS',
        'Code': '7831505',
        'Debit Amount': row['AMOUNT']
    }, ignore_index=True)

    # Third row
    new_df = new_df.append({
        'ROW NO': index + 1,
        'Pages': index + 1,
        'Names': 'SMP',
        'Code': '22001100300000000RMB002Q',
        'Credit Amount': row['AMOUNT']
    }, ignore_index=True)

    # Fourth row
    new_df = new_df.append({
        'Names': row['Client Name']
    }, ignore_index=True)

# Save the transformed DataFrame to a new Excel file
new_df.to_excel('output.xlsx', index=False)
