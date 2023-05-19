import openpyxl
from openpyxl.styles import Border, Side

# Load the workbook
workbook = openpyxl.load_workbook('output.xlsx')

# Select the desired worksheet
worksheet = workbook.active

# Define border styles
border = Border(left=Side(border_style='thin'),
                right=Side(border_style='thin'),
                top=Side(border_style='thin'),
                bottom=Side(border_style='thin'))

# Apply border to columns A to F
for column in ['A', 'B', 'C', 'D', 'E', 'F']:
    for row in range(1, worksheet.max_row + 1):
        cell = worksheet[column + str(row)]
        cell.border = border

# Save the modified workbook
workbook.save('output.xlsx')
