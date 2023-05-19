import openpyxl
from openpyxl.styles import Border, Side

# Load the workbook
workbook = openpyxl.load_workbook('output.xlsx')

# Select the desired worksheet
worksheet = workbook.active

# Define border styles
thin_border = Border(left=Side(border_style='thin'),
                     right=Side(border_style='thin'),
                     top=Side(border_style='thin'),
                     bottom=Side(border_style='thin'))
left_right_border = Border(left=Side(border_style='thin'),
                           right=Side(border_style='thin'))
left_bottom_border = Border(left=Side(border_style='thin'),
                            right=Side(border_style='thin'),
                            bottom=Side(border_style='thin'))
right_bottom_border = Border(right=Side(border_style='thin'),
                             bottom=Side(border_style='thin'))

# Apply borders to the header row
header_row = worksheet[1]
for cell in header_row:
    cell.border = thin_border

# Apply borders to each group of 4 rows
for row_index in range(2, worksheet.max_row + 1, 4):
    # First row in the group
    first_row = worksheet[row_index]
    for cell in first_row:
        cell.border = thin_border

    # Second row in the group
    second_row = worksheet[row_index + 1]
    for cell in second_row:
        if cell.column_letter == 'D':
            cell.border = thin_border
        else:
            cell.border = left_right_border

    # Third row in the group
    third_row = worksheet[row_index + 2]
    for cell in third_row:
        cell.border = left_right_border

    # Fourth row in the group
    fourth_row = worksheet[row_index + 3]
    for cell in fourth_row:
        cell.border = right_bottom_border

# Save the modified workbook
workbook.save('output.xlsx')
