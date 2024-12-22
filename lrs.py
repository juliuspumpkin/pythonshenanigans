import csv
from datetime import datetime

# Function to convert the date format
def convert_date_format(date_str):
    return datetime.strptime(date_str, '%d-%b-%y').strftime('%d/%m/%Y')

# Read the input data from a CSV file
source_file = 'source.csv'

# Open the source CSV file
with open(source_file, mode='r') as file:
    reader = csv.reader(file)
    data = list(reader)

# Extract relevant data
reporting_date = data[1][1]  # "17-Feb-23"
bank_code = data[2][1]        # "054"
records_count = int(data[5][1])  # "6"
total_amount = data[6][1]     # "21235.50"

# Prepare the output
output = []

# Filing Information Section
output.append("Table Start")
output.append("Filing Information,R010-T001")
output.append("Return Name,Return Code,Bank Name,Bank Code,Frequency,Reporting End Date")
output.append("Liberalised Remittance Scheme,R010,TIMES BANK LIMITED,{},{},{}".format(bank_code, 'D', datetime.strptime(convert_date_format(reporting_date), '%d/%m/%Y').strftime('%d%m%Y')))

# Records Section
output.append("Table Start")
output.append("Liberalised Remittance Scheme,R010-T002")
output.append("Record identifier,PAN of remitter,Name of remitter,Aadhar of remitter,Beneficiary’s country code,Date of remittance,Purpose Code,Currency code,Amount (Amount in USD),Remarks/Any other comments")

# Iterate through the records
for row in data[8:]:
    record_id = row[0]
    pan_remitter = row[1]
    name_remitter = row[2]
    aadhar = row[3]
    country_code = row[4]
    date_remittance = convert_date_format(row[5])
    purpose_code = row[6]
    currency_code = row[7]
    amount = row[8]
    remarks = "NA"  # As per your request

    output.append("{},{},{},{},{},{},{},{},{},{}".format(record_id, pan_remitter, name_remitter, aadhar, country_code, date_remittance, purpose_code, currency_code, amount, remarks))

# Total Amount Section
output.append("Table Start")
output.append("Total Amount,R010-T003")
output.append("particulars,Value")
output.append("Total Amount,{:.2f}".format(float(total_amount)))

# Record Count Section
output.append("Table Start")
output.append("Record Count,R010-T004")
output.append("particulars,Value")
output.append("Record Count,{}".format(records_count))

# Write the output to a file
with open('R010.txt', mode='w',encoding='utf8') as target_file:
    for line in output:
        target_file.write(line + '\n')

print("Output written to target.txt successfully! 🎉")
