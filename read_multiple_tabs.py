import pandas as pd
from xml.etree.ElementTree import Element, SubElement, Comment, tostring
import xml.dom.minidom

# read the excel file into a pandas dataframe
df_bills = pd.read_excel('your_file_name.xlsx', sheet_name='Bills')
df_invoices = pd.read_excel('your_file_name.xlsx', sheet_name='Invoices')

# create an empty dictionary to hold bill details
bills_dict = {}

# loop through each row of the bills data frame and add bill details to the dictionary
for index, row in df_bills.iterrows():
    bill_id = row['BillID']
    bill_amount = row['Amount']
    bill_date = row['Date']
    invoices_list = []
    
    # loop through each row of the invoices data frame and add all invoices related to the current bill to the invoices list
    for index2, row2 in df_invoices.loc[df_invoices['BillId'] == bill_id].iterrows():
        invoice_id = row2['InvoiceId']
        invoice_amount = row2['Amount']
        invoice_date = row2['Date']
        invoices_list.append({'id': invoice_id, 'amount': invoice_amount, 'date': invoice_date})
    
    # add the current bill details and its related invoices list to the dictionary
    bills_dict[bill_id] = {'amount': bill_amount, 'date': bill_date, 'invoices': invoices_list}

# create the root element for the XML message
root = Element('bills')

# loop through each bill in the bills dictionary and add it to the XML message
for bill_id, bill_details in bills_dict.items():
    bill_element = SubElement(root, 'bill')
    bill_id_element = SubElement(bill_element, 'billId')
    bill_id_element.text = str(bill_id)
    bill_amount_element = SubElement(bill_element, 'billAmount')
    bill_amount_element.text = str(bill_details['amount'])
    bill_date_element = SubElement(bill_element, 'billDate')
    bill_date_element.text = str(bill_details['date'])
    
    if len(bill_details['invoices']) > 0:
        invoices_element = SubElement(bill_element, 'invoices')
        for invoice in bill_details['invoices']:
            invoice_element = SubElement(invoices_element, 'invoice')
            invoice_id_element = SubElement(invoice_element, 'invoiceId')
            invoice_id_element.text = str(invoice['id'])
            invoice_amount_element = SubElement(invoice_element, 'invoiceAmount')
            invoice_amount_element.text = str(invoice['amount'])
            invoice_date_element = SubElement(invoice_element, 'invoiceDate')
            invoice_date_element.text = str(invoice['date'])
    else:
        invoices_element = SubElement(bill_element, 'invoices')

# pretty print the XML message and display it
xml_message_str = xml.dom.minidom.parseString(tostring(root)).toprettyxml()
print(xml_message_str)
