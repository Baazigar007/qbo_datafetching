from quickbooks import QuickBooks
from intuitlib.client import AuthClient
from intuitlib.enums import Scopes
from quickbooks.objects.customer import Customer
from quickbooks.objects.invoice import Invoice
import uuid
import csv
import mysql.connector
import schedule
import time
from datetime import datetime, date
import pandas as pd
import re

auth_client = AuthClient(
        client_id='ABAIju7db2lIL1HqnR0wTRVKrKyrJkS8ZSrLHBnA52RAKvqY07',
        client_secret='2usElguOgftbR3VTkox3RyPAGjPJRbapvREUfmE3',
        # access_token="",  # If you do not pass this in, the Quickbooks client will call refresh and get a new access token. 
        environment='production',
        redirect_uri='https://developer.intuit.com/v2/OAuth2Playground/RedirectUrl',
    )
print(auth_client.refresh_token)


from quickbooks import QuickBooks

client = QuickBooks(
        auth_client=auth_client,
        refresh_token='AB11707589104G95cyu0IKqmdLvxsSvCpz0pnH4qGCDwMKzO8R',
        company_id='9130356041310986',
    )

client = QuickBooks(
    auth_client=auth_client,
    refresh_token='AB11707589104G95cyu0IKqmdLvxsSvCpz0pnH4qGCDwMKzO8R',
    company_id='9130356041310986',
    minorversion=69
)


# Sending the GET request

invoices = Invoice.all(qb=client,start_position="1", max_results=1000,)

invoices_dict = []






# Getting School 
def getSchool(customfield):
    for values in customfield:
       if values["Name"]=="School":
          return values["StringValue"]    
    return ""


# Getting Product Name
def getProductName(Line):
    for item in Line:
        if "SalesItemLineDetail" in item:
            sales_item_line_detail = item["SalesItemLineDetail"]
            item_ref = sales_item_line_detail.get("ItemRef")
            if item_ref and "name" in item_ref:
                product_name = item_ref["name"]
                if product_name:
                    return product_name  # Return the product name
    return None  # Return None if no product name is found

# Getting Unit Price
def getUnitPrice(Line):
    for item in Line:
        if "SalesItemLineDetail" in item:
            sales_item_line_detail = item["SalesItemLineDetail"]
            unit_price = sales_item_line_detail.get("UnitPrice")
            if unit_price is not None:
                return unit_price  # Return the unit price
    return None  # Return None if no unit price is found

# Getting Qty
def getQty(Line):
    for item in Line:
        if "SalesItemLineDetail" in item:
            sales_item_line_detail = item["SalesItemLineDetail"]
            qty = sales_item_line_detail.get("Qty")
            if qty is not None:
                return qty  # Return the quantity
    return None  # Return None if no quantity is found

# Getting Amount

def getAmount(Line):
 
  amount = []
  for item in Line:
    if "Amount" not in item:
      continue

    amount.append(item.get("Amount"))


  for price in amount:
   return price


# UUID
def get_uuid():
  my_uuid = uuid.uuid4()

  my_uuid_string = str(my_uuid)

  return my_uuid_string




for invoice in invoices:
    invoice_dict = invoice.to_dict()
    newmap = {}
    newmap["uuid"]=get_uuid()
    newmap["invoiceId"]=invoice_dict["DocNumber"]
    newmap["date"]=invoice_dict["TxnDate"]
    newmap["school"]=getSchool(invoice_dict["CustomField"])
    newmap["sorority"] = invoice_dict["CustomerRef"]["name"]
    newmap['product']=getProductName(invoice_dict["Line"])
    newmap["amount"]=getAmount(invoice_dict["Line"])
    newmap["productQty"]=getQty(invoice_dict["Line"])
    newmap["unitPrice"]=getUnitPrice(invoice_dict["Line"])

    
    
    invoices_dict.append(newmap) 

print(invoices_dict)
# print(invoices)


csv_columns = ["uuid", "invoiceId", "date", "school", "sorority", "product", "amount", "productQty", "unitPrice",]


def read_existing_data(file_path):
    existing_data = []
    try:
        with open(file_path, 'r', newline='', encoding="utf-8") as input_file:
            reader = csv.DictReader(input_file)
            for row in reader:
                existing_data.append(row)
    except FileNotFoundError:
        pass  # If the file doesn't exist, there's no existing data
    return existing_data

def write_data_to_csv(data, file_path, csv_columns):
    with open(file_path, 'w', newline='', encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=csv_columns)
        writer.writeheader()
        writer.writerows(data)



csv_columns = ["uuid", "invoiceId", "date", "school", "sorority", "product", "amount", "productQty", "unitPrice",]

def process_invoices():
    existing_data = read_existing_data("outputdataNEW.csv")
    existing_ids = set(entry["invoiceId"] for entry in existing_data)

    for invoice in invoices:
        invoice_dict = invoice.to_dict()
        invoice_id = invoice_dict["DocNumber"]

        if invoice_id not in existing_ids:
            school = getSchool(invoice_dict["CustomField"])
            sorority = invoice_dict["CustomerRef"]["name"]
            invoice_date = invoice_dict["TxnDate"]

            for product_line in invoice_dict["Line"]:
                if "SalesItemLineDetail" in product_line:
                    newmap = {}
                    newmap["uuid"] = get_uuid()
                    newmap["invoiceId"] = invoice_id
                    newmap["date"] = invoice_date
                    newmap["school"] = school
                    newmap["sorority"] = sorority
                    newmap["product"] = getProductName([product_line])
                    newmap["amount"] = getAmount([product_line])
                    newmap["productQty"] = getQty([product_line])
                    newmap["unitPrice"] = getUnitPrice([product_line])
                    existing_data.append(newmap)

    write_data_to_csv(existing_data, "outputdataNEW.csv", csv_columns)

# Call the function to process the invoices
process_invoices()





def import_csv_to_dbeaver_database_using_mysql(csv_file_path, database_connection):
    cursor = database_connection.cursor()

    with open(csv_file_path, "r") as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)

        for row in reader:
                cursor.execute(
                    "INSERT INTO qbo_new (uuid, invoiceId, date, school, sorority, product, amount, productQty, unitPrice) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8])
                )
                

    database_connection.commit()
    cursor.close()  
    print("Data imported into the database")


def process_invoices():
  """Processes the invoices and returns a list of rows to be inserted into the database."""

  # TODO: Implement this function

  return []

def get_last_date_updated():

  # TODO: Implement this function

  return date.today()

def update_database_periodically():
  if is_last_date():
    # Process invoices and import data to the database
    process_invoices()
    import_csv_to_dbeaver_database_using_mysql(csv_file_path, connection)

def is_last_date():
  """Checks if the current date is the same as the last date the data was updated.

  Returns:
    True if the current date is the same as the last date the data was updated,
    False otherwise.
  """

  last_date = get_last_date_updated()
  current_date = datetime.date.today()

  return last_date == current_date

# Specify the CSV file path
csv_file_path = "outputdataNEW.csv"

# Connect to the MySQL database
connection = mysql.connector.connect(
        host="us-cdbr-east-06.cleardb.net",
        user="b529606bdcbbbf",
        password="e577a1cc",
        database="heroku_cd6163c1f2350a7"
    )

schedule.every().day.at("00:00").do(update_database_periodically)
