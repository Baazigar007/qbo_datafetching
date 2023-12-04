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
from datetime import datetime, timedelta
import pandas as pd
import requests
import os

def process_invoices():

# Your client ID and client secret obtained from QuickBooks Developer Dashboard
    client_id = "ABAIju7db2lIL1HqnR0wTRVKrKyrJkS8ZSrLHBnA52RAKvqY07"
    client_secret = "2usElguOgftbR3VTkox3RyPAGjPJRbapvREUfmE3"
    redirect_uri = "https://developer.intuit.com/v2/OAuth2Playground/RedirectUrl"

    # The refresh token obtained during the initial OAuth 2.0 authorization
    refresh_token = "AB11710351767KwdQqbdUrqbBQ14zfD5mDoiJQ9GwlmwPL5wdj"

    # QuickBooks API endpoints
    # authorization_base_url = "https://appcenter.intuit.com/connect/oauth2"
    token_url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

    # Set up OAuth2 session
    oauth = OAuth2Session(client_id, redirect_uri=redirect_uri, token={'refresh_token': refresh_token})

    # Refresh the access token using the refresh token
    token = oauth.refresh_token(token_url, client_id=client_id, client_secret=client_secret)

    # Now you can use the new access token
    access_token = token['access_token']

    # Example: Print the access token
    print("Access Token:", access_token)


    
    auth_client = AuthClient(
            client_id='ABAIju7db2lIL1HqnR0wTRVKrKyrJkS8ZSrLHBnA52RAKvqY07',
            client_secret='2usElguOgftbR3VTkox3RyPAGjPJRbapvREUfmE3',
            access_token=access_token,  # If you do not pass this in, the Quickbooks client will call refresh and get a new access token. 
            environment='production',
            redirect_uri='https://developer.intuit.com/v2/OAuth2Playground/RedirectUrl',

        )


    # # refresh_token = os.environ.get('QUICKBOOKS_REFRESH_TOKEN')
    client = QuickBooks(
            auth_client=auth_client,
            # refresh_token=refresh_token,
            # refresh_token="AB11710351767KwdQqbdUrqbBQ14zfD5mDoiJQ9GwlmwPL5wdj",
            company_id='9130356041310986',
        )

    

    
#     auth_client = AuthClient(
#             client_id='ABAIju7db2lIL1HqnR0wTRVKrKyrJkS8ZSrLHBnA52RAKvqY07',
#             client_secret='2usElguOgftbR3VTkox3RyPAGjPJRbapvREUfmE3',
#             # access_token="",  # If you do not pass this in, the Quickbooks client will call refresh and get a new access token. 
#             environment='production',
#             redirect_uri='https://developer.intuit.com/v2/OAuth2Playground/RedirectUrl',
#         )


# # refresh_token = os.environ.get('QUICKBOOKS_REFRESH_TOKEN')

#     client = QuickBooks(
#             auth_client=auth_client,
#             # refresh_token=refresh_token,
#             refresh_token="AB11710265193s93KyZf6Q8Dv6vX02L7MYGEVEPgWBoL1B5cJm",
#             company_id='9130356041310986',
#         )

#     client = QuickBooks(
#         auth_client=auth_client,
#         refresh_token="AB11710265193s93KyZf6Q8Dv6vX02L7MYGEVEPgWBoL1B5cJm",
#         company_id='9130356041310986',
#         minorversion=69
#     )



    # Sending the GET request
    invoices = Invoice.all(qb=client,start_position="1", max_results=1000)
    # print(invoices)
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


    # Getting Unit Price
    def getUnitPrice(Line):
        for item in Line:
            if "SalesItemLineDetail" in item:
                sales_item_line_detail = item["SalesItemLineDetail"]
                unit_price = sales_item_line_detail.get("UnitPrice")
                if unit_price is not None:
                    return unit_price  # Return the unit price
        return None  # Return None if no unit price is found



    # UUID
    def get_uuid():
        my_uuid = uuid.uuid4()

        my_uuid_string = str(my_uuid)

        return my_uuid_string


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
    #    print (price,amount)
         return price
    # Getting Description

    def get_descriptions(Line):
        descriptions = []  # Initialize a list to store descriptions for each line
        for item in Line:
            # print(item,"item")
            # print(isinstance(item, dict),"Description" in item,item["Description"])
            if isinstance(item, dict) and "Description" in item and item["Description"] is not None:
                # print("description",descriptions)
                description = item["Description"]
                # print(description)
                descriptions.append(description)
                # print(descriptions)
        #return descriptions  
    # Return the list of descriptions outside of the for loop
        for d in descriptions:
            # print (d,descriptions)
            return d
  
    previous_date = datetime.now() - timedelta(days=1)
    previous_date_str = previous_date.strftime("%Y-%m-%d")
    
     # Manually filter invoices based on the transaction date
    filtered_invoices = [invoice for invoice in invoices if invoice.to_dict()["TxnDate"] == previous_date_str]

    for invoice in filtered_invoices:
        invoice_dict = invoice.to_dict()
        newmap = {}
        newmap["uuid"] = get_uuid()
        newmap["invoiceId"] = invoice_dict["DocNumber"]
        newmap["date"] = invoice_dict["TxnDate"]
        newmap["school"] = getSchool(invoice_dict["CustomField"])
        newmap["sorority"] = invoice_dict["CustomerRef"]["name"]
        newmap['product'] = getProductName(invoice_dict["Line"])
        newmap["amount"] = getAmount(invoice_dict["Line"])
        newmap["productQty"] = getQty(invoice_dict["Line"])
        newmap["unitPrice"] = getUnitPrice(invoice_dict["Line"])
        newmap["descriptions"] = get_descriptions(invoice_dict["Line"])

        invoices_dict.append(newmap)




    csv_columns = ["uuid", "invoiceId", "date", "school", "sorority", "product", "amount", "productQty", "unitPrice", "descriptions"]

    existing_data = []

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
            # writer.writeheader()
            # writer.writerows(data)
            if output_file.tell() == 0:  # Check if the file is empty
                writer.writeheader()
            writer.writerows(data)


    existing_data = read_existing_data("outputdataNEW.csv")
    existing_ids = set(entry["invoiceId"] for entry in existing_data)

    # for invoice in invoices:
    for invoice in filtered_invoices:
        invoice_dict = invoice.to_dict()
        invoice_id = invoice_dict["DocNumber"]

        if invoice_id not in existing_ids:
            school = getSchool(invoice_dict["CustomField"])
            sorority = invoice_dict["CustomerRef"]["name"]
            invoice_date = invoice_dict["TxnDate"]

            for product_line in invoice_dict["Line"]:
                # print ("PRODUCT LINE ", product_line,"AND INVOICE DICT",invoice_dict["Line"])
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
                    newmap["descriptions"] = get_descriptions([product_line])
                    existing_data.append(newmap)

    write_data_to_csv(existing_data, "outputdataNEW.csv", csv_columns)
    print("CSV file has been updated.")
# Call the function to process the invoices
process_invoices()


def import_csv_to_dbeaver_database_using_mysql(csv_file_path, database_connection):
    cursor = database_connection.cursor()

    with open(csv_file_path, "r") as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None) # Skip the header row

        for row in reader:
                # If not, insert the record
                cursor.execute(
                    "INSERT INTO qbo_data (uuid, invoiceId, date, school, sorority, product, amount, productQty, unitPrice, descriptions) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
                )
                database_connection.commit()

    cursor.close()
    print("Data imported into the database")


# Function to update the database periodically
def update_database_periodically():
    
    process_invoices()

    print("Starting database update...")
    # Connect to the MySQL database
    connection = mysql.connector.connect(
        host="39q6t8.stackhero-network.com",
        user="qb_sync",
        password="qbo123",
        database="qbo"    
        
    
    )
    # Specify the CSV file path
    csv_file_path = "outputdataNEW.csv"


    # Import data from the CSV file to the database, passing the processed_records set
    import_csv_to_dbeaver_database_using_mysql(csv_file_path, connection)

    # Close the database connection
    connection.close()

# Schedule the update function to run every day at 12 am 
schedule.every().day.at("17:00:00").do(update_database_periodically)
# schedule.every(5).minutes.do(update_database_periodically)
print("Done updating")

while True:
    schedule.run_pending()
    time.sleep(1)






