import base64
import mysql.connector
import requests
import time
import os

from quickbooks import QuickBooks
from intuitlib.client import AuthClient
from quickbooks.objects.invoice import Invoice
import uuid

SECRET_DICT = {}

class TokenRefreshError(Exception):
    pass
    
def load_tokens_from_heroku():
    SECRET_DICT["access_token"] = os.environ.get("QBO_ACCESS_TOKEN")
    SECRET_DICT["refresh_token"] = os.environ.get("QBO_REFRESH_TOKEN")
    SECRET_DICT["mysql_host"] = os.environ.get("MYSQL_HOST")
    SECRET_DICT["mysql_user"] = os.environ.get("MYSQL_USERNAME")
    SECRET_DICT["mysql_pwd"] = os.environ.get("MYSQL_PWD")
    SECRET_DICT["client_id"] = os.environ.get("CLIENT_ID")
    SECRET_DICT["client_secret"] = os.environ.get("CLIENT_SECRET") 
    SECRET_DICT["realm_id"] = os.environ.get("QBO_REALM_ID")
    SECRET_DICT["redirect_uri"] = os.environ.get("REDIRECT_URI")
    SECRET_DICT["authorization_basic"] = base64.b64encode("{client_id}:{client_secret}".format(
        client_id=os.environ.get("CLIENT_ID"),
        client_secret=os.environ.get("CLIENT_SECRET")
    ).encode()).decode()
    
def refresh_tokens(refresh_token, authorization_basic):
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': f'Basic {authorization_basic}',
        'accept': 'application/json',
    }

    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
    }

    try:
        response = requests.post('https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer', headers=headers, data=data)
        response.raise_for_status()  # Check for HTTP errors

        json_data = response.json()
        refresh_token = json_data.get('refresh_token', '')
        access_token = json_data.get('access_token', '')

        if refresh_token and access_token:
            if refresh_token != SECRET_DICT.get('refresh_token', ''):
                print('!!!REFRESH TOKEN GOT CHANGED!!!')
            SECRET_DICT['refresh_token'] = refresh_token
            SECRET_DICT['access_token'] = access_token
    except requests.exceptions.HTTPError as err:
        if err.response.status_code == 400:
            # Log the error and response content for debugging
            print(f"HTTP 400 Bad Request. Response content: {err.response.text}")
        else:
            # Handle other HTTP errors here
            print(f"HTTP Error: {err}")

def refresh_token():
    try:
        refresh_tokens(SECRET_DICT['refresh_token'], SECRET_DICT["authorization_basic"])
    except TokenRefreshError:
        raise

def process_invoices(connection: mysql.connector.connect):
    auth_client = AuthClient(
            client_id=SECRET_DICT["client_id"],
            client_secret=SECRET_DICT["client_secret"],
            environment='production',
            redirect_uri=SECRET_DICT["redirect_uri"]
        )

    client = QuickBooks(
            auth_client=auth_client,
            company_id=SECRET_DICT["realm_id"],
            refresh_token=SECRET_DICT["refresh_token"]
        )
    last_value_date_query = "SELECT max(date) from qbo_data;"
    cursor = connection.cursor()
    cursor.execute(last_value_date_query)
    response = cursor.fetchall()
    last_value_date = response[0][0]

    invoices = Invoice.query("SELECT * from {table_name} where TxnDate > '{last_value_date}'".format(
        table_name = Invoice.qbo_object_name,
        last_value_date = last_value_date
    ), qb=client)

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
            return price

    def get_descriptions(Line):
        descriptions = []  # Initialize a list to store descriptions for each line
        for item in Line:
            if isinstance(item, dict) and "Description" in item and item["Description"] is not None:
                description = item["Description"]
                descriptions.append(description)
        for d in descriptions:
            return d
    
    dataset = []
    for invoice in invoices:
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

        dataset.append(newmap)

    return dataset

def export_data_to_mysql_database(invoices, database_connection):
    query_template = "INSERT INTO qbo_data (uuid, invoiceId, date, school, sorority, product, amount, productQty, unitPrice, descriptions) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor = database_connection.cursor()

    values = []
    for invoice in invoices:
        values.append((
            invoice['uuid'], invoice['invoiceId'], invoice['date'], 
            invoice['school'], invoice['sorority'], invoice['product'], 
            invoice['amount'], invoice['productQty'], invoice['unitPrice'], 
            invoice['descriptions']
        ))

    try:
        cursor.executemany(query_template, values)
        
        database_connection.commit()

        print("Data imported into the database")
    except mysql.connector.Error as err:
        print(f"Error executing SQL query: {err}")
    finally:
        cursor.close()

def update_database_periodically():
    refresh_token()
    connection = mysql.connector.connect(
        host=SECRET_DICT["mysql_host"],
        user=SECRET_DICT["mysql_user"],
        password=SECRET_DICT["mysql_pwd"],
        database="qbo"
    )

    invoices = process_invoices(connection)

    print("Starting database update...")
    if invoices:
        print(f'Dumping {len(invoices)} records...')
        export_data_to_mysql_database(invoices, connection)
        print("Data uploaded successfully!")
    else:
        print('No new data to upload!')
    connection.close()

if __name__ == "__main__":
    load_tokens_from_heroku()
    while True:
        update_database_periodically()
        time.sleep(60*30) # waiting for 30 mins
