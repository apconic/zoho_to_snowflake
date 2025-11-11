# -*- coding: utf-8 -*-
"""
Created on Fri Nov  7 11:45:11 2025

@author: admin
"""

import requests
import csv
import urllib.parse
import json

# List of tables provided
tables = [
    "Accounts",
    "Accrual Transactions",
    "Attribute",
    "Attribute Options",
    "Batch Number IN",
    "Batch Number Out",
    "Bill Item",
    "Bills",
    "Brand",
    "Category",
    "Composite Item",
    "Composite Item Details",
    "Credit Note Items",
    "Credit Notes",
    "Credit Notes Refund",
    "Creditnotes Invoice",
    "Customer Addresses",
    "Customer Contact Persons",
    "Customer Payments",
    "Customer Visit",
    "Customers",
    "Delivery Challan",
    "Delivery Challan Item",
    "Delivery Challans Invoice",
    "Expense Item",
    "Expenses",
    "FIFO Mapping Table",
    "Inventory Adjustment Items",
    "Inventory Adjustments",
    "Inventory Mapping",
    "Invoice Items",
    "Invoice Payments",
    "Invoices",
    "Item Group",
    "Items",
    "Locations",
    "Manual Journal Items",
    "Manual Journals",
    "Manufacturer",
    "Payment Refunds",
    "Payments Made",
    "Price List Item",
    "Price lists",
    "Purchase Order Bill",
    "Purchase Order Items",
    "Purchase Order Sales Order Mapping",
    "Purchase Orders",
    "RD Sales Person",
    "Reporting Tags",
    "Sales Order Invoice",
    "Sales Order Items",
    "Sales Orders",
    "Sales Persons",
    "Salesforce Sep25 Accounts",
    "Salesforce Sep25 Products",
    "Serial Numbers",
    "Stock In Flow Table",
    "Stock Out Flow Table",
    "Taxes",
    "Transfer Order",
    "Transfer Order Items",
    "Users",
    "Vendor Contact Persons",
    "Vendor Credit Items",
    "Vendor Credit Refunds",
    "Vendor Credits",
    "Vendor Credits Bill",
    "Vendor Payment Refund",
    "Vendor Payments",
    "Vendors",
    "Warehouses"
]

# Prompt for required inputs
owner_email = "hitesh@retaildenmart.com".strip()
workspace_name = "Zoho Finance Retail Denmart"
access_token = "1000.54d9d78686c60e48e745e9d3dba8cb14.1c197531f0fc6b26dbeb8fc0fb0b45de".strip()

base_url = "https://analyticsapi.zoho.in"

headers = {
    "Authorization": f"Zoho-oauthtoken {access_token}"
}

params = {
    "ZOHO_ACTION": "VIEWMETADATA",
    "ZOHO_OUTPUT_FORMAT": "JSON",
    "ZOHO_ERROR_FORMAT": "JSON",
    "ZOHO_API_VERSION": "1.0"
}

for table in tables:
    # URL encode the table name
    encoded_table = urllib.parse.quote(table, safe='')
    url = f"{base_url}/api/{owner_email}/{workspace_name}/{encoded_table}"
    
    try:
        response = requests.post(url, data=params, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        if "response" in data and "result" in data["response"] and "viewInfo" in data["response"]["result"]:
            column_list = data["response"]["result"]["viewInfo"].get("columnList", [])
            columns = [col["columnName"] for col in column_list if "columnName" in col]
            
            # Create CSV filename by replacing spaces with underscores
            csv_filename = f"{table.replace(' ', '_').replace('/', '_')}.csv"
            
            with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["Column Name"])  # Header
                for column in columns:
                    writer.writerow([column])
            
            print(f"Successfully extracted columns for '{table}' and saved to '{csv_filename}'")
        else:
            print(f"Error: Invalid response structure for table '{table}'")
            
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for table '{table}': {e}")
    except KeyError as e:
        print(f"Error parsing response for table '{table}': {e}")
    except Exception as e:
        print(f"Unexpected error for table '{table}': {e}")