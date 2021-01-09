# -*- coding: utf-8 -*-
# Copyright (c) 2018, hello@openetech.com and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe.model.document import Document
import frappe, json, time, datetime, dateutil, math, csv, StringIO
from datetime import datetime
from frappe.utils import flt, today
import amazon_mws as mws
from frappe import _
from erpnext.controllers.stock_controller import update_gl_entries_after
from erpnext.stock import get_warehouse_account_map

#Get and Create Products
def get_products_details():
		products = get_products_instance()
		reports = get_reports_instance()

		mws_settings = frappe.get_doc("MWS Integration Settings")
		market_place_list = return_as_list(mws_settings.market_place_id)

		for marketplace in market_place_list:
			report_id = request_and_fetch_report_id("_GET_FLAT_FILE_OPEN_LISTINGS_DATA_", None, None, market_place_list)

			if report_id:
				listings_response = reports.get_report(report_id=report_id)

				string_io = StringIO.StringIO(listings_response.original)
				csv_rows = list(csv.reader(string_io, delimiter=str('\t')))
				asin_list = list(set([row[1] for row in csv_rows[1:]]))
				#break into chunks of 10
				asin_chunked_list = list(chunks(asin_list, 10))

				sku_asin = [{"asin":row[1],"sku":row[0]} for row in csv_rows[1:]]

				for asin_list in asin_chunked_list:
					products_response = call_mws_method(products.get_matching_product,marketplaceid=marketplace,
						asins=asin_list)

					matching_products_list = products_response.parsed 

					for product in matching_products_list:
						skus = [row["sku"] for row in sku_asin if row["asin"]==product.ASIN]
						for sku in skus:
							create_item_code(product, sku)

		return "Success"

def get_products_instance():
	mws_settings = frappe.get_doc("MWS Integration Settings")
	products = mws.Products(
			account_id = mws_settings.seller_id,
			access_key = mws_settings.aws_access_key_id,
			secret_key = mws_settings.secret_key,
			region = mws_settings.region,
			domain = mws_settings.domain
			)

	return products

def get_reports_instance():
	mws_settings = frappe.get_doc("MWS Integration Settings")
	reports = mws.Reports(
			account_id = mws_settings.seller_id,
			access_key = mws_settings.aws_access_key_id,
			secret_key = mws_settings.secret_key,
			region = mws_settings.region,
			domain = mws_settings.domain
	)

	return reports

#amazon list format, list method does not work in integration
def return_as_list(input_value):
	if type(input_value) == list:
		return input_value
	else:
		return [input_value]

def chunks(l, n):
	for i in range(0, len(l), n):
		yield l[i:i+n]

def request_and_fetch_report_id(report_type, start_date=None, end_date=None, marketplaceids=None):
	reports = get_reports_instance()
	report_response = reports.request_report(report_type=report_type, 
			start_date=start_date, 
			end_date=end_date, 
			marketplaceids=marketplaceids)	

	#add time delay to wait for amazon to generate report
	time.sleep(20)
	report_request_id = report_response.parsed["ReportRequestInfo"]["ReportRequestId"]["value"]
	generated_report_id = None
	#poll to get generated report
	for x in range(1,10):
		report_request_list_response = reports.get_report_request_list(requestids=[report_request_id])
		report_status = report_request_list_response.parsed["ReportRequestInfo"]["ReportProcessingStatus"]["value"]

		if report_status == "_SUBMITTED_" or report_status == "_IN_PROGRESS_":
			#add time delay to wait for amazon to generate report
			time.sleep(15)
			continue
		elif report_status == "_CANCELLED_":
			break
		elif report_status == "_DONE_NO_DATA_":
			break
		elif report_status == "_DONE_":
			generated_report_id =  report_request_list_response.parsed["ReportRequestInfo"]["GeneratedReportId"]["value"]
			break

	return generated_report_id

def call_mws_method(mws_method, *args, **kwargs):

	mws_settings = frappe.get_doc("MWS Integration Settings")
	max_retries = mws_settings.max_retry_limit

	for x in xrange(0, max_retries):
		try:
			response = mws_method(*args, **kwargs)
			return response
		except Exception as e:
			delay = math.pow(4, x) * 125
			mws_method_string = str(mws_method)
			frappe.log_error(message=e, title=mws_method_string[:135])
			time.sleep(delay)
			continue

	mws_settings.enable_synch = 0
	mws_settings.save()

	frappe.throw(_("Sync has been temporarily disabled because maximum retries have been exceeded"))

def create_item_code(amazon_item_json, sku):
	if frappe.db.get_value("Item", sku):
		return

	item = frappe.new_doc("Item")

	new_manufacturer = create_manufacturer(amazon_item_json)
	new_brand = create_brand(amazon_item_json)

	mws_settings = frappe.get_doc("MWS Integration Settings")

	item.item_group = mws_settings.item_group
	item.description = amazon_item_json.Product.AttributeSets.ItemAttributes.Title
	item.item_code = sku
	item.market_place_item_code = amazon_item_json.ASIN
	item.brand = new_brand
	item.manufacturer = new_manufacturer
	item.web_long_description = amazon_item_json.Product.AttributeSets.ItemAttributes.Title
	item.image = amazon_item_json.Product.AttributeSets.ItemAttributes.SmallImage.URL

	temp_item_group = amazon_item_json.Product.AttributeSets.ItemAttributes.ProductGroup

	item_group = frappe.db.get_value("Item Group",filters={"item_group_name": temp_item_group})

	if not item_group:
		igroup = frappe.new_doc("Item Group")
		igroup.item_group_name = temp_item_group
		igroup.parent_item_group =  mws_settings.item_group
		igroup.insert()

	item.insert(ignore_permissions=True)
	new_item_price = create_item_price(amazon_item_json, item.item_code)

	return item.name

def create_manufacturer(amazon_item_json):
	existing_manufacturer = frappe.db.get_value("Manufacturer",
		filters={"short_name":amazon_item_json.Product.AttributeSets.ItemAttributes.Manufacturer})

	if not existing_manufacturer:
		manufacturer = frappe.new_doc("Manufacturer")
		manufacturer.short_name = amazon_item_json.Product.AttributeSets.ItemAttributes.Manufacturer
		manufacturer.insert()
		return manufacturer.short_name
	else:
		return existing_manufacturer

def create_brand(amazon_item_json):
	existing_brand = frappe.db.get_value("Brand",
		filters={"brand":amazon_item_json.Product.AttributeSets.ItemAttributes.Brand})
	if not existing_brand:
		brand = frappe.new_doc("Brand")
		brand.brand = amazon_item_json.Product.AttributeSets.ItemAttributes.Brand
		brand.insert()
		return brand.brand
	else:
		return existing_brand

def create_item_price(amazon_item_json, item_code):
	item_price = frappe.new_doc("Item Price")
	item_price.price_list = frappe.db.get_value("MWS Integration Settings", "MWS Integration Settings", "price_list")	
	if not("ListPrice" in amazon_item_json.Product.AttributeSets.ItemAttributes):
		item_price.price_list_rate = 0
	else:
		item_price.price_list_rate = amazon_item_json.Product.AttributeSets.ItemAttributes.ListPrice.Amount

	item_price.item_code = item_code
	item_price.insert()

#Get and create Orders
def get_orders(after_date):
	try:
		orders = get_orders_instance()
		statuses = ["PartiallyShipped", "Unshipped", "Shipped", "Canceled"]
		mws_settings = frappe.get_doc("MWS Integration Settings")
		market_place_list = return_as_list(mws_settings.market_place_id)

		orders_response = call_mws_method(orders.list_orders, marketplaceids=market_place_list, 
			fulfillment_channels=["MFN", "AFN"], 
			lastupdatedafter=after_date,	
			orderstatus=statuses,
			max_results='50')

		while True:
			orders_list = []

			if "Order" in orders_response.parsed.Orders:
				orders_list = return_as_list(orders_response.parsed.Orders.Order)

			if len(orders_list) == 0:
				break

			for order in orders_list:
				create_sales_order(order, after_date)

			if not "NextToken" in orders_response.parsed:
				break

			next_token = orders_response.parsed.NextToken
			orders_response = call_mws_method(orders.list_orders_by_next_token, next_token)

		return "Success"

	except Exception as e:
		frappe.log_error(title="get_orders", message=e)

#Get and create Orders
def get_order_create_invoice(after_date):
	try:
		orders = get_orders_instance()
		statuses = ["PartiallyShipped", "Unshipped", "Shipped", "Canceled"]
		mws_settings = frappe.get_doc("MWS Integration Settings")
		market_place_list = return_as_list(mws_settings.market_place_id)

		orders_response = call_mws_method(orders.list_orders, marketplaceids=market_place_list, 
			fulfillment_channels=["MFN", "AFN"], 
			lastupdatedafter=after_date,	
			orderstatus=statuses,
			max_results='50')

		while True:
			orders_list = []

			if "Order" in orders_response.parsed.Orders:
				orders_list = return_as_list(orders_response.parsed.Orders.Order)

			if len(orders_list) == 0:
				break

			for order in orders_list:
				create_sales_invoice(order, after_date)

			if not "NextToken" in orders_response.parsed:
				break

			next_token = orders_response.parsed.NextToken
			orders_response = call_mws_method(orders.list_orders_by_next_token, next_token)

		return "Success"

	except Exception as e:
		frappe.log_error(title="create_invoice", message=e)


def get_orders_instance():
	mws_settings = frappe.get_doc("MWS Integration Settings")
	orders = mws.Orders(
			account_id = mws_settings.seller_id,
			access_key = mws_settings.aws_access_key_id,
			secret_key = mws_settings.secret_key,
			region= mws_settings.region,
			domain= mws_settings.domain,
			version="2013-09-01"
		)

	return orders

def create_sales_order(order_json,after_date):
	customer_name, contact = create_customer(order_json)
	address = create_address(order_json, customer_name)

	market_place_order_id = order_json.AmazonOrderId
	fulfillment_channel = order_json.FulfillmentChannel

	so = frappe.db.get_value("Sales Order", 
			filters={"market_place_order_id": market_place_order_id},
			fieldname="name")

	taxes_and_charges = frappe.db.get_value("MWS Integration Settings", "MWS Integration Settings", "taxes_charges")

	if so:
		return

	if not so:
		items, mws_items = get_order_items(market_place_order_id, fulfillment_channel)
		delivery_date = dateutil.parser.parse(order_json.LatestShipDate).strftime("%Y-%m-%d")
		transaction_date = dateutil.parser.parse(order_json.PurchaseDate).strftime("%Y-%m-%d")

		so = frappe.get_doc({
				"doctype": "Sales Order",
				"naming_series": "SO-",
				"market_place_order_id": order_json.AmazonOrderId,
				"marketplace_id": order_json.MarketplaceId,
				"customer": customer_name,
				"delivery_date": delivery_date,
				"transaction_date": transaction_date, 
				"items": items,
				"company": frappe.db.get_value("MWS Integration Settings", "MWS Integration Settings", "company")
			})

		try:
			if taxes_and_charges:
				charges_and_fees = get_charges_and_fees(market_place_order_id)
				for charge in charges_and_fees.get("charges"):
					so.append('taxes', charge)

				for fee in charges_and_fees.get("fees"):
					so.append('taxes', fee)

				for tax in charges_and_fees.get("taxwithheld"):
					so.append('taxes', tax)
			#validate items
			total_qty = 0
			for item in so.items:
				total_qty += flt(item.qty)

			if total_qty > 0:
				so.insert(ignore_permissions=True)
				so.submit()

		except Exception as e:
			frappe.log_error(message=e, title="Create Sales Order")

def create_sales_invoice(order_json,after_date):
	customer_name, contact = create_customer(order_json)
	address = create_address(order_json, customer_name)

	market_place_order_id = order_json.AmazonOrderId
	fulfillment_channel = order_json.FulfillmentChannel
	transaction_date = dateutil.parser.parse(order_json.PurchaseDate).strftime("%Y-%m-%d")
	order_status = order_json.OrderStatus

	si = frappe.db.get_value("Sales Invoice", 
			filters={"market_place_order_id": market_place_order_id},
			fieldname="name")

	taxes_and_charges = frappe.db.get_value("MWS Integration Settings", "MWS Integration Settings", "taxes_charges")

	if si:
		return

	if not si:
		items, mws_items = get_order_items(market_place_order_id, fulfillment_channel)
		delivery_date = dateutil.parser.parse(order_json.LatestShipDate).strftime("%Y-%m-%d")
		transaction_date = dateutil.parser.parse(order_json.PurchaseDate).strftime("%Y-%m-%d")

		si_doc = frappe.get_doc({
				"doctype": "Sales Invoice",
				"market_place_order_id": market_place_order_id,
				"naming_series":frappe.db.get_value("MWS Integration Settings", "MWS Integration Settings", "mws_invoice_series"),
				"is_pos": 1,
				"set_posting_time": 1,
				"posting_date": transaction_date,
				"customer": customer_name,
				"delivery_date": delivery_date,
				"transaction_date": transaction_date, 
				"items": items,
				"company": frappe.db.get_value("MWS Integration Settings", "MWS Integration Settings", "company")
			})

		if order_status == "Unshipped" and fulfillment_channel == "MFN":
			si_doc.taxes_and_charges = frappe.db.get_value("MWS Integration Settings", "MWS Integration Settings", "default_tax_template")
		try:
			if taxes_and_charges:
				charges_and_fees = get_charges_and_fees(market_place_order_id)
				for charge in charges_and_fees.get("charges"):
					si_doc.append('taxes', charge)

				for fee in charges_and_fees.get("fees"):
					si_doc.append('taxes', fee)

				for tax in charges_and_fees.get("taxwithheld"):
					si_doc.append('taxes', tax)
				#validate items
				total_qty = 0
				for item in si_doc.items:
					total_qty += flt(item.qty)

				if total_qty > 0:
					#payment info
					si_doc.update_stock = frappe.db.get_value("MWS Integration Settings", "MWS Integration Settings", "update_stock")
					si_doc.save(ignore_permissions=True)
					mode_of_payment = frappe.db.get_value("MWS Integration Settings", "MWS Integration Settings", "mode_of_payment")
					si_doc.append('payments', {"mode_of_payment": mode_of_payment, 
											"amount": si_doc.outstanding_amount, 
											"base_amount":si_doc.outstanding_amount})
					si_doc.paid_amount = si_doc.outstanding_amount
					si_doc.save(ignore_permissions=True)
					if order_status == "Shipped" and total_qty > 0:
						for item in si_doc.items:
							stock_qty = stock_balance(item.warehouse, item.item_code)
							if stock_qty < item.qty:
								frappe.throw(_("Insufficient quantity {0} for item {1} in warehouse {2}").format(item.qty, item.item_code, item.warehouse))
						si_doc.submit()
						si_doc.update_stock_ledger()
						items, warehouses = si_doc.get_items_and_warehouses()
						update_gl_entries_after(si_doc.posting_date, si_doc.posting_time, warehouses, items, company=si_doc.company)

		except Exception as e:
			frappe.log_error(message=e, title="Create Sales Invoice" + " for Order ID " + market_place_order_id)

def create_customer(order_json):
	order_customer_name = ""

	if not("BuyerName" in order_json):
		order_customer_name = "Buyer - " + order_json.AmazonOrderId
	else:
		order_customer_name = order_json.BuyerName

	existing_customer_name = frappe.db.get_value("Customer", 
			filters={"name": order_customer_name}, fieldname="name")

	if existing_customer_name:
		filters = [
				["Dynamic Link", "link_doctype", "=", "Customer"],
				["Dynamic Link", "link_name", "=", existing_customer_name],
				["Dynamic Link", "parenttype", "=", "Contact"]
			]

		existing_contacts = frappe.get_list("Contact", filters)

		if existing_contacts:
			existing_contact_name = existing_contacts[0].name
		else:
			new_contact = frappe.new_doc("Contact")
			new_contact.first_name = order_customer_name
			new_contact.append('links', {
				"link_doctype": "Customer",
				"link_name": existing_customer_name
			})
			new_contact.insert()
			existing_contact_name = new_contact.first_name

		return existing_customer_name, existing_contact_name
	else:
		mws_customer_settings = frappe.get_doc("MWS Integration Settings")
		new_customer = frappe.new_doc("Customer")		
		new_customer.customer_name = order_customer_name
		new_customer.customer_group = mws_customer_settings.customer_group
		new_customer.territory = mws_customer_settings.territory
		new_customer.customer_type = mws_customer_settings.customer_type
		new_customer.save()

		new_contact = frappe.new_doc("Contact")
		new_contact.first_name = order_customer_name
		new_contact.append('links', {
			"link_doctype": "Customer",
			"link_name": new_customer.name
		})

		new_contact.insert()

		return new_customer.name, new_contact.name

def create_address(amazon_order_item_json, customer_name):

	filters = [
			["Dynamic Link", "link_doctype", "=", "Customer"],
			["Dynamic Link", "link_name", "=", customer_name],
			["Dynamic Link", "parenttype", "=", "Address"]
		]

	existing_address = frappe.get_list("Address", filters)

	if not("ShippingAddress" in amazon_order_item_json):
		return None
	else:
		make_address = frappe.new_doc("Address")

		if "AddressLine1" in amazon_order_item_json.ShippingAddress:
			make_address.address_line1 = amazon_order_item_json.ShippingAddress.AddressLine1
		else:
			make_address.address_line1 = "Not Provided"

		if "City" in amazon_order_item_json.ShippingAddress:
			make_address.city = amazon_order_item_json.ShippingAddress.City
		else:
			make_address.city = "Not Provided"

		if "StateOrRegion" in amazon_order_item_json.ShippingAddress:
			make_address.state = amazon_order_item_json.ShippingAddress.StateOrRegion

		if "PostalCode" in amazon_order_item_json.ShippingAddress:
			make_address.pincode = amazon_order_item_json.ShippingAddress.PostalCode

		for address in existing_address:
			address_doc = frappe.get_doc("Address", address["name"])
			if (address_doc.address_line1 == make_address.address_line1 and 
				address_doc.pincode == make_address.pincode):
				return address

		make_address.append("links", {
			"link_doctype": "Customer",
			"link_name": customer_name
		})
		make_address.address_type = "Shipping"
		make_address.insert()
		return make_address

def get_order_items(market_place_order_id, fulfillment_channel):
	mws_orders = get_orders_instance()

	order_items_response = call_mws_method(mws_orders.list_order_items, amazon_order_id=market_place_order_id)
	final_order_items = []

	order_items_list = return_as_list(order_items_response.parsed.OrderItems.OrderItem)
	order_items_mws = order_items_list

	def_warehouse = frappe.db.get_value("MWS Integration Settings", "MWS Integration Settings", "warehouse")
	mfn_warehouse = frappe.db.get_value("MWS Integration Settings", "MWS Integration Settings", "mfn_warehouse")

	while True:
		for order_item in order_items_list:

			if not "ItemPrice" in order_item:
				price = 0
			else:
				price = order_item.ItemPrice.Amount

			item_code = get_item_code(order_item)
			item_values = frappe.db.get_value("Item", {"item_code": item_code}, ["seller_fulfilled_item", "default_warehouse"])
			if item_values[0] and item_values[1]:
				warehouse = item_values[1]
			else:
				warehouse = def_warehouse
			if fulfillment_channel == "MFN":
				warehouse = mfn_warehouse

			final_order_items.append({
				"item_code": item_code,
				"item_name": order_item.SellerSKU,
				"description": order_item.Title,
				"rate": price,
				"qty": order_item.QuantityOrdered,
				"stock_uom": "Each",
				"warehouse": warehouse,
				"conversion_factor": "1.0"
			})

		if not "NextToken" in order_items_response.parsed:
			break

		next_token = order_items_response.parsed.NextToken

		order_items_response = call_mws_method(mws_orders.list_order_items_by_next_token, next_token)
		order_items_list = return_as_list(order_items_response.parsed.OrderItems.OrderItem)
		order_items_mws += order_items_list 

	return final_order_items, order_items_mws

def get_item_code(order_item):
	asin = order_item.ASIN
	sku = order_item.SellerSKU
	item_code = frappe.db.get_value("Item", {"item_code": sku}, "item_code")
	if item_code:
		return item_code
	else:
		item = frappe.new_doc("Item")
		mws_settings = frappe.get_doc("MWS Integration Settings")

		item.item_group = mws_settings.item_group
		item.description = sku
		item.item_code = sku
		item.market_place_item_code = asin
		item.insert(ignore_permissions=True)

		return item.item_code

def get_charges_and_fees(market_place_order_id):
	finances = get_finances_instance()

	charges_fees = {"charges":[], "fees":[], "taxwithheld": []}

	response = call_mws_method(finances.list_financial_events, amazon_order_id=market_place_order_id)

	shipment_event_list = return_as_list(response.parsed.FinancialEvents.ShipmentEventList)
	
	for shipment_event in shipment_event_list:
		if shipment_event:
			if 'ShipmentItemList' in shipment_event.keys():
				shipment_item_list = return_as_list(shipment_event.ShipmentEvent.ShipmentItemList.ShipmentItem)

				for shipment_item in shipment_item_list:
					if 'ItemChargeList' in shipment_item.keys():
						charges = return_as_list(shipment_item.ItemChargeList.ChargeComponent)
					else:
						charges = []

					if 'ItemFeeList' in shipment_item.keys():
						fees = return_as_list(shipment_item.ItemFeeList.FeeComponent)
					else:
						fees = []

					if 'ItemTaxWithheldList' in shipment_item.keys():
						taxes_witheld = return_as_list(shipment_item.ItemTaxWithheldList.TaxWithheldComponent.TaxesWithheld.ChargeComponent)
					else:
						taxes_witheld = []

					for charge in charges:
						if(charge.ChargeType != "Principal") and float(charge.ChargeAmount.CurrencyAmount) != 0:
							charge_account = get_account(charge.ChargeType)
							charges_fees.get("charges").append({
								"charge_type":"Actual",
								"account_head": charge_account,
								"tax_amount": charge.ChargeAmount.CurrencyAmount,
								"description": charge.ChargeType + " for " + shipment_item.SellerSKU
							})

					for fee in fees:
						if float(fee.FeeAmount.CurrencyAmount) != 0:
							fee_account = get_account(fee.FeeType)
							charges_fees.get("fees").append({
								"charge_type":"Actual",
								"account_head": fee_account,
								"tax_amount": fee.FeeAmount.CurrencyAmount,
								"description": fee.FeeType + " for " + shipment_item.SellerSKU
							})
					#marketplace facilitator tax
					for tax in taxes_witheld:
						if(tax.ChargeType == "MarketplaceFacilitatorTax-Principal"):
							mws_settings = frappe.get_doc("MWS Integration Settings")
							tax_account = mws_settings.market_place_tax_account
							charges_fees.get("taxwithheld").append({
								"charge_type":"Actual",
								"account_head": tax_account,
								"tax_amount": tax.ChargeAmount.CurrencyAmount,
								"description": tax.ChargeType + " for " + shipment_item.SellerSKU
							})

	return charges_fees

def get_orders_create_refund(after_date):
	try:
		orders = get_orders_instance()
		statuses = ["PartiallyShipped", "Unshipped", "Shipped", "Canceled"]
		mws_settings = frappe.get_doc("MWS Integration Settings")
		market_place_list = return_as_list(mws_settings.market_place_id)

		orders_response = call_mws_method(orders.list_orders, marketplaceids=market_place_list, 
			fulfillment_channels=["MFN", "AFN"], 
			lastupdatedafter=after_date,	
			orderstatus=statuses,
			max_results='50')

		while True:
			orders_list = []

			if "Order" in orders_response.parsed.Orders:
				orders_list = return_as_list(orders_response.parsed.Orders.Order)

			if len(orders_list) == 0:
				break

			for order in orders_list:
				amazon_order_id = order.AmazonOrderId
				get_refund_details(amazon_order_id)

			if not "NextToken" in orders_response.parsed:
				break

			next_token = orders_response.parsed.NextToken
			orders_response = call_mws_method(orders.list_orders_by_next_token, next_token)

		return "Success"

	except Exception as e:
		frappe.log_error(title="create_refund" + "-" + amazon_order_id, message=e)

def get_refund_details(before_date, after_date):
	finances = get_finances_instance()
	response = call_mws_method(finances.list_financial_events, posted_after=after_date, posted_before=before_date)
	shipment_event_list = return_as_list(response.parsed.FinancialEvents.RefundEventList)

	#ret wh
	mws_settings = frappe.get_doc("MWS Integration Settings")
	ret_wh = mws_settings.return_warehouse

	if len(shipment_event_list) != 0:
		for shipment_event in shipment_event_list:
			if 'ShipmentEvent' in shipment_event.keys():
				events = return_as_list(shipment_event.ShipmentEvent)
				for event in events:
				#market_place_order_id = shipment_event.ShipmentEvent.SellerOrderId
				#date_str = shipment_event.ShipmentEvent.PostedDate
					if 'SellerOrderId' in event.keys():
						market_place_order_id = event.SellerOrderId
						date_str = event.PostedDate
						customer = frappe.db.sql('''
										select
											customer
										from
											`tabSales Invoice`
										where
											docstatus = 1 and is_return = 0
											and market_place_order_id = %s
									''', (market_place_order_id))
						if customer:
							posting_date = datetime.strptime(date_str[0:10], '%Y-%m-%d')
							se_args = {
								"company" : mws_settings.company,
								"naming_series" : "AMZ-CR-",
								"posting_date" : posting_date,
								"customer": customer[0][0],
								"set_posting_time":1,
								"market_place_order_id": market_place_order_id,
								"items" : [],
								"taxes" : []
							}
							create_return = False
							if shipment_event:
								#shipment_item_list = return_as_list(shipment_event.ShipmentEvent.ShipmentItemAdjustmentList.ShipmentItem)
								shipment_item_list = return_as_list(event.ShipmentItemAdjustmentList.ShipmentItem)
								for shipment_item in shipment_item_list:
									if 'ItemChargeAdjustmentList' in shipment_item.keys():
										charges = return_as_list(shipment_item.ItemChargeAdjustmentList.ChargeComponent)
									else:
										charges = []

									if 'ItemFeeAdjustmentList' in shipment_item.keys():
										fees = return_as_list(shipment_item.ItemFeeAdjustmentList.FeeComponent)
									else:
										fees = []

									if 'ItemTaxWithheldList' in shipment_item.keys():
										taxes_witheld = return_as_list(shipment_item.ItemTaxWithheldList.TaxWithheldComponent.TaxesWithheld.ChargeComponent)
									else:
										taxes_witheld = []

									if 'PromotionAdjustmentList' in shipment_item.keys():
										promotions_adj = return_as_list(shipment_item.PromotionAdjustmentList.Promotion)
									else:
										promotions_adj = []

									for charge in charges:
										if(charge.ChargeType != "Principal") and float(charge.ChargeAmount.CurrencyAmount) != 0:
											charge_account = get_account(charge.ChargeType)
											se_args['taxes'].append({
												"charge_type":"Actual",
												"account_head": charge_account,
												"tax_amount": charge.ChargeAmount.CurrencyAmount or 0,
												"description": charge.ChargeType + " for " + shipment_item.SellerSKU
											})
										if charge.ChargeType == "Principal":
											create_return = True
										

									for fee in fees:
										if float(fee.FeeAmount.CurrencyAmount) != 0:
											fee_account = get_account(fee.FeeType)
											se_args['taxes'].append({
												"charge_type":"Actual",
												"account_head": fee_account,
												"tax_amount": fee.FeeAmount.CurrencyAmount or 0,
												"description": fee.FeeType + " for " + shipment_item.SellerSKU
											})

									for tax in taxes_witheld:
										if((tax.ChargeType == "MarketplaceFacilitatorTax-Shipping")
											or (tax.ChargeType == "MarketplaceFacilitatorTax-Principal")):
											mws_settings = frappe.get_doc("MWS Integration Settings")
											tax_account = mws_settings.market_place_tax_account
											se_args['taxes'].append({
												"charge_type":"Actual",
												"account_head": tax_account,
												"tax_amount": tax.ChargeAmount.CurrencyAmount or 0,
												"description": tax.ChargeType + " for " + shipment_item.SellerSKU
											})

									for promotion in promotions_adj:
										if(promotion.PromotionType == "PromotionMetaDataDefinitionValue"):
											charge_account = get_account(promotion.PromotionType)
											se_args['taxes'].append({
												"charge_type":"Actual",
												"account_head": charge_account,
												"tax_amount": promotion.PromotionAmount.CurrencyAmount or 0,
												"description": promotion.PromotionType + " for " + shipment_item.SellerSKU
											})

									invoices =	frappe.db.sql('''
														select
															sum(b.qty) as qty, sum(b.amount) as amount
														from
															`tabSales Invoice` a, `tabSales Invoice Item` b
														where 
															a.name = b.parent and
															a.market_place_order_id = %s and
															b.item_code = %s
													''', (market_place_order_id, shipment_item.SellerSKU), as_dict=1)
									for invoice in invoices:
										se_args['items'].append({
											"item_code": shipment_item.SellerSKU,
											"item_name": shipment_item.SellerSKU,
											"description": shipment_item.SellerSKU,
											"rate": invoice['amount']  or 0,
											"qty": invoice['qty'] * -1,
											"stock_uom": "Each",
											"warehouse": ret_wh,
											"conversion_factor": "1.0"
										})
								if create_return:
									create_return_invoice(se_args)
								else:
									create_return_jv(se_args)
						else:
							frappe.log_error(message="Corresponding Sales Invoice does not exist", 
								title="Credit Invoice Error" + market_place_order_id)

def create_return_jv(se_args):
	company = frappe.db.get_value("MWS Integration Settings", "MWS Integration Settings", "company")
	mode_of_payment = frappe.db.get_value("MWS Integration Settings", "MWS Integration Settings", "mode_of_payment")
	args = frappe._dict(se_args)
	if frappe.db.exists({'doctype': 'Journal Entry','cheque_no': args.market_place_order_id}):
		pass
	else:
		#credit mop
		#debit charges account
		credit_account = frappe.db.sql('''
							select
								default_account
							from
								`tabMode of Payment Account`
							where
								company = %s and
								parent = %s
							''', (company, mode_of_payment))
		#create jv
		je_doc = frappe.new_doc("Journal Entry")
		je_doc.company = company
		je_doc.voucher_type = "Journal Entry"
		je_doc.cheque_no = args.market_place_order_id
		je_doc.posting_date = args.posting_date
		je_doc.cheque_date = args.posting_date
		cost_center = "Main - SHM"
		tot_amount = 0
		for charge in args.taxes:
			if flt(charge['tax_amount']) < 0:
				je_doc.append("accounts", {
					"account": charge['account_head'],
					"cost_center": cost_center,
					"debit_in_account_currency": flt(charge['tax_amount']) * -1,
					"debit": flt(charge['tax_amount']) * -1,
					"credit_in_account_currency": 0,
					"credit": 0
				})
			else:
				je_doc.append("accounts", {
					"account": charge['account_head'],
					"cost_center": cost_center,
					"debit_in_account_currency": 0,
					"debit": 0,
					"credit_in_account_currency": flt(charge['tax_amount']),
					"credit": tot_amount
				})
		try:
			je_doc.insert(ignore_permissions=True)
			if je_doc.total_debit == 0 and je_doc.total_credit == 0:
				frappe.delete_doc('Journal Entry', je_doc.name)
			else:
				return je_doc.name
		except Exception as e:
			frappe.log_error(message=e, title="Return JV Error" + je_doc.cheque_no + je_doc.posting_date.strftime('%Y-%m-%d'))


def get_order_create_label_jv(after_date):
	warehouse = frappe.db.get_value("MWS Integration Settings", "MWS Integration Settings", "mfn_warehouse")
	orders = frappe.db.sql('''
				select
					a.name, a.market_place_order_id, a.posting_date
				from
					`tabSales Invoice` a, `tabSales Invoice Item` b
				where
					a.name = b.parent and
					a.docstatus = 1 and
					b.warehouse = %s and
					a.posting_date >= %s and
					a.market_place_order_id IS NOT NULL
					and a.market_place_order_id not in (select cheque_no from `tabJournal Entry` where cheque_no IS NOT NULL)
					AND a.naming_series = 'AMZ-' LIMIT 50
				''', (warehouse, after_date), as_dict=1)
	for order in orders:
		order_id = order['market_place_order_id']
		if order_id.endswith('-refund'):
			order_id = order_id[:7]
		elif order_id.endswith('-1'):
			order_id = order_id[:2]
		fees_dict = get_postal_fees(order_id)
		jv_no = create_jv(order['market_place_order_id'], order['posting_date'], fees_dict.get('fees') * -1)

def create_jv(market_place_order_id, transaction_date, fees):
	company = frappe.db.get_value("MWS Integration Settings", "MWS Integration Settings", "company")
	credit_account = frappe.db.get_value("MWS Integration Settings", "MWS Integration Settings", "shipping_label_credit_account")
	debit_account = frappe.db.get_value("MWS Integration Settings", "MWS Integration Settings", "shipping_label_debit_account")
	je_doc = frappe.new_doc("Journal Entry")
	je_doc.company = company
	je_doc.voucher_type = "Journal Entry"
	je_doc.cheque_no = market_place_order_id
	je_doc.posting_date = transaction_date
	je_doc.cheque_date = transaction_date
	cost_center = "Main - SHM"
	je_doc.append("accounts", {
		"account": credit_account,
		"cost_center": cost_center,
		"debit_in_account_currency": 0,
		"debit": 0,
		"credit_in_account_currency": fees,
		"credit": fees
	})
	je_doc.append("accounts", {
		"account": debit_account,
		"cost_center": cost_center,
		"debit_in_account_currency": fees,
		"debit": fees,
		"credit_in_account_currency": 0,
		"credit": 0
	})
	try:
		je_doc.insert(ignore_permissions=True)
		if je_doc.total_debit == 0 and je_doc.total_credit == 0:
			frappe.delete_doc('Journal Entry', je_doc.name)
		else:
			return je_doc.name
	except Exception as e:
		frappe.log_error(message=e, title="JV Error" + je_doc.cheque_no + je_doc.posting_date.strftime('%Y-%m-%d'))

def get_postal_fees(market_place_order_id):
	finances = get_finances_instance()
	response = call_mws_method(finances.list_financial_events, amazon_order_id=market_place_order_id)
	adjustment_events = return_as_list(response.parsed.FinancialEvents.AdjustmentEventList)
	for adjustment_event in adjustment_events:
		total_fees = 0
		if adjustment_event:
			adjustment_event_list = return_as_list(adjustment_event.AdjustmentEvent)
			for adjustment in adjustment_event_list:
				if 'AdjustmentType' in adjustment.keys():
					#if (adjustment.AdjustmentType == "PostageBilling_Postage" or adjustment.AdjustmentType == "PostageBilling_SignatureConfirmation"):
					total_fees += flt(adjustment.AdjustmentAmount.CurrencyAmount)
				else:
					return {'fees': flt(total_fees)}
							
	return {'fees': flt(total_fees)}

def get_finances_instance():

	mws_settings = frappe.get_doc("MWS Integration Settings")

	finances = mws.Finances(
			account_id = mws_settings.seller_id,
			access_key = mws_settings.aws_access_key_id,
			secret_key = mws_settings.secret_key,
			region= mws_settings.region,
			domain= mws_settings.domain,
			version="2015-05-01"
		)

	return finances

def get_shipments_instance():

	mws_settings = frappe.get_doc("MWS Integration Settings")

	shipments = mws.Fulfilment(
			account_id = mws_settings.seller_id,
			access_key = mws_settings.aws_access_key_id,
			secret_key = mws_settings.secret_key,
			region= mws_settings.region,
			domain= mws_settings.domain,
			version="2010-10-01"
		)

	return shipments

def get_shipments_details(after_date, before_date):
	mws_settings = frappe.get_doc("MWS Integration Settings")
	
	shipments = get_shipments_instance()
	status_list = ["SHIPPED","RECEIVING","IN_TRANSIT","DELIVERED","CHECKED_IN","CLOSED"] #removed status "WORKING" as no line items are returned from MWS
	response = call_mws_method(shipments.list_inbound_shipments, posted_after=after_date, posted_before=before_date, statuses = status_list)
	shipment_events = return_as_list(response.parsed.ShipmentData)
	for shipment in shipment_events:
		shipment_list = return_as_list(shipment.member)
		for member in shipment_list:
			pin_code = member.ShipFromAddress.PostalCode
			frm_wh = get_warehouse(member.ShipFromAddress.PostalCode)
			shipment_id = member.ShipmentId
			#check if shipment ID already exists
			frappe.msgprint("Shipment ID(s) {1}".format(shipment_id))
			shipment_se = frappe.db.sql('''
							select
								'X'
							from
								`tabStock Entry`
							where
								shipment_id = %s
							''', (shipment_id), as_list=1)
			if shipment_se:
				pass
			else:
				date_str = member.ShipmentName
				s_date = date_str[5:22].split(",")
				posting_date = datetime.strptime(s_date[0], '%m/%d/%y')
				if frm_wh:
					se_args = {
						"company" : mws_settings.company,
						"naming_series" : "MAT-STE-.YYYY.-",
						"purpose" : "Material Transfer",
						"shipment_id" : shipment_id,
						"posting_date" : posting_date,
						"from_warehouse": frm_wh,
						"to_warehouse": mws_settings.target_warehouse,
						"items" : [],
						"additional_costs" : []
					}
					response = call_mws_method(shipments.list_shipment_details, shipment_id=shipment_id)
					item_details = return_as_list(response.parsed.ItemData)
					for item in item_details:
						item_list = return_as_list(item.member)
						for item_member in item_list:
							se_args['items'].append({
								's_warehouse': frm_wh,
								't_warehouse': mws_settings.target_warehouse,
								'item_code': item_member.SellerSKU,
								'qty': item_member.QuantityShipped,
								'uom': 'Unit'
							})
					response = call_mws_method(shipments.list_transport_details, shipment_id=shipment_id)
					transport_details = return_as_list(response.parsed.TransportContent)
					for detail in transport_details:
						ship_type_descr = ""
						if detail.TransportHeader.ShipmentType == "SP":
							ship_type_descr = "Small Parcel"
						elif detail.TransportHeader.ShipmentType == "LTL":
							ship_type_descr = "Less Than Truckload"
						elif detail.TransportHeader.ShipmentType == "FTL":
							ship_type_descr = "Full Truckload"
						tdetails = return_as_list(detail.TransportDetails)
						for td in tdetails:
							if 'PartneredSmallParcelData' in td.keys():
								parcel_details = return_as_list(td.PartneredSmallParcelData)
								amount = 0
								for d in parcel_details:
									if 'PartneredEstimate' in td.keys():
										amount += flt(d.PartneredEstimate.Amount.Value)
								se_args['additional_costs'].append({
									'description': ship_type_descr,
									'amount': amount
								})
					create_stock_entry(se_args)
				else:
					frappe.msgprint("No Warehouse found for pin code {0} for Shipment ID {1}".format(pin_code, shipment_id))

def create_shipment_se(shipment_events):
	mws_settings = frappe.get_doc("MWS Integration Settings")
	for shipment in shipment_events:
		if "member" in shipment.keys():
			shipment_list = return_as_list(shipment.member)
			for member in shipment_list:
				pin_code = member.ShipFromAddress.PostalCode
				frm_wh = get_warehouse(member.ShipFromAddress.PostalCode)
				shipment_id = member.ShipmentId
				#check if shipment ID already exists
				shipment_se = frappe.db.sql('''
								select
									'X'
								from
									`tabStock Entry`
								where
									shipment_id = %s
								''', (shipment_id), as_list=1)
				if shipment_se:
					pass
				else:
					date_str = member.ShipmentName
					s_date = date_str[5:22].split(",")
					posting_date = datetime.strptime(s_date[0], '%m/%d/%y')
					if frm_wh:
						se_args = {
							"company" : mws_settings.company,
							"naming_series" : "MAT-STE-.YYYY.-",
							"purpose" : "Material Transfer",
							"shipment_id" : shipment_id,
							"posting_date" : posting_date,
							"from_warehouse": frm_wh,
							"to_warehouse": mws_settings.target_warehouse,
							"items" : [],
							"additional_costs" : []
						}
						shipments = get_shipments_instance()
						response = call_mws_method(shipments.list_shipment_details, shipment_id=shipment_id)
						item_details = return_as_list(response.parsed.ItemData)
						for item in item_details:
							item_list = return_as_list(item.member)
							for item_member in item_list:
								se_args['items'].append({
									's_warehouse': frm_wh,
									't_warehouse': mws_settings.target_warehouse,
									'item_code': item_member.SellerSKU,
									'qty': item_member.QuantityShipped,
									'uom': 'Unit'
								})
						response = call_mws_method(shipments.list_transport_details, shipment_id=shipment_id)
						transport_details = return_as_list(response.parsed.TransportContent)
						for detail in transport_details:
							ship_type_descr = ""
							if detail.TransportHeader.ShipmentType == "SP":
								ship_type_descr = "Small Parcel"
							elif detail.TransportHeader.ShipmentType == "LTL":
								ship_type_descr = "Less Than Truckload"
							elif detail.TransportHeader.ShipmentType == "FTL":
								ship_type_descr = "Full Truckload"
							tdetails = return_as_list(detail.TransportDetails)
							for td in tdetails:
								if 'PartneredSmallParcelData' in td.keys():
									parcel_details = return_as_list(td.PartneredSmallParcelData)
									amount = 0
									for d in parcel_details:
										if 'PartneredEstimate' in td.keys():
											amount += flt(d.PartneredEstimate.Amount.Value)
									se_args['additional_costs'].append({
										'description': ship_type_descr,
										'amount': amount
									})
						create_stock_entry(se_args)
					else:
						frappe.msgprint("No Warehouse found for pin code {0} for Shipment ID {1}".format(pin_code, shipment_id))

def get_in_shipments(after_date, before_date):
	mws_settings = frappe.get_doc("MWS Integration Settings")
	shipments = get_shipments_instance()
	status_list = ["SHIPPED","RECEIVING","IN_TRANSIT","DELIVERED","CHECKED_IN","CLOSED"]
	response = call_mws_method(shipments.list_inbound_shipments, posted_after=after_date, posted_before=before_date, statuses = status_list)

	while True:
		shipment_events = []
		shipment_events = return_as_list(response.parsed.ShipmentData)

		if len(shipment_events) == 0:
			break

		create_shipment_se(shipment_events)

		if not "NextToken" in response.parsed:
			break

		next_token = response.parsed.NextToken
		response = call_mws_method(shipments.list_inbound_shipments_by_next_token, next_token)

	return "Success"

def get_account(name):
	existing_account = frappe.db.get_value("Account", {"account_name": "Amazon {0}".format(name)})
	account_name = existing_account
	mws_settings = frappe.get_doc("MWS Integration Settings")

	if not existing_account:
		try:
			new_account = frappe.new_doc("Account")
			new_account.account_name = "Amazon {0}".format(name)
			new_account.company = mws_settings.company
			new_account.parent_account = mws_settings.market_place_account_group
			new_account.insert(ignore_permissions=True)
			account_name = new_account.name
		except Exception as e:
			frappe.log_error(message=e, title="Create Account")

	return account_name

def get_warehouse(pin_code):
	frm_wh = frappe.db.sql('''
				select
					b.link_name 
				from 
					`tabAddress` a, `tabDynamic Link` b 
				where
					a.name = b.parent and 
					b.parenttype = 'Address' and 
					b.link_doctype = 'Warehouse' and 
					a.pincode = %s
				''',(pin_code), as_list=1)
	if frm_wh:
		return frm_wh[0][0]

def create_stock_entry(args):
	mws_settings = frappe.get_doc("MWS Integration Settings")
	args = frappe._dict(args)
	se = frappe.get_doc({
		"doctype": "Stock Entry",
		"naming_series": args.naming_series,
		"purpose":args.purpose,
		"shipment_id": args.shipment_id,
		"posting_date": args.posting_date,
		"from_warehouse": args.from_warehouse,
		"to_warehouse": args.to_warehouse,
		"set_posting_time": 1,
		"items": args["items"],
		"additional_costs": args["additional_costs"]
	})

	try:
		if frappe.db.exists({'doctype': 'Stock Entry','shipment_id': args.shipment_id}):
			pass
			#frappe.log_error(message="Unique Shipment ID check", title="Create Stock Entry: " + args.get("shipment_id") + "already exists")
		else:
			se.set_missing_values()
			se.insert(ignore_mandatory=True)
			if mws_settings.submit_stock_entry:
				se.submit()

	except Exception as e:
		frappe.log_error(message=e,
			title="Create Stock Entry: " + args.get("shipment_id"))
		return None

def create_return_invoice(args):
	mws_settings = frappe.get_doc("MWS Integration Settings")
	args = frappe._dict(args)
	order_id = args.market_place_order_id + "-RET"
	se = frappe.get_doc({
		"doctype": "Sales Invoice",
		"company": mws_settings.company,
		"naming_series": args.naming_series,
		"customer":args.customer,
		"market_place_order_id": order_id,
		"posting_date": args.posting_date,
		"due_date": args.posting_date,
		"set_posting_time": 1,
		"selling_price_list": "Standard Selling",
		"is_return": 1,
		"is_pos": 1,
		"pos_profile": "Amazon FBA",
		"items": args["items"],
		"taxes": args["taxes"]
	})

	try:
		if frappe.db.exists({'doctype': 'Sales Invoice','market_place_order_id': order_id}):
			pass
			#frappe.log_error(message="Unique Credit Invoice check", title="Create Sales Invoice: " + market_place_order_id + "already exists")
		else:
			se.set_missing_values()
			se.insert(ignore_mandatory=True, ignore_permissions=True)
			mode_of_payment = frappe.db.get_value("MWS Integration Settings", "MWS Integration Settings", "mode_of_payment")
			se.payments = []
			se.append('payments', {"mode_of_payment": mode_of_payment,"amount": se.grand_total,"base_amount":se.grand_total})
			se.paid_amount = se.grand_total
			se.outstanding_amount = 0
			se.write_off_amount = 0
			se.save(ignore_permissions=True)
			if mws_settings.submit_credit_invoice:
				se.submit()

	except Exception as e:
		frappe.log_error(message=e,
			title="Create Sales Return Invoice: " + order_id)
		return None

def auto_submit_mws():
	company = frappe.db.get_value("MWS Integration Settings", "MWS Integration Settings", "company")
	warehouse = frappe.db.get_value("MWS Integration Settings", "MWS Integration Settings", "mfn_warehouse")
	invoices = frappe.db.sql('''
					select 
						distinct a.name
					from
						`tabSales Invoice` a, `tabSales Invoice Item` b
					where
						a.company = %s and a.name = b.parent and a.docstatus = 0 and
						b.warehouse = %s and
						a.market_place_order_id IS NOT NULL and
						a.naming_series = 'AMZ-'
					''', (company, warehouse), as_dict=1)

	for invoice in invoices:
		si_doc = frappe.get_doc('Sales Invoice', invoice['name'])
		insufficient_stock = False
		for item in si_doc.items:
			stock_qty = stock_balance(item.warehouse, item.item_code)
			if stock_qty < item.qty and item.qty > 0:
				insufficient_stock = True
		if insufficient_stock:
			msg = "Insufficient quantity {0} for item {1} in warehouse {2} for invoice {3}".format(item.qty, item.item_code, item.warehouse, si_doc.name)
			frappe.log_error(message = msg, title = "Quantity Error")
		else:
			si_doc.submit()
			si_doc.update_stock_ledger()
			items, warehouses = si_doc.get_items_and_warehouses()
			update_gl_entries_after(si_doc.posting_date, si_doc.posting_time, warehouses, items, company=si_doc.company)

def stock_balance(warehouse, item_code):
	stock_bal = frappe.db.sql('''
					select
						actual_qty
					from
						`tabBin`
					where
						warehouse = %s and
						item_code = %s''', (warehouse, item_code))
	if stock_bal:
		return stock_bal[0][0]
	else:
		return 0
