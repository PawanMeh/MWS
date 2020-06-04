# -*- coding: utf-8 -*-
# Copyright (c) 2018, hello@openetech.com and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe.model.document import Document
import frappe, json, time, datetime, dateutil, math, csv, StringIO
from frappe.utils import flt
import amazon_mws as mws
from frappe import _

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
						si_doc.docstatus = 1
						si_doc.save()

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

def get_order_create_label_jv(after_date):	
	orders = frappe.db.sql('''
				select
					name, market_place_order_id, transaction_date
				from
					`tabSales Order`
				where
					transaction_date >= %s and
					market_place_order_id IS NOT NULL
					and market_place_order_id not in (select cheque_no from `tabJournal Entry` where cheque_no IS NOT NULL) LIMIT 30
				''', (after_date), as_dict=1)
	for order in orders:
		fees_dict = get_postal_fees(order['market_place_order_id'])
		jv_no = create_jv(order['market_place_order_id'], order['transaction_date'], fees_dict.get('fees') * -1)

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
		frappe.log_error(message=e, title="JV Error" + je_doc.cheque_no + je_doc.posting_date)

def get_postal_fees(market_place_order_id):
	finances = get_finances_instance()
	response = call_mws_method(finances.list_financial_events, amazon_order_id=market_place_order_id)
	adjustment_events = return_as_list(response.parsed.FinancialEvents.AdjustmentEventList)

	total_fees = 0
	for adjustment_event in adjustment_events:
		if adjustment_event:
			adjustment_event_list = return_as_list(adjustment_event.AdjustmentEvent)
			for adjustment in adjustment_event_list:
				if 'AdjustmentType' in adjustment.keys():
					if (adjustment.AdjustmentType == "PostageBilling_Postage" or adjustment.AdjustmentType == "PostageBilling_SignatureConfirmation"):
						total_fees += flt(adjustment.AdjustmentAmount.CurrencyAmount)

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

def auto_submit_mws():
	company = frappe.db.get_value("MWS Integration Settings", "MWS Integration Settings", "company")
	warehouse = frappe.db.get_value("MWS Integration Settings", "MWS Integration Settings", "mfn_warehouse")
	invoices = frappe.db.sql('''
					select 
						distinct a.name
					from
						`tabSales Invoice` a, `tabSales Invoice Item` b
					where
						a.company = %s and
						a.name = b.parent and
						a.docstatus = 0 and
						b.warehouse = %s and
						a.market_place_order_id IS NOT NULL
					''', (company, warehouse), as_dict=1)

	for invoice in invoices:
		si_doc = frappe.get_doc('Sales Invoice', invoice['name'])
		for item in si_doc.items:
			stock_qty = stock_balance(item.warehouse, item.item_code)
			if stock_qty < item.qty and item.qty > 0:
				frappe.throw(_("Insufficient quantity {0} for item {1} in warehouse {2}").format(item.qty, item.item_code, item.warehouse))
		si_doc.docstatus = 1
		si_doc.save()

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