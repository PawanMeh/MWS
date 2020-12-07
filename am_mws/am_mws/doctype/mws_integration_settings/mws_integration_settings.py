# -*- coding: utf-8 -*-
# Copyright (c) 2018, Open eTechnologies and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe.model.document import Document
import dateutil
from amazon_methods import get_products_details, get_orders, get_order_create_invoice, get_order_create_label_jv, auto_submit_mws, get_shipments_details, get_refund_details

class MWSIntegrationSettings(Document):
	def get_products_details(self):
		products = get_products_details()

	def get_order_details(self):
		after_date = dateutil.parser.parse(self.after_date).strftime("%Y-%m-%d")
		orders = get_orders(after_date = after_date)

	def get_order_create_invoice(self):
		after_date = dateutil.parser.parse(self.after_date).strftime("%Y-%m-%d")
		invoices = get_order_create_invoice(after_date = after_date)

	def get_order_create_label_jv(self):
		jvs = get_order_create_label_jv(self.post_after_date)

	def get_shipments(self):
		after_date = dateutil.parser.parse(self.fulfil_after_date).strftime("%Y-%m-%d")
		before_date = dateutil.parser.parse(self.fulfil_before_date).strftime("%Y-%m-%d")
		shipments = get_shipments_details(after_date, before_date)

	def get_refunds(self):
		after_date = dateutil.parser.parse(self.refund_after_date).strftime("%Y-%m-%d")
		before_date = dateutil.parser.parse(self.refund_before_date).strftime("%Y-%m-%d")
		#refunds = get_orders_create_refund(after_date)
		refunds = get_refund_details(before_date, after_date)

def schedule_get_order_details():
	mws_settings = frappe.get_doc("MWS Integration Settings")

	if mws_settings.enable_synch and not mws_settings.import_as_sales_invoice:
		after_date = dateutil.parser.parse(mws_settings.after_date).strftime("%Y-%m-%d")
		orders = get_orders(after_date = after_date)

	if mws_settings.enable_synch and mws_settings.import_as_sales_invoice:
		after_date = dateutil.parser.parse(mws_settings.after_date).strftime("%Y-%m-%d")
		sales_invoices = get_order_create_invoice(after_date = after_date)

	if mws_settings.enable_synch and mws_settings.import_label_jv:
		jvs = get_order_create_label_jv(mws_settings.post_after_date)

	if mws_settings.enable_synch:
		after_date = dateutil.parser.parse(mws_settings.fulfil_after_date).strftime("%Y-%m-%d")
		before_date = dateutil.parser.parse(mws_settings.fulfil_before_date).strftime("%Y-%m-%d")
		shipments = get_shipments_details(after_date, before_date)

def submit_mfn_invoices():
	mws_settings = frappe.get_doc("MWS Integration Settings")
	if mws_settings.auto_submit_mfn_invoices:
		auto_submit_mws()

def update_refund_fulfil_dates():
	update_last_run = frappe.db.sql('''
				update
					`tabSingles`
				set
					value = addtime(now(),'-00:15:00.000000')
				where
					doctype = 'MWS Integration Settings'
					and field in ('refund_before_date', 'fulfil_before_date')
				''')