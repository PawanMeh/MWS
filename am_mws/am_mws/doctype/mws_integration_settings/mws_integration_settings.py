# -*- coding: utf-8 -*-
# Copyright (c) 2018, Open eTechnologies and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe.model.document import Document
from amazon_methods import get_products_details, get_orders

class MWSIntegrationSettings(Document):
	def get_products_details(self):
		products = get_products_details()

	def get_order_details(self):
		orders = get_orders(after_date = "2018-07-01",before_date = "2018-12-30")

def schedule_get_order_details():
	orders = get_orders(after_date = "2018-07-01",before_date = "2018-12-30")
