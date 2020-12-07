# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from . import __version__ as app_version

app_name = "am_mws"
app_title = "Am Mws"
app_publisher = "Open eTechnologies"
app_description = "MWS connector"
app_icon = "octicon octicon-file-directory"
app_color = "grey"
app_email = "hello@openetech.com"
app_license = "MIT"

# Includes in <head>
# ------------------

# include js, css files in header of desk.html
# app_include_css = "/assets/am_mws/css/am_mws.css"
# app_include_js = "/assets/am_mws/js/am_mws.js"

# include js, css files in header of web template
# web_include_css = "/assets/am_mws/css/am_mws.css"
# web_include_js = "/assets/am_mws/js/am_mws.js"

# include js in page
# page_js = {"page" : "public/js/file.js"}

# include js in doctype views
# doctype_js = {"doctype" : "public/js/doctype.js"}
# doctype_list_js = {"doctype" : "public/js/doctype_list.js"}
# doctype_tree_js = {"doctype" : "public/js/doctype_tree.js"}
# doctype_calendar_js = {"doctype" : "public/js/doctype_calendar.js"}

# Home Pages
# ----------

# application home page (will override Website Settings)
# home_page = "login"

# website user home page (by Role)
# role_home_page = {
#	"Role": "home_page"
# }

# Website user home page (by function)
# get_website_user_home_page = "am_mws.utils.get_home_page"

# Generators
# ----------

# automatically create page for each record of this doctype
# website_generators = ["Web Page"]

# Installation
# ------------

# before_install = "am_mws.install.before_install"
# after_install = "am_mws.install.after_install"

# Desk Notifications
# ------------------
# See frappe.core.notifications.get_notification_config

# notification_config = "am_mws.notifications.get_notification_config"

# Permissions
# -----------
# Permissions evaluated in scripted ways

# permission_query_conditions = {
# 	"Event": "frappe.desk.doctype.event.event.get_permission_query_conditions",
# }
#
# has_permission = {
# 	"Event": "frappe.desk.doctype.event.event.has_permission",
# }

# Document Events
# ---------------
# Hook on document methods and events

# doc_events = {
# 	"*": {
# 		"on_update": "method",
# 		"on_cancel": "method",
# 		"on_trash": "method"
#	}
# }

# Scheduled Tasks
# ---------------

# scheduler_events = {
# 	"all": [
# 		"am_mws.tasks.all"
# 	],
# 	"daily": [
# 		"am_mws.tasks.daily"
# 	],
# 	"hourly": [
# 		"am_mws.tasks.hourly"
# 	],
# 	"weekly": [
# 		"am_mws.tasks.weekly"
# 	]
# 	"monthly": [
# 		"am_mws.tasks.monthly"
# 	]
# }
scheduler_events = {
		"hourly": [
			"am_mws.am_mws.doctype.mws_integration_settings.mws_integration_settings.update_refund_fulfil_dates",
			"am_mws.am_mws.doctype.mws_integration_settings.mws_integration_settings.schedule_get_order_details"
	],
		"cron": {
			"*/15 * * * *":[
				"am_mws.am_mws.doctype.mws_integration_settings.mws_integration_settings.submit_mfn_invoices"
				]
			}
}
# Testing
# -------

# before_tests = "am_mws.install.before_tests"

# Overriding Whitelisted Methods
# ------------------------------
#
# override_whitelisted_methods = {
# 	"frappe.desk.doctype.event.event.get_events": "am_mws.event.get_events"
# }

