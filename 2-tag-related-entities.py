#!/usr/bin/env python
# encoding: utf-8
"""
2-tag-related-entities.py
Bart J - 2013-10-02.

Tag certain pre-defined entities of interest in DB
"""

import time
import gdata.spreadsheet.service
from py2neo import neo4j, rel, node

email = ''
password = ''
weight = '180'
db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

spreadsheet_key = ''

client = gdata.spreadsheet.service.SpreadsheetsService()
client.email = email
client.password = password
client.source = 'Tag Related Entities'
client.ProgrammaticLogin()

feed = client.GetWorksheetsFeed(spreadsheet_key, visibility='private')
worksheet_id = feed.entry[0].id.text.rsplit('/',1)[1]

rows = client.GetListFeed(spreadsheet_key, worksheet_id).entry

for row in rows:
	if row.custom['linkedinid']:
		node = db.get_indexed_node("linkedin", "linkedin", row.custom['linkedinid'].text)

		if node:
			node['tagged'] = 1
			print 'Node ', node['linkedin'], ' is getagged'
		
