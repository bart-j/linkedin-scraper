#!/usr/bin/env python
# encoding: utf-8
"""
1-linkedin-scraper.py
Bart J - 2013-10-02.

(re)building the LinkedIn Graph
"""

import logging 
import oauth2 as oauth
import urlparse
import simplejson
import codecs
import urlparse
import requests
import random
import pickle
import ConfigParser
from py2neo import neo4j, rel, node
from time import sleep

# Generate random headers, so the scraper detection of LinkedIn won't notice us
def generate_headers():
	headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.66 Safari/537.36',
	'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
	'Accept-Encoding': 'gzip,deflate,sdch',
	'Accept-Language': 'nl-NL,nl;q=0.8,en-US;q=0.6,en;q=0.4',
	'Cache-Control': 'max-age=0',
	'Connection': 'keep-alive'}
	return headers

# Wait for a random time, so the scraper detection of LinkedIn won't notice us	
def random_wait():
	wait = random.gauss(int(config.get('http','throttle')),int(config.get('http','throttle_stdev')))
	print 'Randomly waiting for ',  wait,  ' seconds'
	if wait < 0:
		wait = 0
	sleep(wait)	
	
# Extract LinkedIn Connection ID from URL in connection field
def get_id_from_url(url):
	parsed = urlparse.urlparse(url)
	return urlparse.parse_qs(parsed.query)['id'][0]
	
	
def http_login():
	global http

	random_wait()
	# Start session in LinkedIn
	http = requests.session()
	request = http.get('https://www.linkedin.com/uas/login-submit?session_key=' + config.get('http','username') + '&session_password=' + config.get('http','password'), headers=generate_headers())
	return http

def http_request(url):
	global http
	
	try:
		content = http.get(url, headers=generate_headers())
	except requests.exceptions.ConnectionError:
		sleep(10)
		http.close()
		http = http_login()
		return http_request(url)

	return content.text

def api_get_profile():
	consumer = oauth.Consumer(key=config.get('linkedin-api','consumer_key'),
							  secret=config.get('linkedin-api','consumer_secret'))
	token = oauth.Token(key=config.get('linkedin-api','token_key'), 
						secret=config.get('linkedin-api','token_secret'))
	client = oauth.Client(consumer, token)	
	
	resp, content = client.request('http://api.linkedin.com/v1/people/~/?format=json', headers=generate_headers())
	return simplejson.loads(content)
	
def api_get_firstdegree_connections():
	consumer = oauth.Consumer(key=config.get('linkedin-api','consumer_key'),
							  secret=config.get('linkedin-api','consumer_secret'))
	token = oauth.Token(key=config.get('linkedin-api','token_key'), 
						secret=config.get('linkedin-api','token_secret'))
	client = oauth.Client(consumer, token)	
	
	resp, content = client.request('http://api.linkedin.com/v1/people/~/connections?format=json', headers=generate_headers())
	content = simplejson.loads(content)    
	return content['values']
		
def http_get_seconddegree_connections(linkedinid):
	global http
	
	random_wait()
	data = http_request('https://www.linkedin.com/profile/view?id=' + str(linkedinid) + '&trk=nav_responsive_tab_profile')

	sleep(10)	
	data = http_request('https://www.linkedin.com/profile/profile-v2-connections?id=' + linkedinid + '&offset=0&count=1000&distance=0&type=ALL')
	
	data = simplejson.loads(data)
	if 'connections' not in data['content'] or 'connections' not in data['content']['connections']:
		http.close()
		http = http_login()
		return http_get_seconddegree_connections(linkedinid)
	else:
		return data['content']['connections']['connections']

def db_create_node(node_id, name, headline):
	node = db.get_or_create_indexed_node("linkedin", "linkedin", node_id, 
			   {"linkedin": int(node_id),
			    "name": name,
			   "headline": headline})
	return node
		
#memory = pickle.load(open('memory.pickle', 'rb'))
config = ConfigParser.ConfigParser()
config.read('settings.ini')
db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

# Make sure all the used indexes are available in the database
node_index = db.get_or_create_index(neo4j.Node, "linkedin")
relationship_index = db.get_or_create_index(neo4j.Relationship, "connected")

# Login HTTP session
http = http_login()

#logging.basicConfig(level=logging.DEBUG)

def linkedin_scraper():	
	# Create the starting point of the scraper, the "center", the user from which the network is build
	center_profile = api_get_profile()
	center_profile_id = get_id_from_url(center_profile['siteStandardProfileRequest']['url'])
	
	
	if 'headline' in center_profile:
		headline = center_profile['headline']
	else:
		headline = ''
		
	db_center_profile = db_create_node(center_profile_id, center_profile['firstName'] + " " + center_profile['lastName'], headline)	

	# Loop through first-degree connections
	connections = api_get_firstdegree_connections()	
	for index, firstdegree_connection in enumerate(connections, start=1):
		# Resume on halt
		if index < 543:
			continue
		
		if not 'siteStandardProfileRequest' in firstdegree_connection:
			continue		

		firstdegree_connection_id = get_id_from_url(firstdegree_connection["siteStandardProfileRequest"]["url"])

		name = firstdegree_connection['firstName'] + " " + firstdegree_connection['lastName']
		db_firstdegree_connection = db_create_node(firstdegree_connection_id, name, firstdegree_connection['headline'])
		print "[" + str(index) + "] Created node: ", firstdegree_connection_id , " ", name.encode('utf8')
		
		# Create relationship with starting point
		relationship = neo4j.Path(db_firstdegree_connection, "knows", db_center_profile)
		relationship.get_or_create(db)
		relationship = neo4j.Path(db_center_profile, "knows", db_firstdegree_connection)
		relationship.get_or_create(db)

		# Write 2-nd degree contacts
		batch = neo4j.WriteBatch(db)
		seconddegree_connections = http_get_seconddegree_connections(firstdegree_connection_id)		

		print 'Processing 2-nd degree relationships...'				
		for seconddegree_connection in seconddegree_connections:
			if 'headline' in seconddegree_connection:
				headline = seconddegree_connection['headline']
			else:
				headline = ''
				
			db_seconddegree_connection = batch.get_or_create_in_index(neo4j.Node, node_index, "linkedin", seconddegree_connection['memberID'], node(linkedin=seconddegree_connection['memberID'], name=seconddegree_connection['fmt__full_name'], headline=headline))	
			
		results = batch.submit()
		
		# Write 2-nd degree contact relationships
		batch = neo4j.WriteBatch(db)	
			
		for result in results:
			batch.get_or_create_path(result, "knows", db_firstdegree_connection)
			batch.get_or_create_path(db_firstdegree_connection, "knows", result)
			
		batch.run()

	print "Finished the scraping!"
	print "----"
	print "Center node", db_center_profile
			
if __name__ == '__main__':
	linkedin_scraper()
