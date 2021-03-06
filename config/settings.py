import os

config = {
	'APP_ROOT' : os.path.abspath(__file__ + "/../../"),
	'RAB_QUERY_API': '',
	'ADMIN_EMAIL' : '',
	'ADMIN_PASS' : '',
}

config['DATA_DIR'] = os.path.join(config['APP_ROOT'], 'data')
config['LOG_FILE'] = os.path.join(config['APP_ROOT'], 'logs',
    'citation-feed.log')