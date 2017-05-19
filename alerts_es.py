import getpass
import pprint
import requests
import os
import sys
import getopt
import logging
import logging.handlers
from pyelasticsearch import ElasticSearch
import urllib3
urllib3.disable_warnings()

API_URL = "https://api.zerofox.com/1.0/alerts/"
LOGFILE = "/tmp/alerts_es.py.log"

LIMIT = 200000
FILTERS = ''

logger = logging.getLogger('alerts_es.py')
formatter = logging.Formatter( '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = logging.handlers.RotatingFileHandler(LOGFILE, maxBytes=10*1000*1000, backupCount=2, encoding='UTF-8')
handler.setFormatter(formatter)
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

def main():
    pp = pprint.PrettyPrinter(indent=2)

    limit = 100
    company = ''
    host = 'localhost'
    port = 9200
    username = ''

    opts,args = getopt.getopt(sys.argv[1:], ':c:h:r:k:', ['help'])
    for opt, arg in opts:
        if opt == '--help':
            print 'alerts_es.py -c <company> -h <ES host> -r <ES port> -k <API key>'
            sys.exit(2)
        if opt == '-c':
            company = '_' + arg
        if opt == '-h':
            host = arg
        if opt == '-r':
            port = arg
        if opt == '-k':
            key = arg

    if port == '443':
        url = 'https://'
    else:
        url = 'http://'
    url += host + ':' + str(port)
    es = ElasticSearch(url)
    index = 'alerts' + company

    if key == '':
        key = raw_input("API Key: ")

    alerts = get_alerts(key, limit)
    if len(alerts) > 0:
        index_es(alerts, index, es, url)


def get_alerts(key, limit):
    offset = 0
    rowcount = 0
    alerts = []
    done = 'n'
    while done == 'n':
        response = get_page(key, limit, offset)
        alertarray = response['alerts']
        if len(alertarray) == 0:
            done = 'y'
            break
        for alert in alertarray:
            alert = transform(alert)
            alerts.append(alert)
            rowcount +=1
            if rowcount >= LIMIT:
                done = 'y'
                break

        offset += 100
    print str(rowcount) + " alerts retrieved"
    logger.info(str(rowcount) + " alerts retrieved")
    return alerts

def index_es(alerts, index, es, url):
    try:
        es.delete_index(index)
    except:
        pass    # This will generate exception if index doesn't already exist so ignore exception
    settings = {
        "mappings": {
            "alert": {
                "properties": {
                    "alert_type": {
                        "type": "string", "index" : "not_analyzed"
                    },
                    "entity_id": {
                        "type": "long"
                    },
                    "entity_image": {
                        "type": "string"
                    },
                    "entity_name": {
                        "type": "string", "index" : "not_analyzed"
                    },
                    "entity_term_id": {
                        "type": "string"
                    },
                    "entity_term_name": {
                        "type": "string", "index" : "not_analyzed"
                    },
                    "entity_term_type": {
                        "type": "string"
                    },
                    "assignee": {
                        "type": "string"
                    },
                    "entered_by": {
                        "type": "string"
                    },
                    "metadata": {
                        "type": "string"
                    },
                    "network": {
                        "type": "string"
                    },
                    "notes": {
                        "type": "string"
                    },
                    "offending_content_url": {
                        "type": "string"
                    },
                    "perpetrator_account_number": {
                        "type": "string"
                    },
                    "perpetrator_display_name": {
                        "type": "string", "index" : "not_analyzed"
                    },
                    "perpetrator_id": {
                        "type": "long"
                    },
                    "perpetrator_image": {
                        "type": "string"
                    },
                    "perpetrator_type": {
                        "type": "string"
                    },
                    "perpetrator_username": {
                        "type": "string", "index" : "not_analyzed"
                    },
                    "protected_social_object": {
                        "type": "string"
                    },
                    "rule_id": {
                        "type": "long"
                    },
                    "rule_name": {
                        "type": "string", "index" : "not_analyzed"
                    },
                    "severity": {
                        "type": "long"
                    },
                    "status": {
                        "type": "string"
                    },
                    "takedown_requestor": {
                        "type": "string"
                    },
                    "time_accept": {
                        "format": "strict_date_optional_time||epoch_millis",
                        "type": "date"
                    },
                    "time_accept_takedown": {
                        "format": "strict_date_optional_time||epoch_millis",
                        "type": "date"
                    },
                    "time_assign": {
                        "format": "strict_date_optional_time||epoch_millis",
                        "type": "date"
                    },
                    "time_close": {
                        "format": "strict_date_optional_time||epoch_millis",
                        "type": "date"
                    },
                    "time_close_due_to_whitelist": {
                        "format": "strict_date_optional_time||epoch_millis",
                        "type": "date"
                    },
                    "time_deny_takedown": {
                        "format": "strict_date_optional_time||epoch_millis",
                        "type": "date"
                    },
                    "time_email": {
                        "format": "strict_date_optional_time||epoch_millis",
                        "type": "date"
                    },
                    "time_open": {
                        "format": "strict_date_optional_time||epoch_millis",
                        "type": "date"
                    },
                    "time_open_for_review": {
                        "format": "strict_date_optional_time||epoch_millis",
                        "type": "date"
                    },
                    "time_reject": {
                        "format": "strict_date_optional_time||epoch_millis",
                        "type": "date"
                    },
                    "time_reopen": {
                        "format": "strict_date_optional_time||epoch_millis",
                        "type": "date"
                    },
                    "time_request_takedown": {
                        "format": "strict_date_optional_time||epoch_millis",
                        "type": "date"
                    },
                    "time_whitelist": {
                        "format": "strict_date_optional_time||epoch_millis",
                        "type": "date"
                    },
                    "timestamp": {
                        "format": "strict_date_optional_time||epoch_millis",
                        "type": "date"
                    }
                }
            }
        }
    }
    es.create_index(index, settings=settings)
    try:
        es.bulk((es.index_op(alert, id=alert.pop('id')) for alert in alerts),
            index=index,
            doc_type='alert')
        logger.info("Sent alerts to " + url + " with index " + index)
    except Exception as e:
        logger.error(e)

def transform(alert):
    alert['network'] = keyCheck('network', alert, '')
    alert['notes'] = keyCheck('notes', alert, '')
    alert['rule_name'] = keyCheck('rule_name', alert, '')
    alert['entity_name'] = alert['entity']['name']
    alert['entity_image'] = alert['entity']['image']
    alert['entity_id'] = alert['entity']['id']
    alert['perpetrator_username'] = keyCheck('username', alert['perpetrator'], '')
    alert['perpetrator_display_name'] = keyCheck('display_name', alert['perpetrator'], '')
    alert['perpetrator_image'] = keyCheck('image', alert['perpetrator'], '')
    alert['perpetrator_id'] = keyCheck('id', alert['perpetrator'], '')
    alert['perpetrator_type'] = keyCheck('type', alert['perpetrator'], '')
    alert['perpetrator_account_number'] = keyCheck('account_number', alert['perpetrator'], '')
    alert['entity_term_name'] = keyCheck('name', alert['entity_term'], '')
    alert['entity_term_type'] = keyCheck('type', alert['entity_term'], '')
    alert['entity_term_id'] = str(keyCheck('id', alert['entity_term'], ''))
    for log in alert['logs']:
        key = 'time_' + log['action'].replace(' ', '_')
        alert[key] = log['timestamp']
        if log['action'] == 'request takedown':
            alert['takedown_requestor'] = log['actor']
    del alert['entity']
    del alert['perpetrator']
    del alert['entity_term']
    del alert['logs']
    return alert

def keyCheck(key, arr, default):
    if arr is None:
        return default
    if key in arr.keys():
        if arr[key] is None:
            return default
        else:
            return arr[key]
    else:
        return default

def get_page(key, limit, offset):
    filters = {
        'limit': limit,
        'offset': offset
    }
    
    headers = {
        'Authorization' : 'Token ' + key
    }

    filters.update(FILTERS)
    #print filters
    r = requests.get(API_URL, params=filters, headers=headers)
    if r.status_code >= 300:
        print "error: status code %d" % r.status_code
        logging.error("error: status code %d" % r.status_code)
        return {'count': 0, 'alerts': []}

    response = r.json()
    return response

main()

