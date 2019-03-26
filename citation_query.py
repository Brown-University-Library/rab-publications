import requests
import urllib.parse
import csv
import os
import sys
import json
import argparse
import time
import logging
import logging.handlers

from config import development as settings

query_url = settings.config['RAB_QUERY_API']
email = settings.config['ADMIN_EMAIL']
passw = settings.config['ADMIN_PASS']
log_file = settings.config['QUERY_LOG_FILE']

logger = logging.getLogger(__name__)
handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=100000, backupCount=2)
formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s')

logger.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger.addHandler(handler)

def query_pubs(debug=False, test=False):
    query = """
        PREFIX rdf:		<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs:	<http://www.w3.org/2000/01/rdf-schema#>
        PREFIX bcite:	<http://vivo.brown.edu/ontology/citation#>
        PREFIX tmp:     <http://temporary.name.space/>
        CONSTRUCT {
            ?cite a bcite:Citation .
            ?cite ?p ?string .
            ?cite tmp:venue ?venue .
            ?cite tmp:publisher ?publisher .
            ?cite tmp:location ?location .
            ?cite tmp:country ?country .
            ?cite tmp:conference ?conference .
            ?cite tmp:authority ?authority .
        }
        WHERE {
            {
                ?cite a bcite:Citation .
                ?cite ?p ?o .
                BIND(str(?o) as ?string )
            }
            UNION {
                ?cite a bcite:Citation .
                ?cite bcite:hasVenue ?x .
                ?x rdfs:label ?venue .
            }
            UNION {
                ?cite a bcite:Citation .
                ?cite bcite:hasPublisher ?x .
                ?x rdfs:label ?publisher .
            }
            UNION {
                ?cite a bcite:Citation .
                ?cite bcite:hasLocation ?x .
                ?x rdfs:label ?location .
            }
            UNION {
                ?cite a bcite:Citation .
                ?cite bcite:hasCountry ?x .
                ?x rdfs:label ?country .
            }
            UNION {
                ?cite a bcite:Citation .
                ?cite bcite:hasConference ?x .
                ?x rdfs:label ?conference .
            }
            UNION {
                ?cite a bcite:Citation .
                ?cite bcite:hasAuthority ?x .
                ?x rdfs:label ?authority .
            }
        }
    """
    if test:
        query += "\nLIMIT 20"
    # MIME type for ntriples
    # https://www.w3.org/2001/sw/RDFCore/ntriples/
    headers = {'Accept': 'text/plain', 'charset':'utf-8'}
    data = { 'email': email, 'password': passw, 'query': query }
    resp = requests.post(query_url, data=data, headers=headers)
    if debug:
        logger.debug("SENT: Headers {}".format(resp.request.headers))
        logger.debug("SENT: Body >> {}".format(
            urllib.parse.unquote_plus(
                resp.request.body[resp.request.body.index('query'):]) ))
        logger.debug("RECEIVED: status code {}".format(resp.status_code))
        logger.debug("RECEIVED: Headers {}".format(resp.headers))
    if resp.status_code == 200:
        logger.info(
            'Citation query successful. Response length: {}'.format(
                (resp.headers['content-length']) ))
        return resp
    else:
        logger.error('Bad response from Query API: {}'.format(resp.text))
        return []

def main(debug=False, test=False):
    logger.info('Begin citation query')
    if debug:
        logger.setLevel(logging.DEBUG)
        logger.debug('DEBUG MODE')
    logger.info('Submitting citation query to: {}'.format(query_url))
    data = query_pubs(debug=debug, test=test)
    if debug:
        logger.debug(data.text)
        logger.debug('DEBUGGING COMPLETE')
        return

    with open(os.path.join('data', 'query_data.nt'), 'wb') as f:
        logger.info('Writing data to {}'.format(f.name))
        for d in data:
            f.write(d)

    logger.info('Citation query complete')

if __name__ == "__main__":
    arg_parse = argparse.ArgumentParser()
    arg_parse.add_argument("-d","--debug", action="store_true")
    arg_parse.add_argument("-t","--test", action="store_true")
    args = arg_parse.parse_args()
    main(debug=args.debug, test=args.test)