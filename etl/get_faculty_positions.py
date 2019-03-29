import requests
import urllib.parse
import csv
import os
import sys
import json
import argparse
import time
import logging

logger = logging.getLogger(__name__)

def query_faculty(queryUrl, user, passw, debug=False, test=False):
    query = """
        PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs:     <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX blocal:   <http://vivo.brown.edu/ontology/vivo-brown/>
        PREFIX bwday:    <http://vivo.brown.edu/ontology/workday#>
        PREFIX vivo:     <http://vivoweb.org/ontology/core#>
        PREFIX tmp:     <http://temporary.name.space/>

        SELECT ?fac ?shortid ?pos ?rank ?unit
        WHERE
        {
              ?fac a vivo:FacultyMember.
              ?fac blocal:shortId ?shortid .
              ?fac vivo:personInPosition ?pos .
              ?pos bwday:appointmentRank ?rank .
              ?pos vivo:positionInOrganization ?org .
              ?org rdfs:label ?unit .
        }
    """
    if test:
        query += "\nLIMIT 20"
    headers = {'Accept': 'text/csv', 'charset':'utf-8'}
    data = { 'email': user, 'password': passw, 'query': query }
    resp = requests.post(queryUrl, data=data, headers=headers)
    if debug:
        logger.debug("SENT: Headers {}".format(resp.request.headers))
        logger.debug("SENT: Body >> {}".format(
            urllib.parse.unquote_plus(
                resp.request.body[resp.request.body.index('query'):]) ))
        logger.debug("RECEIVED: status code {}".format(resp.status_code))
        logger.debug("RECEIVED: Headers {}".format(resp.headers))
    if resp.status_code == 200:
        logger.info(
            'Faculty query successful. Response length: {}'.format(
                len(resp.text) ))
        return resp
    else:
        logger.error('Bad response from Query API: {}'.format(resp.text))
        return []

def main(queryUrl, user, passw, dataDir, debug=False, test=False):
    logger.info('Begin faculty query')
    if debug:
        logger.setLevel(logging.DEBUG)
        logger.debug('DEBUG MODE')
    logger.info('Submitting faculty query to: {}'.format(queryUrl))
    data = query_faculty(queryUrl, user, passw, debug=debug, test=test)
    if debug:
        logger.debug(data.text)
        logger.debug('DEBUGGING COMPLETE')
        return

    with open(os.path.join(dataDir, 'faculty_data.csv'), 'w') as f:
        logger.info('Writing data to {}'.format(f.name))
        f.write(data.text)

    logger.info('Faculty query complete')

if __name__ == '__main__':
    arg_parse.add_argument('-e','--endpoint', action="store_true")
    arg_parse.add_argument('-u','--user', action="store_true")
    arg_parse.add_argument('-p','--password', action="store_true")
    arg_parse.add_argument('-r','--data', action="store_true")
    arg_parse.add_argument('-d','--debug', action="store_true")
    arg_parse.add_argument('-t','--test', action="store_true")
    args = arg_parse.parse_args()
    main(args.endpoint, args.user, args.password,
        args.data, debug=args.debug, test=args.test)