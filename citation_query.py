import requests
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
log_file = settings.config['LOG_FILE']
throttle = settings.config['THROTTLE']

logger = logging.getLogger(__name__)
handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=100000, backupCount=2)
formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s')

logger.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger.addHandler(handler)


attrMap = {
    '@id': 'url',
    'http://xmlns.com/foaf/0.1/firstName' : 'first',
    'http://xmlns.com/foaf/0.1/lastName' : 'last',
    'http://vivoweb.org/ontology/core#middleName' : 'middle',
    'http://temporary.name.space/fullName' : 'full',
    'http://vivoweb.org/ontology/core#preferredTitle' : 'title',
    'http://vivoweb.org/ontology/core#primaryEmail' : 'email',
    'http://temporary.name.space/fullImage' : 'image',
    'http://temporary.name.space/image' : 'thumbnail',
    'http://vivoweb.org/ontology/core#overview' : 'overview',
    'http://temporary.name.space/affiliations' : 'affiliations',
    'http://temporary.name.space/researchArea' : 'topics',
    'http://temporary.name.space/researchGeo' : 'countries',
    'http://vivoweb.org/ontology/core#educationalTraining' : 'education',
    'http://temporary.name.space/eduOrg' : 'organization',
    'http://temporary.name.space/degreeTitle' :'degree',
    'http://vivo.brown.edu/ontology/vivo-brown/degreeDate' : 'year',
    'http://temporary.name.space/facultyTitle' :'faculty_title',
    'http://temporary.name.space/adminTitle' :'admin_title'
}

def mint_roster_obj():
    return {
        'url' : '',
        'first' : '',
        'last' : '',
        'middle' : '',
        'full' : '',
        'title' : '',
        'title_detail' : {},
        'email' : '',
        'image' : '',
        'thumbnail' : '',
        'overview' : '',
        'affiliations' : [],
        'topics' : [],
        'countries' : [],
        'education' : []
    }


def query_pubs(org_uri):
    query = """
        PREFIX rdf:		<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs:	<http://www.w3.org/2000/01/rdf-schema#>
        PREFIX bcite:	<http://vivo.brown.edu/ontology/citation#>
        PREFIX tmp:     <http://temporary.name.space/>
        CONSTRUCT {{
            ?cite a bcite:Citation .
            ?cite ?p ?o .
            ?cite tmp:venue ?venue .
            ?cite tmp:publisher ?publisher .
            ?cite tmp:location ?location .
            ?cite tmp:country ?country .
            ?cite tmp:conference ?conference .
            ?cite tmp:authority ?authority .
        }}
        WHERE {{
            {{
                ?cite a bcite:Citation .
                ?cite ?p ?o .
            }}
            UNION {{
                ?cite a bcite:Citation .
                ?cite bcite:hasVenue ?x .
                ?x rdfs:label ?venue .
            }}
            UNION {{
                ?cite a bcite:Citation .
                ?cite bcite:hasPublisher ?x .
                ?x rdfs:label ?publisher .
            }}
            UNION {{
                ?cite a bcite:Citation .
                ?cite bcite:hasLocation ?x .
                ?x rdfs:label ?location .
            }}
            UNION {{
                ?cite a bcite:Citation .
                ?cite bcite:hasCountry ?x .
                ?x rdfs:label ?country .
            }}
            UNION {{
                ?cite a bcite:Citation .
                ?cite bcite:hasConference ?x .
                ?x rdfs:label ?conference .
            }}
            UNION {{
                ?cite a bcite:Citation .
                ?cite bcite:hasAuthority ?x .
                ?x rdfs:label ?authority .
            }}
        }}
    """.format(org_uri)
    headers = {'Accept': 'application/json', 'charset':'utf-8'}	
    data = { 'email': email, 'password': passw, 'query': query }
    resp = requests.post(query_url, data=data, headers=headers)
    if resp.status_code == 200:
        return resp.json()
    else:
        logger.error('Bad response from Query API: {}'.format(resp.text))
        return []

def extract_education_data(dataList):
    edu_map = {}
    del_index = []
    for i, data in enumerate(dataList):
        try:
            type_data = data['@type']
        except:
            del_index.append(i)
        if 'http://vivoweb.org/ontology/core#EducationalTraining' in type_data:
            del_index.append(i)
            edu_map[data['@id']] = data
        else:
            continue
    newList = [ data for i, data in enumerate(dataList) if i not in del_index ]
    return ( newList, edu_map )

def cast_edu_data(data):
    out = {}
    for k, v in data.items():
        if k in ('@id', '@type'):
            continue
        else:
            attr = attrMap[k]
            for obj in v:
                out[attr] = obj['@value']
    return out

def cast_roster_data(data, edu_map):
    out = mint_roster_obj()
    for k, v in data.items():
        if k == '@type':
            continue
        attr = attrMap[k]
        if attr == 'education':
            for eduId in v:
                eduObj = edu_map[eduId['@id']]
                edu_cast = cast_edu_data(eduObj)
                out[attr].append(edu_cast)
        elif attr == 'url':
            out[attr] += v
        elif attr in ( 'affiliations','topics','countries' ):
            for obj in v:
                out[attr].append(obj['@value'])
        elif attr in ( 'first','last','middle','title', 'full',
                        'email','image','thumbnail','overview'):
            for obj in v:
                out[attr] += obj['@value']
        elif attr in ('faculty_title', 'admin_title'):
            attrs = {
                'faculty_title': 'faculty',
                'admin_title': 'administrative',
            }
            out['title_detail'][attrs[attr]] = [ obj['@value'] for obj in v ]
        else:
            raise Exception(k)
    return out

def main(uri=None, all_uris=False):
    logger.info('Initiating roster build')
    uri_tuples = []
    if all_uris:
        with open('data/org_ids.csv','r') as f:
            rdr = csv.reader(f)
            for row in rdr:
                uri_tuples.append(row)
    elif uri:
        uri_tuples.append( (uri, uri[33:]) )
    for uri_tup in uri_tuples:
        time.sleep(throttle)
        logger.info('Building roster for: {}'.format(uri_tup[1]))
        try:
            roster_resp = query_roster(uri_tup[0])
        except:
            logger.error(
                'Failure to query roster for: {}'.format(uri_tup[1]))
            continue
        try:
            roster_list, edu_map = extract_education_data(roster_resp)
        except:
            logger.error(
                'Failure to extract data for: {}'.format(uri_tup[1]))
            continue
        unit_data = { 'unit': uri_tup[0], 'roster': [] }
        for prsn in roster_list:
            try:
                prsn_data = cast_roster_data(prsn, edu_map)
            except:
                rabid = prsn.get('@id', 'Could not parse RABID')
                logger.error(
                    'Failure to cast data for: {}'.format(rabid))
                continue
            unit_data['roster'].append(prsn_data)
        logger.info('Writing JSON for: {}'.format(uri_tup[1]))
        with open(os.path.join('rosters', uri_tup[1] +'.json'), 'w') as f:
            json.dump(unit_data, f,
                indent=2, sort_keys=True, ensure_ascii=False)
    logger.info('Roster build complete')

if __name__ == "__main__":
    arg_parse = argparse.ArgumentParser()
    arg_parse.add_argument("-u","--uri")
    arg_parse.add_argument("-a","--all", action="store_true")
    arg_parse.add_argument("-t","--test", action="store_true")
    args = arg_parse.parse_args()
    if args.uri:
        main(uri=args.uri)
    if args.all:
        main(all_uris=True)
    if args.test:
        main(uri='http://vivo.brown.edu/individual/org-brown-univ-dept29')
