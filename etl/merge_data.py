from collections import defaultdict
from operator import itemgetter
import json
import argparse
import re
import os
import csv
import logging

logger = logging.getLogger(__name__)

attr_map = {
    '<http://www.w3.org/2000/01/rdf-schema#label>': 'title',
    '<http://vivo.brown.edu/ontology/citation#authorList>': 'authors',
    '<http://vivo.brown.edu/ontology/citation#chapter>' : 'chapter',
    '<http://vivo.brown.edu/ontology/citation#pageEnd>' : 'page_end',
    '<http://vivo.brown.edu/ontology/citation#isbn>' : 'isbn',
    '<http://vivo.brown.edu/ontology/citation#volume>' : 'volume',
    '<http://vivo.brown.edu/ontology/citation#doi>' : 'doi',
    '<http://vivo.brown.edu/ontology/citation#patentNumber>' : 'patent_number',
    '<http://vivo.brown.edu/ontology/citation#pmcid>' : 'pmcid',
    '<http://vivo.brown.edu/ontology/citation#wokId>' : 'wok_id',
    '<http://vivo.brown.edu/ontology/citation#pages>' : 'pages',
    '<http://vivo.brown.edu/ontology/citation#number>' : 'number',
    '<http://vivo.brown.edu/ontology/citation#pmid>' : 'pmid',
    '<http://vivo.brown.edu/ontology/citation#issn>' : 'issn',
    '<http://vivo.brown.edu/ontology/citation#oclc>' : 'oclc',
    '<http://vivo.brown.edu/ontology/citation#issue>' : 'issue',
    # '<http://vivo.brown.edu/ontology/citation#title>' : 'title',
    '<http://vivo.brown.edu/ontology/citation#reviewOf>' : 'review_of',
    '<http://vivo.brown.edu/ontology/citation#url>' : 'url',
    '<http://vivo.brown.edu/ontology/citation#conferenceDate>' : 'conference_date',
    '<http://vivo.brown.edu/ontology/citation#eissn>' : 'eissn',
    '<http://vivo.brown.edu/ontology/citation#book>' : 'book',
    '<http://vivo.brown.edu/ontology/citation#date>' : 'date',
    '<http://vivo.brown.edu/ontology/citation#version>' : 'version',
    '<http://vivo.brown.edu/ontology/citation#editorList>' : 'editors',
    '<http://vivo.brown.edu/ontology/citation#pageStart>': 'page_start',
    '<http://temporary.name.space/venue>' : 'published_in',
    '<http://temporary.name.space/publisher>' : 'publisher',
    '<http://temporary.name.space/location>' : 'location',
    '<http://temporary.name.space/country>' : 'country',
    '<http://temporary.name.space/conference>' : 'conference',
    '<http://temporary.name.space/authority>' : 'authority'
}

def get_active_faculty_titles(facultyRows):
    faculty = defaultdict(list)
    for row in facultyRows:
        faculty[row['shortid']].append(
            {'rank': row['rank'], 'unit': row['unit'], 'type': row['type']})
    return faculty

def key_titles_on_position_type(titleDict):
    by_type = { shortid: { 'faculty_titles': [], 'admin_titles': [] }
        for shortid in titleDict }
    for shortid, titles in titleDict.items():
        for title in titles:
            title_type = title['type']
            del title['type']
            if title_type == 'http://vivoweb.org/ontology/core#FacultyAdministrativePosition':
                by_type[shortid]['admin_titles'].append(title)
            else:
                by_type[shortid]['faculty_titles'].append(title)
    return by_type

def get_rabid_suffix(uri):
    return uri[34:-1]

dtype_pattern = re.compile(r'\^\^\<[^>]+\>$')
def clean_datatyping(dataStr):
    if re.search(dtype_pattern, dataStr):
        dataStr = dataStr[:dataStr.rfind('^^<')-1]
    return dataStr

triple_pattern = re.compile(r'^(?P<sbj>\<[^>]+\>) (?P<pred>\<[^>]+\>) (?P<obj>.+) \.\n$')
def parse_triples(raw):
    triples = []
    for r in raw:
        matched = re.match(triple_pattern, r)
        obj = clean_datatyping(matched['obj'])
        triples.append((matched['sbj'], matched['pred'], obj))
    # Should already be sorted by URI, but let's be extra careful
    striples = sorted(triples, key=itemgetter(0))
    return striples

def convert_triples_to_data_objects(triples):
    # Initialize running variables
    citations = {}
    stamp = { v: '' for v in attr_map.values() }
    authors = defaultdict(set)
    skipped = set()

    # Initialize overwritten variables
    i = 0
    uri = triples[i][0]
    data = stamp.copy()
    contributors = set()

    while i < len(triples):
        triple = triples[i]
        if triple[0] != uri:
            # save current data, and start over
            citations[uri] = data
            for auth in contributors:
                try:
                    authors[auth].add(uri)
                except KeyError:
                    logger.error('Author inactive or unknown: {}'.format(auth))
            uri = triple[0]
            data = stamp.copy()
            contributors = set()
        pred = triple[1]
        if pred == '<http://vivo.brown.edu/ontology/citation#hasContributor>':
            # this is author data, special handling
            contributors.add(get_rabid_suffix(triple[2]))
        else:
            data_key = attr_map.get(pred, None)
            if data_key is None:
                skipped.add(pred)
            else:
                data[data_key] = triple[2].strip('"')
        i += 1
    logger.debug('Ignoring RDF properties: {}'.format(skipped))
    return citations, authors

def write_citation_objects_to_json(citations, authorCitationMap,
    authorPositions):
    # Use only faulty with positions
    for author in authorPositions:
        out = { 'titles': [], 'publications': [] }
        out['titles'] = authorPositions[author]
        citation_ids = authorCitationMap[author]
        logger.info('{}: {} citations'.format(author, len(citation_ids)))
        for cid in citation_ids:
            cite = citations[cid]
            cite['rab_id'] =  get_rabid_suffix(cid)
            out['publications'].append(cite)
        with open(os.path.join('citations', author + '.json'), 'w') as f:
            json.dump(out, f, indent=2, sort_keys=True)

def main(dataDir, debug=False, test=False):
    logger.info('Begin conversion of faculty and citation data')
    logger.info('Reading data from {}'.format(dataDir))
    with open(os.path.join(dataDir,'citation_data.nt'),'r') as f:
        ntriples = f.readlines()
    with open(os.path.join(dataDir,'faculty_data.csv'),'r') as f:
        rdr = csv.DictReader(f)
        rows = [ row for row in rdr]
    if test:
        logger.debug('TESTING: subset of ntriples')
        ntriples = ntriples[:200]
    logger.info('Data successfully accessed')
    logger.info('Transforming active faculty')
    active_faculty = get_active_faculty_titles(rows)
    faculty_titles = key_titles_on_position_type(active_faculty)
    logger.info('Begin parsing ntriples')
    parsed = parse_triples(ntriples)
    if debug:
        logger.setLevel(logging.DEBUG)
        logger.debug('DEBUG MODE')
    logger.info('Raw data successfully parsed')
    logger.info(
        'Converting {} lines of parsed data to data maps'.format(
            len(parsed)))
    citation_objs, author_key = convert_triples_to_data_objects(parsed)
    logger.info('Conversion successful')
    if debug:
        logger.debug('DEBUG COMPLETED')
        return
    logger.info('Begin write to individual JSON files')
    write_citation_objects_to_json(citation_objs, author_key,
        faculty_titles)
    logger.info('Creation of citation JSON complete')

if __name__ == '__main__':
    arg_parse = argparse.ArgumentParser()
    arg_parse.add_argument('data')
    arg_parse.add_argument('-d','--debug', action='store_true')
    arg_parse.add_argument('-t','--test', action='store_true')
    args = arg_parse.parse_args()
    main(args.data, debug=args.debug, test=args.test)