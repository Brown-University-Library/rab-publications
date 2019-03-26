from collections import defaultdict
from operator import itemgetter
import json
import argparse
import re
import os
import logging
import logging.handlers

from config import development as settings

log_file = settings.config['LOG_FILE']

logger = logging.getLogger(__name__)
handler = logging.handlers.RotatingFileHandler(
    log_file, maxBytes=100000, backupCount=2)
formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s')

logger.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger.addHandler(handler)

attr_map = {
    '<http://www.w3.org/2000/01/rdf-schema#label>': 'label',
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
    '<http://vivo.brown.edu/ontology/citation#title>' : 'title',
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

def get_shortid(uri):
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
    authors = defaultdict(list)
    stamp = { v: '' for v in attr_map.values() }
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
                authors[auth].append(uri)
            uri = triple[0]
            data = stamp.copy()
            contributors = set()
        pred = triple[1]
        if pred == '<http://vivo.brown.edu/ontology/citation#hasContributor>':
            # this is author data, special handling
            contributors.add(get_shortid(triple[2]))
        else:
            data_key = attr_map.get(pred, None)
            if data_key is None:
                skipped.add(pred)
            else:
                data[data_key] = triple[2].strip('"')
        i += 1
    logger.debug('Ignoring RDF properties: {}'.format(skipped))
    return citations, authors


def write_citation_objects_to_json(citations, authorCitationMap):
    for author in authorCitationMap:
        citation_ids = authorCitationMap[author]
        logger.info('{}: {} citations'.format(author, len(citation_ids)))
        out = []
        for cid in citation_ids:
            out.append(citations[cid])
        with open(os.path.join('citations', author + '.json'), 'w') as f:
            json.dump(out, f, indent=2, sort_keys=True)

def main(ntriples, debug=False):
    logger.info('Begin conversion of ntriples to JSON files')
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
    write_citation_objects_to_json(citation_objs, author_key)
    logger.info('Creation of citation JSON complete')

if __name__ == '__main__':
    arg_parse = argparse.ArgumentParser()
    arg_parse.add_argument('-d','--debug', action='store_true')
    arg_parse.add_argument('-t','--test', action='store_true')
    args = arg_parse.parse_args()
    with open(os.path.join('data','query_data.nt'),'r') as f:
        nt = f.readlines()
    if args.test:
        nt = nt[:200]
    main(nt, debug=args.debug)