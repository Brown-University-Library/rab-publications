from collections import defaultdict
from operator import itemgetter
import json
import re
import os

def get_shortid(uri):
    return uri[34:-1]

triple_pattern = re.compile('(?P<sbj>\<[^<]+\>) (?P<pred>\<[^<]+\>) (?P<obj>.+$)')

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


def parse_triples(raw):
    matched = [ re.match(triple_pattern, r.strip(' |.|\n')) for r in raw ]
    triples = [ (m['sbj'], m['pred'], m['obj']) for m in matched ]
    striples = sorted(triples, key=itemgetter(0))

    # Initialize running variables
    citations = {}
    authors = defaultdict(list)
    stamp = { v: '' for v in attr_map.values() }
    skipped = set()

    # Initialize overwritten variables
    i = 0
    uri = striples[i][0]
    data = stamp.copy()
    contributors = set()

    while i < len(striples):
        print(i)
        triple = striples[i]
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

    print(skipped)

    for auth in authors:
        citation_ids = authors[auth]
        out = []
        for cid in citation_ids:
            out.append(citations[cid])
        with open(os.path.join('citations', auth + '.json'), 'w') as f:
            json.dump(out, f, indent=2, sort_keys=True)

if __name__ == '__main__':
    with open('data/query_data.nt','r') as f:
        nt = f.readlines()
    parse_triples(nt)