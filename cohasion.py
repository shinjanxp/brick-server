from dotenv import load_dotenv
load_dotenv()
from tests.remote.common import ENTITY_BASE, authorize_headers, BRICK, QUERY_BASE
from tests.remote.test_entities import *


def load_ttl():
    with open('examples/data/acad1.ttl', 'rb') as fp:
        headers = authorize_headers({
            'Content-Type': 'text/turtle',
        })
        resp = requests.post(ENTITY_BASE + '/upload',
                             headers=headers, data=fp, allow_redirects=False)
        print(resp.json())


def get_entity():
    headers = authorize_headers()
    resp = requests.get(ENTITY_BASE + '/' +
                        quote_plus(znt_id), headers=headers)
    print(resp.json())


def sparql_location_tree():
    qstr = """
select ?child ?parent ?pc ?cc where {
    {?child brick:isPartOf ?parent.}
    UNION
    {?parent brick:hasPoint ?child.}
    ?parent a ?pc.
    ?child a ?cc.
}
"""
    headers = authorize_headers({
        'Content-Type': 'sparql-query'
    })
    resp = requests.post(QUERY_BASE + '/sparql', data=qstr, headers=headers)
    print(resp)
    print(resp.json()['results']['bindings'])
    return resp.json()


def sparql_root_nodes():
    qstr = """
    SELECT DISTINCT ?child 
       WHERE {
           ?child a/rdfs:subClassOf* brick:Location.
           FILTER NOT EXISTS{
               ?parent brick:hasPart ?child.
           }
       }
    """
    headers = authorize_headers({
        'Content-Type': 'sparql-query'
    })
    resp = requests.post(QUERY_BASE + '/sparql', data=qstr, headers=headers)
    # print(resp.json())
    return resp.json()




load_ttl()
# graph = sparql_location_tree()
roots = sparql_root_nodes()
print(roots)