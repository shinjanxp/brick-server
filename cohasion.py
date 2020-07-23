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


def get_entity(entity_id):
    headers = authorize_headers()
    resp = requests.get(ENTITY_BASE + '/' +
                        quote_plus(entity_id), headers=headers)
    return resp.json()

def get_children(entity_id):
    qstr = """
    PREFIX acad1: <acad1:>
    SELECT DISTINCT ?child
    WHERE {
        {?child a/rdfs:subClassOf* brick:Location .
        %s brick:hasPart ?child .}
        UNION
        {?child a/rdfs:subClassOf* brick:Location .
        ?child brick:isPartOf %s .}
    }
    """ % (entity_id, entity_id)
    headers = authorize_headers({
        'Content-Type': 'sparql-query'
    })
    resp = requests.post(QUERY_BASE + '/sparql', data=qstr, headers=headers)
    resp = (resp.json()['results']['bindings'])
    return list(map(lambda x: x['child']['value'], resp))


def get_points(entity_id):
    qstr = """
    SELECT DISTINCT ?point
    WHERE {
        ?point a/rdfs:subClassOf* brick:Point .
        {%s brick:hasPoint ?child .}
        UNION
        {?child brick:isPointOf %s .}
    }
    """ % (entity_id, entity_id)
    headers = authorize_headers({
        'Content-Type': 'sparql-query'
    })
    resp = requests.post(QUERY_BASE + '/sparql', data=qstr, headers=headers)
    return (resp.json()['results']['bindings'])


def get_root_nodes():
    qstr = """
    SELECT DISTINCT ?node
    WHERE {
        ?node a/rdfs:subClassOf* brick:Location .
        MINUS{
            {?parent brick:hasPart ?node.}
            UNION
            {?node brick:isPartOf ?parent.}
        }
    }
    """
    headers = authorize_headers({
        'Content-Type': 'sparql-query'
    })
    resp = requests.post(QUERY_BASE + '/sparql', data=qstr, headers=headers)
    # print(resp.json())
    resp= resp.json()['results']['bindings']
    return  list(map(lambda x: x['node']['value'], resp))


def dfs(root):
    # if already visited, ignore
    if root in visited:
        return
    # get children
    children = get_children(root)

    for child in children:
        # iterate over children
        dfs(child)
    # do stuff
    print(root)
    visited[root] = True


load_ttl()
roots = get_root_nodes()
print(roots)
visited={}
for root in roots:
    dfs(root)