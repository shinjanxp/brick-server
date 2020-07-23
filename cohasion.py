from dotenv import load_dotenv
load_dotenv()
from tests.remote.common import ENTITY_BASE, authorize_headers, BRICK, QUERY_BASE
from tests.remote.test_entities import *

class Entity:
    def __init__(self,id, classId):
        self.id = id
        self.classId = classId.split("#")[1]
    def __str__(self):
        return "%s => %s"%(self.id,self.classId)
    def __repr__(self):
        return "%s => %s"%(self.id,self.classId)
        
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
    SELECT DISTINCT ?child ?cc
    WHERE {
        {?child a/rdfs:subClassOf* brick:Location .
        %s brick:hasPart ?child .
        ?child a ?cc .}
        UNION
        {?child a/rdfs:subClassOf* brick:Location .
        ?child brick:isPartOf %s .
        ?child a ?cc .}
    }
    """ % (entity_id, entity_id)
    headers = authorize_headers({
        'Content-Type': 'sparql-query'
    })
    resp = requests.post(QUERY_BASE + '/sparql', data=qstr, headers=headers)
    resp = (resp.json()['results']['bindings'])
    return  list(map(lambda x: Entity(x['child']['value'], x['cc']['value']), resp))


def get_points(entity_id):
    qstr = """
    PREFIX acad1: <acad1:>

    SELECT DISTINCT ?point ?pc
    WHERE {
        {%s brick:hasPoint ?point .
        ?point a/rdfs:subClassOf* brick:Point .
        ?point a ?pc .}
        UNION
        {?point brick:isPointOf %s .
        ?point a/rdfs:subClassOf* brick:Point .
        ?point a ?pc .}
        
    }
    """ % (entity_id, entity_id)
    headers = authorize_headers({
        'Content-Type': 'sparql-query'
    })
    resp = requests.post(QUERY_BASE + '/sparql', data=qstr, headers=headers)
    resp= resp.json()['results']['bindings']
    return  list(map(lambda x: Entity(x['point']['value'], x['pc']['value']), resp))



def get_root_nodes():
    qstr = """
    SELECT DISTINCT ?node ?nc
    WHERE {
        ?node a/rdfs:subClassOf* brick:Location .
        MINUS{
            {?parent brick:hasPart ?node.}
            UNION
            {?node brick:isPartOf ?parent.}
        }
        ?node a ?nc .
    }
    """
    headers = authorize_headers({
        'Content-Type': 'sparql-query'
    })
    resp = requests.post(QUERY_BASE + '/sparql', data=qstr, headers=headers)
    # print(resp.json())
    resp= resp.json()['results']['bindings']
    return  list(map(lambda x: Entity(x['node']['value'], x['nc']['value']), resp))


def dfs(root):
    # if already visited, ignore
    if root in visited:
        return
    # get children
    children = get_children(root.id)

    for child in children:
        # iterate over children
        dfs(child)
    # do stuff
    print(root.id)
    points = get_points(root.id)
    print("points",points)
    visited[root] = True


load_ttl()
roots = get_root_nodes()
print(roots)
visited={}
for root in roots:
    dfs(root)
# print (get_points('acad1:CG11'))