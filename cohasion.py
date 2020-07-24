from dotenv import load_dotenv
load_dotenv()
from rdflib import Namespace
from time import sleep
from collections import defaultdict
from tests.remote.test_entities import *
from tests.remote.common import ENTITY_BASE, authorize_headers, BRICK, QUERY_BASE
import json

BRICK_VERSION = '1.0.3'
BRICK = Namespace(f'https://brickschema.org/schema/{BRICK_VERSION}/Brick#')


class Entity:
    def __init__(self, id, classId):
        self.id = id
        self.classId = classId.split("#")[1]

    def __str__(self):
        return "%s => %s" % (self.id, self.classId)

    def __repr__(self):
        return "%s => %s" % (self.id, self.classId)


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
    return list(map(lambda x: Entity(x['child']['value'], x['cc']['value']), resp))


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
    resp = resp.json()['results']['bindings']
    return list(map(lambda x: Entity(x['point']['value'], x['pc']['value']), resp))


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
    resp = resp.json()['results']['bindings']
    return list(map(lambda x: Entity(x['node']['value'], x['nc']['value']), resp))


def create_entity(classId):
    headers = authorize_headers()
    body = {
        "%s%s" % (BRICK, classId): 1,
    }
    resp = requests.post(ENTITY_BASE, json=body, headers=headers)
    print(resp.json())
    return resp.json()["%s%s" % (BRICK, classId)][0]


def update_entity(subject, prop, obj):
    headers = authorize_headers()
    body = {
        "relationships": [
            [str(prop), str(obj)]
        ]
    }
    print (json.dumps(body))
    resp = requests.post(ENTITY_BASE + '/' +
                         quote_plus(subject), json=body, headers=headers)
    print(resp.json())


def get_all_entities():
    headers = authorize_headers()
    resp = requests.get(ENTITY_BASE, headers=headers)
    assert resp.status_code == 200
    print(resp.json()['entity_ids'])


def groupPointsForNode(points):
    groups = defaultdict(list)
    # Append points to a dict keyed by classId
    for point in points:
        groups[point.classId].append(point)
    # Iterate through each class
    for classId in groups.keys():
        print(groups[classId])
    # print(groups)


def dfs(node):
    # if already visited, ignore
    if node.id in visited:
        return
    # get children
    children = get_children(node.id)

    for child in children:
        # iterate over children
        dfs(child)
    # do stuff
    print(node.id)
    points = get_points(node.id)
    # Compute pointGroup for this node
    groupPointsForNode(points)
    # print("points",points)
    visited[node.id] = True


load_ttl()
roots = get_root_nodes()
print(roots)
visited = {}
# for root in roots:
# dfs(root)
entityId = create_entity('Zone_Temperature_Sensor')
sleep(1)
update_entity(entityId, 'brick:hasAssociatedTag', 'brick_tag:Aggregate')
