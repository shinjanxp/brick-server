from dotenv import load_dotenv
load_dotenv()
from rdflib import Namespace
from time import sleep
from collections import defaultdict
from tests.remote.test_entities import *
from tests.remote.common import ENTITY_BASE, authorize_headers, BRICK, QUERY_BASE
import json
from urllib.parse import quote_plus
from uuid import uuid4 as gen_uuid

BRICK_VERSION = '1.0.3'
BRICK = Namespace(f'https://brickschema.org/schema/{BRICK_VERSION}/Brick#')


class Entity:
    def __init__(self, id, classId):
        self.id = id
        self.classId = classId

    def __str__(self):
        return "%s => %s" % (self.id, self.classId)

    def __repr__(self):
        return "%s => %s" % (self.id, self.classId)

class AggregatePoint(Entity):
    def __init__(self, id, classId, aggregatesFor=None):
        Entity.__init__(self, id, classId)
        self.aggregatesFor = aggregatesFor

###############################################################
#################  API calls  #################################
###############################################################

def load_ttl():
    with open('examples/data/acad1.ttl', 'rb') as fp:
        headers = authorize_headers({
            'Content-Type': 'text/turtle',
        })
        resp = requests.post(ENTITY_BASE + '/upload',
                             headers=headers, data=fp, allow_redirects=False)


def get_entity(entity_id):
    headers = authorize_headers()
    resp = requests.get(ENTITY_BASE + '/' +
                        quote_plus(entity_id), headers=headers)
    return resp.json()


def get_children(entity_id):
    qstr = """
    PREFIX acad1: <http://example.com/building/acad1#>
    SELECT DISTINCT ?child ?cc
    WHERE {
        {?child a/rdfs:subClassOf* brick:Location .
        <%s> brick:hasPart ?child .
        ?child a ?cc .}
        UNION
        {?child a/rdfs:subClassOf* brick:Location .
        ?child brick:isPartOf <%s> .
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
    PREFIX acad1: <http://example.com/building/acad1#>

    SELECT DISTINCT ?point ?pc
    WHERE {
        {<%s> brick:hasPoint ?point .
        ?point a ?pc .}
        UNION
        {?point brick:isPointOf <%s> .
        ?point a ?pc .}
        FILTER NOT EXISTS {
            ?point brick:hasAssociatedTag brick_tag:Aggregate
        }
        
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


def create_entity(classId, entityId):
    headers = authorize_headers()
    body = {
        "%s" % (classId): "%s"%entityId,
    }
    resp = requests.post(ENTITY_BASE, json=body, headers=headers)
    return resp.json()["%s" %(classId)][0]


def update_entity(subject, prop, obj):
    headers = authorize_headers()
    body = {
        "relationships": [
            [str(prop), str(obj)]
        ]
    }
    resp = requests.post(ENTITY_BASE + '/' +
                         quote_plus(subject), json=body, headers=headers)


def get_all_entities():
    headers = authorize_headers()
    resp = requests.get(ENTITY_BASE, headers=headers)
    assert resp.status_code == 200
    return resp.json()['entity_ids']

def get_classes_for_point(point):
    classes = []
    # Get the class that point is an instance of
    qstr = """
    PREFIX acad1: <http://example.com/building/acad1#>
    SELECT DISTINCT ?pc
    WHERE {
        <%s> a ?pc
    }
    """%(point.id)
    headers = authorize_headers({
        'Content-Type': 'sparql-query'
    })
    resp = requests.post(QUERY_BASE + '/sparql', data=qstr, headers=headers)
    resp = resp.json()['results']['bindings']
    classes.append(resp[0]['pc']['value'])

    # Get the classes that point aggregatesFor
    qstr = """
    PREFIX acad1: <http://example.com/building/acad1#>
    SELECT DISTINCT ?afc
    WHERE {
        <%s> brick:aggregatesForClass ?afc
    }
    """%(point.id)
    headers = authorize_headers({
        'Content-Type': 'sparql-query'
    })
    resp = requests.post(QUERY_BASE + '/sparql', data=qstr, headers=headers)
    resp = resp.json()['results']['bindings']
    classes.extend(list(map(lambda x:x['afc']['value'] , resp)))
    return tuple(classes)

def random_query():
    qstr = """
    PREFIX acad1: <http://example.com/building/acad1#>
    SELECT DISTINCT ?subject ?object ?oc ?afc
    WHERE {
        ?subject brick:hasPoint ?object .
        ?object a ?oc .
        ?object brick:hasAssociatedTag brick_tag:Aggregate .
        OPTIONAL {?object brick:aggregatesForClass ?afc .}
    }
    """
    headers = authorize_headers({
        'Content-Type': 'sparql-query'
    })
    resp = requests.post(QUERY_BASE + '/sparql', data=qstr, headers=headers)
    # print(resp.json())
    resp = resp.json()['results']['bindings']
    print(len(resp))
    return resp
    # return list(map(lambda x: Entity(x['node']['value'], x['nc']['value']), resp))

def writeDictToFile(d, filename):
    with open(filename, 'w') as outfile:
        outfile.write(json.dumps(d, indent=4))
        
    
if __name__=="__main__":
    writeDictToFile(random_query(), 'result.txt')
    