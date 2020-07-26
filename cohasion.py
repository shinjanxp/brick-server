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

def random_query():
    qstr = """
    PREFIX acad1: <http://example.com/building/acad1#>
    SELECT DISTINCT ?subject ?object
    WHERE {
        ?subject brick:hasPoint ?object .
        ?object brick:hasAssociatedTag brick_tag:Aggregate .
    }
    """
    headers = authorize_headers({
        'Content-Type': 'sparql-query'
    })
    resp = requests.post(QUERY_BASE + '/sparql', data=qstr, headers=headers)
    # print(resp.json())
    resp = resp.json()['results']['bindings']
    return resp
    # return list(map(lambda x: Entity(x['node']['value'], x['nc']['value']), resp))


###############################################################
#################  aggregation methods  #######################
###############################################################

def groupPointsForNode(points):
    groups = defaultdict(list)
    # Append points to a dict keyed by classId
    for point in points:
        groups[point.classId].append(point)
    # Iterate through each class
    return groups

def checkAggregateExists(points, aggregatesForClassId=None):
    qstr = """
    SELECT DISTINCT ?node ?nc
    WHERE {
        ?node a ?nc .
        """
    for point in points:
        qstr +="?node brick:aggregates <%s> . \n    "%(point.id)
    if aggregatesForClassId:
        qstr +=" ?node brick:aggregatesForClass <%s> . \n    "%(aggregatesForClassId)

    qstr+="}"
    
    headers = authorize_headers({
        'Content-Type': 'sparql-query'
    })
    resp = requests.post(QUERY_BASE + '/sparql', data=qstr, headers=headers)
    # print(resp.json())
    resp = resp.json()['results']['bindings']
    if(len(resp)==0):
        return None
    else:
        return Entity(resp[0]['node']['value'], resp[0]['nc']['value'])

def getAggregatePointsForNode(node):
    qstr = """
    SELECT DISTINCT ?point ?pc ?afc
    WHERE {
        ?point a ?pc .
        ?point brick:hasAssociatedTag brick_tag:Aggregate .
        <%s> brick:hasPoint ?point .
        OPTIONAL { ?point brick:AggregatesForClass ?afc . }
    }
    """%node.id
    
    headers = authorize_headers({
        'Content-Type': 'sparql-query'
    })
    resp = requests.post(QUERY_BASE + '/sparql', data=qstr, headers=headers)
    # print(resp.json())
    resp = resp.json()['results']['bindings']
    if(len(resp)==0):
        return None
    else:
        return list(map(lambda x: AggregatePoint(x['point']['value'], x['pc']['value']), resp))

def generateAggregatePoints(node, groups):
    
    for classId, points in groups.items():
        existingAggregate = checkAggregateExists(points)
        if(existingAggregate):
            continue
        # Need to aggregate

        # If only one point exists in group, use that as the aggregate
        if len(points) == 1:
            update_entity(quote_plus(points[0].id), 'brick:hasAssociatedTag', 'brick_tag:Aggregate') # Assign it the aggregate tag
            continue
        
        # Generate a new aggregate point with the same class as all the other points
        aggregatePointId = "http://example.com/building/acad1#%s"%gen_uuid()
        aggregatePoint = Entity(create_entity(classId, aggregatePointId), classId) # Create the new point entity
        

        update_entity(quote_plus(aggregatePoint.id), 'brick:hasAssociatedTag', 'brick_tag:Aggregate') # Assign it the aggregate tag
        update_entity(quote_plus(node.id), 'brick:hasPoint', aggregatePoint.id) # we need to urlencode the first arguement since that will be used as a url parameter. The rest are in request body
        for point in points:
            # Relate generated point with existing points via aggregates relation
            update_entity(quote_plus(aggregatePoint.id), 'brick:aggregates', point.id)
        # Relate generated point with node

def generateAggregatePointsForChildClasses(node, groups):
    for childClassId, allPoints in groups.items():
        # Count the number of child nodes 
        childNodesCount = len(allPoints)

        # Iterate through the list of points per child and group the points based on their classId
        pointGroups = defaultdict(list)
        for childNode in allPoints:
            # childNode could be None due to the getAggregatePointsForNode function's return type
            if not childNode:
                continue
            for point in childNode:
                pointGroups[point.classId].append(point)

       # Go through each pointgroup
        for pointClassId, points in pointGroups.items():
            # Count the number of aggregate points for pointClassId. If that is not equal to the number 
            # of child nodes, that means all child nodes are not covered. So we can ignore this group
            if len(points) !=childNodesCount:
                continue
            # Check if a point already exists that aggregates the given set of points for childClassId
            existingAggregate = checkAggregateExists(points, childClassId)
            if(existingAggregate):
                # It's possible that this set of points was already aggregated for another parent node
                # Let's associate it with the current node just to be safe
                update_entity(quote_plus(node.id), 'brick:hasPoint', existingAggregate.id) 
                continue

            # If only one point exists in group, use that as the aggregate for given classId
            if len(points) == 1:
                # Assign it the aggregatesForClass relationship with the current childClass
                update_entity(quote_plus(points[0].id), 'brick:aggregatesForClass', childClassId) 
                # Associate the point with the given node 
                update_entity(quote_plus(node.id), 'brick:hasPoint', points[0].id) 
                # The point should already have the Aggregate tag, so no need to reassign it
                continue

            # Generate a new aggregate point with the same class as all the other points
            aggregatePointId = "http://example.com/building/acad1#%s"%gen_uuid()
            aggregatePoint = Entity(create_entity(pointClassId, aggregatePointId), pointClassId)
            # Assign it the aggregate tag
            # we need to urlencode the first argument in update_entity function call since that will be used as a url parameter. The rest are in request body
            update_entity(quote_plus(aggregatePoint.id), 'brick:hasAssociatedTag', 'brick_tag:Aggregate')
            # Relate the generated point with the parent node via hasPoint relationship 
            update_entity(quote_plus(node.id), 'brick:hasPoint', aggregatePoint.id)
            # Assign it the aggregatesForClass relationship with the current childClass
            update_entity(quote_plus(aggregatePoint.id), 'brick:aggregatesForClass', childClassId)
            for point in points:
                # Relate generated point with existing points via aggregates relation
                update_entity(quote_plus(aggregatePoint.id), 'brick:aggregates', point.id)
            

def dfs(node):
    # if already visited, ignore
    if node.id in visited:
        return
    # get children
    children = get_children(node.id)

    # group points attached to children
    groups = defaultdict(list)
    
    for child in children:
        # iterate over children
        dfs(child)
        childPoints = getAggregatePointsForNode(child)
        groups[child.classId].append(childPoints)
    generateAggregatePointsForChildClasses(node, groups)
    # do stuff
    points = get_points(node.id)
    # Compute pointGroup for this node
    groups = groupPointsForNode(points)
    aggregatePoints=None
    if(len(groups)>0):
        # Generate aggregate points for each group
        aggregatePoints = generateAggregatePoints(node, groups)
    visited[node.id] = True
    return aggregatePoints


# load_ttl()
# roots = get_root_nodes()
# print(roots)
# visited = {}
# for root in roots:
#     dfs(root)

# print(random_query())