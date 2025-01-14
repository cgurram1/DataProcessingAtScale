from neo4j import GraphDatabase

class Interface:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.create_graph_projection()

    def close(self):
        self.driver.close()
        
    def create_graph_projection(self):
        with self.driver.session() as session:
            session.run("""
                CALL gds.graph.drop('myGraph', false) 
                YIELD graphName, nodeCount, relationshipCount
            """)
            session.run("""
                CALL gds.graph.project(
                    'myGraph',
                    'Location',
                    'TRIP',
                    {
                        relationshipProperties: ['distance', 'fare']
                    }
                )
            """)
    def bfs(self, start_node, target_node):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (start:Location {name: $start_node})
                MATCH (end:Location {name: $target_node})
                CALL gds.bfs.stream('myGraph', {
                    sourceNode: id(start),
                    targetNodes: [id(end)]
                })
                YIELD path
                RETURN [node in nodes(path) | {name: toInteger(node.name)}] AS path
                LIMIT 1
            """, start_node=start_node, target_node=target_node)
            
            paths = list(result)
            
            if paths and paths[0]["path"]:
                return [{"path": paths[0]["path"]}]
            else:
                return [{"path": []}]

    def pagerank(self, max_iter, weight_property):
        with self.driver.session() as session:
            result = session.run("""
                CALL gds.pageRank.stream('myGraph', {
                    maxIterations: $max_iter,
                    relationshipWeightProperty: $weight_property
                })
                YIELD nodeId, score
                WITH gds.util.asNode(nodeId) AS node, score
                ORDER BY score DESC
                RETURN node.name AS name, score
            """, max_iter=max_iter, weight_property=weight_property)

            all_results = list(result)
            return [
                {"name": int(all_results[0]["name"]), "score": all_results[0]["score"]},
                {"name": int(all_results[-1]["name"]), "score": all_results[-1]["score"]}
            ]

