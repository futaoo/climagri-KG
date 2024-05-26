from neo4j import GraphDatabase
from tqdm import tqdm


class Graph:
    def __init__( self, uri, user, password ):
        self.driver = GraphDatabase.driver( uri, auth = (user, password) )

    def close( self ):
        self.driver.close()

    def create_and_return_nodes( self, df1, label, name ):
        properties = dict( df1 )
        properties[ "name" ] = name
        with self.driver.session() as session:
            result = session.write_transaction( self._create_node, properties, label )

    def create_relationship_by_properties( self, label1, label2, properties1, properties2, rel_type ):
        with self.driver.session() as session:
            match_clause = f"MATCH (n1:{label1}), (n2:{label2}) "
            where_clause = "WHERE " + " AND ".join(
                [ f"n1.{prop1} = n2.{prop2}" for prop1, prop2 in zip( properties1, properties2 ) ] ) + " "
            create_clause = f"CREATE (n1)-[:{rel_type}]->(n2)"
            query = match_clause + where_clause + create_clause
            print(query)
            result = session.run( query )

    @staticmethod
    def _create_node( tx, properties, label ):
        result = tx.run( "CREATE (a:`{}`) SET a = $properties RETURN id(a)".format( label ), properties = properties )
        return result.single()[ 0 ]



