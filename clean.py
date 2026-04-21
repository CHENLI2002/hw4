from neo4j import GraphDatabase

external_ip = "35.193.180.181"

driver = GraphDatabase.driver(f"bolt://{external_ip}:7687", auth=("neo4j", "12345678"))

# test connection
with driver.session() as session:
        result = session.run("MATCH (n) DETACH DELETE n")
        print("deleted all nodes")