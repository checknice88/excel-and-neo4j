import pandas as pd
from neo4j import GraphDatabase, Query
#注释处为需要填写的部分

class Neo4jDriver:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_nodes_and_properties(self, data):
        node_properties = {}
        with self.driver.session() as session:
            for index, row in data.iterrows():
                node = row['节点']
                property_value = row['属性'] if pd.notna(row['属性']) else "null"
                label = row['标签']

                node_properties[node] = property_value

                query_str = f"MERGE (n:{label} {{name: $name, property: $prop}})"
                query = Query(query_str)
                result = session.run(query, name=node, prop=property_value)
                print(f"创建节点及属性中... {node} - {property_value} - {label}")

    def create_relationships(self, data):
        with self.driver.session() as session:
            node_label_map = {}
            result = session.run("MATCH (n) RETURN n.name, labels(n)[0]")
            for record in result:
                node_label_map[record["n.name"]] = record["labels(n)[0]"]

            for index, row in data.iterrows():
                node1 = row['节点1']
                node2 = row['节点2']
                relationship = row['联系']

                label1 = node_label_map.get(node1)
                label2 = node_label_map.get(node2)

                if not label1:
                    print(f"Node {node1} does not exist. Skipping relationship creation.")
                    continue
                if not label2:
                    print(f"Node {node2} does not exist. Skipping relationship creation.")
                    continue

                query_str = f"MATCH (n1:{label1} {{name: $name1}}), (n2:{label2} {{name: $name2}}) " \
                            f"MERGE (n1)-[r:{relationship}]->(n2) RETURN r"
                query = Query(query_str)
                result = session.run(query, name1=node1, name2=node2)
                relationship_result = result.single()
                if relationship_result:
                    print(f"创建节点的联系中... {node1} -[{relationship}]-> {node2}")
                else:
                    print(f"Failed to create relationship: {node1} -[{relationship}]-> {node2}")

    def count_nodes(self):
        with self.driver.session() as session:
            result = session.run("MATCH (n) RETURN count(n) as node_count")
            return result.single()["node_count"]

    def count_relationships(self):
        with self.driver.session() as session:
            result = session.run("MATCH ()-[r]->() RETURN count(r) as relationship_count")
            return result.single()["relationship_count"]


def main():
    # 请替换为你的 Neo4j 数据库连接信息
    uri = "example"
    user = "neo4j"
    password = "example"

    # 请替换为你的第一个 Excel 文件路径（用于创建节点和属性）
    excel_file_path_nodes = "/your_pass/node.xlsx"
    # 请替换为你的第二个 Excel 文件路径（用于创建关系）
    excel_file_path_relationships = "/your_pass/edge.xlsx"


    df_nodes = pd.read_excel(excel_file_path_nodes)
    df_relationships = pd.read_excel(excel_file_path_relationships)

    neo4j_driver = Neo4jDriver(uri, user, password)
    neo4j_driver.create_nodes_and_properties(df_nodes)
    neo4j_driver.create_relationships(df_relationships)

    node_count = neo4j_driver.count_nodes()
    relationship_count = neo4j_driver.count_relationships()

    print(f"共创建了: {node_count} 个节点")
    print(f"共创建了: {relationship_count} 条边")

    neo4j_driver.close()


if __name__ == "__main__":
    main()