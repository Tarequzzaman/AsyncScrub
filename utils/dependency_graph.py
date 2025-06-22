from sqlalchemy import MetaData, Table
from typing import List
import networkx as nx

def get_insertion_order(metadata: MetaData) -> List[Table]:
    """
    Returns a list of SQLAlchemy Tables in FK-safe insert order
    using topological sorting via a directed graph.

    Parent tables come before child tables that depend on them.
    """
    tables = metadata.tables.values()
    graph = nx.DiGraph()

    # Add all table nodes
    for table in tables:
        graph.add_node(table.name, table=table)

    # Add foreign key edges (parent → child)
    for table in tables:
        for column in table.columns:
            for fk in column.foreign_keys:
                parent = fk.column.table.name
                child = table.name
                graph.add_edge(parent, child)

    try:
        sorted_table_names = list(nx.topological_sort(graph))
        return [metadata.tables[name] for name in sorted_table_names]
    except nx.NetworkXUnfeasible:
        raise RuntimeError("❌ Circular foreign key dependencies detected.")
