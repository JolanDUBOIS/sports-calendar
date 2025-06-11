from __future__ import annotations
import hashlib
from dataclasses import dataclass

import pandas as pd
import networkx as nx # type: ignore

from . import logger


class Entity:
    """ TODO """

    def __init__(self, id: str, source: str):
        """ TODO """
        self.id = id
        self.source = source

    def __eq__(self, other):
        if not isinstance(other, Entity):
            return False
        return self.id == other.id and self.source == other.source

    def __hash__(self):
        return hash((self.id, self.source))

    def __repr__(self) -> str:
        return f"Entity(id={self.id}, source={self.source})"

    def as_series(self) -> pd.Series:
        """ Convert the Entity to a pandas Series. """
        return pd.Series({"source_id": self.id, "source": self.source})

    @classmethod
    def from_row(cls, row: pd.Series, id_col: str, source_col: str) -> Entity:
        """ Create an Entity from a DataFrame row. """
        return cls(id=row[id_col], source=row[source_col])

    @classmethod
    def from_df(cls, df: pd.DataFrame, id_col: str, source_col: str) -> list[Entity]:
        """ Create a list of Entities from a DataFrame. """
        if id_col not in df.columns or source_col not in df.columns:
            logger.error(f"Columns '{id_col}' or '{source_col}' not found in DataFrame.")
            raise KeyError(f"Columns '{id_col}' or '{source_col}' not found in DataFrame.")
        
        return [cls.from_row(row, id_col, source_col) for _, row in df.iterrows()]

@dataclass(frozen=True)
class Edge:
    origin: Entity
    target: Entity
    similarity_score: float

    @property
    def sources(self) -> tuple[str, str]:
        """ Return the sources of the edge as a tuple. """
        return (self.origin.source, self.target.source)

class ConnectedComponent:
    """ TODO """

    def __init__(self, entities: set[Entity]):
        """ TODO """
        self.entities = entities

    def __contains__(self, entity: Entity) -> bool:
        """ Check if an entity is in the connected component. """
        return entity in self.entities

    def __repr__(self) -> str:
        return f"ConnectedComponent(entities={self.entities})"

    def is_valid(self) -> bool:
        """
        Check if the connected component is valid 
        TODO explain more, invalid means the same source appears multiple times.
        """
        sources = {entity.source for entity in self.entities}
        return len(sources) == len(self.entities)

    def generate_id(self) -> str:
        """ Generate a unique hashed ID for the connected component based on its entities. """
        raw = "|".join(sorted(f"{entity.id}_{entity.source}" for entity in self.entities))
        return hashlib.sha256(raw.encode()).hexdigest()

class SimilarityGraph:
    """ TODO """

    def __init__(self, nodes: list[Entity], edges: list[Edge]):
        """ TODO """
        self.nodes = nodes
        self.edges = edges

    def __repr__(self) -> str:
        return f"SimilarityGraph(nodes={self.nodes}, edges={self.edges})"

    def connected_components(self) -> list[ConnectedComponent]:
        """ Group entities into connected components based on edges. """
        G = nx.Graph()
        for node in self.nodes:
            G.add_node(node)

        for edge in self.edges:
            G.add_edge(edge.origin, edge.target, weight=edge.similarity_score)

        logger.debug(f"Graph built with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")

        components = [
            ConnectedComponent(set(nodes))
            for nodes in nx.connected_components(G)
        ]
        return components

class SimilarityTable:
    """ TODO """

    def __init__(self, df: pd.DataFrame):
        """ TODO """
        self.df = df

    def get_best_matches(self, threshold: float = 0) -> SimilarityTable:
        """ TODO """
        _df = self.df.copy()
        _df = _df[_df["similarity_score"] >= threshold]

        best_A = _df.groupby(["id_A", "source_A", "source_B"])["similarity_score"].transform("max")
        best_B = _df.groupby(["id_B", "source_A", "source_B"])["similarity_score"].transform("max")

        best_df = _df[(_df["similarity_score"] == best_A) & (_df["similarity_score"] == best_B)]
        return SimilarityTable(best_df.reset_index(drop=True))

    def as_graph(self) -> SimilarityGraph:
        """ TODO """
        _df = self.df.copy()
        dfA = _df[["id_A", "source_A"]].rename(columns={"id_A": "id", "source_A": "source"})
        dfB = _df[["id_B", "source_B"]].rename(columns={"id_B": "id", "source_B": "source"})
        _df = pd.concat([dfA, dfB], ignore_index=True).drop_duplicates()
        nodes = Entity.from_df(_df, "id", "source")

        edges = [
            Edge(
                Entity(row["id_A"], row["source_A"]),
                Entity(row["id_B"], row["source_B"]),
                row["similarity_score"]
            )
            for _, row in self.df.iterrows()
        ]

        return SimilarityGraph(nodes, edges)

def merge_components(components: list[ConnectedComponent]) -> pd.DataFrame:
    merged_rows = []
    for cc in components:
        canonical_id = cc.generate_id()
        for entity in cc.entities:
            new_row = entity.as_series()
            new_row["id"] = canonical_id
            merged_rows.append(new_row)
    return pd.DataFrame(merged_rows)

def create_mapping_table(sources: dict[str, pd.DataFrame], similarity_table: str, **kwargs) -> pd.DataFrame:
    """ TODO """
    sim_table = SimilarityTable(sources[similarity_table])

    sim_table = sim_table.get_best_matches(threshold=kwargs.get("threshold", 0))
    logger.debug(f"Similarity best matches: \n{sim_table.df.head(20)}")
    sim_graph = sim_table.as_graph()
    logger.debug(f"Similarity graph created with {len(sim_graph.nodes)} nodes and {len(sim_graph.edges)} edges.")
    components = sim_graph.connected_components()
    
    # ---- Debugging ----
    logger.debug(f"Found {len(components)} connected components in the similarity graph.")
    for component in components:
        logger.debug(f"Component: {component}")
    # -------------------

    valid_components = [cc for cc in components if cc.is_valid()]

    merged_df = merge_components(valid_components)
    return merged_df
