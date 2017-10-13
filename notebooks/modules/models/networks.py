import networkx
import itertools
from collections import Counter

class CoworkingNetwork(networkx.Graph):
    """
    Class for coworking networks. Extends networkx Graph class.
    
    Parameters
    ----------
    namesSets : iterable
        An iterable of iterables containing names used to compose cliques 
        in the network.

    weighted : bool
        If set to True the resulting network will have weighted edges. Default False.
        
    namesMap : NamesMap
        A NamesMap object for normalizing nodes names.
        
    Examples
    --------
    >>> namesSets = [ ['a','b','c'], ['d','e'], ['a','c'] ]
    >>> CoworkingNetwork( namesSets, weighted=True).edges(data=True)
    [('b', 'a', {'weight': 1}),
     ('b', 'c', {'weight': 1}),
     ('a', 'c', {'weight': 2}),
     ('e', 'd', {'weight': 1})]
    
    >>> CoworkingNetwork( namesSets ).edges(data=True)
    [('b', 'a', {}), 
     ('b', 'c', {}), 
     ('a', 'c', {}), 
     ('e', 'd', {})]
    """
    def __init__(self, namesSets, weighted=False, namesMap=None):
        super().__init__()
        
        if namesMap:
            nmap = namesMap.getMap()
            namesSets = [ [ nmap[n] for n in nset ] for nset in namesSets ]
            
        cliques = map( lambda n: itertools.combinations(n,r=2), namesSets )
        edges = [ e for edges in cliques for e in edges ]
        self.add_edges_from(edges)
        
        if weighted:
            edges_weights = Counter(edges)

            for (u,v),w in edges_weights.items():
                try:
                    self[u][v]['weight'] += w
                except:
                    self[u][v]['weight'] = w
        
        return
