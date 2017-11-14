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
    def __init__(self, data=None, namesSets=None, weighted=False, namesMap=None, **attr):
       
        if namesSets is not None:
            if namesMap:
                nmap = namesMap.getMap()
                namesSets = [ [ nmap[n] for n in nset ] for nset in namesSets ]

            cliques = map( lambda n: itertools.combinations(n,r=2), namesSets )
            data = [ e for edges in cliques for e in edges ]

        super().__init__(data=data,**attr)

        if weighted:
            edges = data
            edges_weights = Counter(edges)

            for (u,v),w in edges_weights.items():
                try:
                    self[u][v]['weight'] += w
                except:
                    self[u][v]['weight'] = w



class SpeciesCollectorsNetwork(networkx.Graph):
    """
    Class for Species-collectors networks
    
    Parameters
    ----------
    
    A dataframe with two columns: an atomized collectors names 
    """
    def __init__(self, data=None, species=None, collectorsNames=None, weighted=False, namesMap=None, **attr):
        
        self._speciesBag_matrix = None
        
        set_bipartite_attr=False
        if species is not None and collectorsNames is not None:
            if namesMap:
                nmap = namesMap.getMap()
                collectorsNames = [ [ nmap[n] for n in nset ] for nset in collectorsNames ]
            
            # build edges
            if len(species)==len(collectorsNames):
                species = list(species)
                collectorsNames = list(collectorsNames)
                
                data = [ (sp,col) for i,sp in enumerate(species) for col in collectorsNames[i] ]
                set_bipartite_attr=True

        super().__init__(data=data,**attr)
        
        if set_bipartite_attr:
            networkx.set_node_attributes( self, 'bipartite', dict( (n,1) for n in species) )
            networkx.set_node_attributes( self, 'bipartite', dict( (n,0) for cols in collectorsNames for n in cols) )
            
        if weighted:
            edges = data
            edges_weights = Counter(edges)

            for (u,v),w in edges_weights.items():
                try:
                    self[u][v]['weight'] += w
                except:
                    self[u][v]['weight'] = w    
    
    def _buildSpeciesBagMatrix( self, col_sp_order=None ):
        col_sp_order=(sorted(self.getCollectorsNodes()),sorted(self.getSpeciesNodes())) if col_sp_order is None else col_sp_order
        m = networkx.bipartite.biadjacency_matrix(self,row_order=col_sp_order[0],column_order=col_sp_order[1])
        self._speciesBag_matrix = (*col_sp_order,m)
                    
    def getSpeciesNodes(self,data=False):
        return [ (n,d) if data==True else n for n,d in self.nodes(data=True) if d['bipartite']==1 ]
        
    def getCollectorsNodes(self,data=False):
        return [ (n,d) if data==True else n for n,d in self.nodes(data=True) if d['bipartite']==0 ]
    
    def getSpeciesBag( self, collectorName ):
	"""
	Returns
	-------
	A tuple (colList, similarityVector), where the first element is the list will all collectors and
	the second is the similarity vector for the input collector.
	"""
        if self._speciesBag_matrix is None:
            self._buildSpeciesBagMatrix()
            
        colList, spList, m = self._speciesBag_matrix
        i = colList.index(collectorName)
        return (colList, m.getrow(i))


