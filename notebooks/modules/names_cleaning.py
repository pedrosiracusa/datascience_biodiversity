
# coding: utf-8

# In[ ]:




# # Names atomization

# In[32]:

import re

def namesFromString( namesStr, delim=';', unique=False, preserveOrder=False ):
    """
    Atomizes names in a names string using specified delimiters. 
    
    Parameters
    ----------
    namesStr : str
        Names string to be atomized.
        
    delim : str or list, default ';'
        Delimiter that is used to separate names in the names string. 
        If a list of delimiters is passed they will be concatenated
        into a regular expression for splitting.
    
    unique : bool, default False
        If set to true, returned values are unique. 
    
    preserveOrder : bool, default False
        If True the order in which names appear in the string is 
        guaranteed to be preserved in the list (slower).
        
    Returns
    -------
        A list with the names extracted from the string.
    """
    if type(delim)==str:
        namesSplit = namesStr.split(delim)
        
    elif type(delim)==list:
        delim = [ '\|' if i=='|' else i for i in delim ] 
        namesSplit = re.split( '|'.join( c for c in delim ), namesStr)
    
    namesList = [ n for n in [ name.strip() for name in namesSplit ] if n!='' ]
    
    if unique:
        if not preserveOrder: return list(set(namesList))
        else:
            namesCounts = dict( (n,0) for n in namesList )
            unique_namesList = []
            for n in namesList:
                if namesCounts[n]: continue
                namesCounts[n]+=1
                unique_namesList.append(n)
        return unique_namesList   
    
    return namesList


# In[40]:

def atomizeNames( col, operation=namesFromString, ignoreNames=None ):
    """
    Applies an atomization operation on a names column, which must be a pandas Series object. 
    The atomized names at each row are stored as a list.
    
    Parameters
    ----------
    col : pd.Series
        Names column to be atomized.
        
    operation : function
        The atomization operation to be applied to the names column
        
    Returns
    -------
        A pandas Series with lists of atomized names.
    """
    return col.apply( operation )


# ---

# # Names normalization

# In[59]:

import unicodedata, string

def normalize(name, normalizationForm='NFKD'):
    name = name.lower() # to lowecase
    name = name.replace('.','') # remove periods
    name_ls = tuple( part.strip() for part in name.split(',') ) # split and strip names into tuples

    normalize = lambda s: ''.join( x for x in unicodedata.normalize(normalizationForm, s) if x in string.ascii_letters ) # remove accents
    name_ls = tuple( normalize(name) for name in name_ls )
    
    return ','.join(name_ls)


# ---

# # The Names Index

# In[60]:

def getNamesIndexes( df, atomizedNamesCol, namesMap=None ):
    # split_names_col is a column with names already split
    namesIndexes = dict( (name,[]) for name in namesMap.values() )
    for i,names in df[atomizedNamesCol].iteritems():
        for name in names:
            if namesMap is not None:
                try:
                    namesIndexes[namesMap[name]].append(i)
                except KeyError:
                    pass
            else:
                namesIndexes[name].append(i)
            
    return namesIndexes


# ---

# # The Names Map class

# In[ ]:

from copy import deepcopy

class NamesMap:
    _map=None
    _normalizationFunction=None
    _remappingDict=None
    
    def __init__(self, names, normalizationFunction):
        self._normalizationFunction = normalizationFunction
        self._map = dict( (n, self._normalizationFunction(n)) for n in names )
        return
    
    def clearMap(self):
        self._map={}
        return
    
    def remap(self, remappingDict):
        if self._remappingDict is None:
            self._remappingDict = remappingDict
        else:
            self._remappingDict.update(remappingDict)
        return     
    
    def insertNames(self, names, normalizationFunction=None, rebuild=False):
        # if rebuild is true, the entire map (but not the remapping dict) is rebuilt from scratch
        if rebuild==True:
            self.clearMap()
        if normalizationFunction is None: 
            normalizationFunction = self._normalizationFunction
        self._map.update( dict( (n,normalizationFunction(n)) for n in names ) )
        return
    
    def getMap(self, remap=True):
        # Returns a COPY of the map
        # If remap is set to true, some remapping occurrs
        res = deepcopy(self._map)
        if remap and self._remappingDict is not None:
            getNamesPrimitives = lambda n: ( name for name,norm in self._map.items() if norm == n )
            for n,t in ( (n,t) for s,t in self._remappingDict.items() for n in getNamesPrimitives(s) ):
                res[n]=t
        return res
    
    def getNormalizedNames(self, remap=True):
        return sorted(list(set(self.getMap(remap=remap).values())))
    
    def getNamePrimitives(self, n):
        nmap = self.getMap()
        return [ name for name,norm in nmap.items() if norm == n ]
        

