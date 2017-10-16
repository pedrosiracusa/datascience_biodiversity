"""
Names Cleaning module
"""

import json, re, string, unicodedata
from collections import Counter
from copy import deepcopy
from warnings import warn





# =================
# Names Atomization
# -----------------

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


def atomizeNames( col, operation=None, replaces=None ):
    """
    Applies an atomization operation on a names column, which must be a pandas Series. 
    The atomized names at each row are stored as a list.
    
    Parameters
    ----------
    col : pandas.Series
        Names column to be atomized.
        
    operation : function
        The atomization operation to be applied to the names column
        
    replaces:
        A list of 2-tuples (srclst, tgt), where srclst is a list of names to be replaced by tgt.
        The element tgt can be either a string or a function which results in a string.
        
    Returns
    -------
        A pandas Series with lists of atomized names.
    """
    if replaces is not None:
        replacesDict = dict( (src, tgt(src)) if callable(tgt) else (src,tgt) for (srclst, tgt) in replaces for src in srclst )
        col = col.replace( replacesDict )
        
    col_atomized = col.apply( operation )
    return col_atomized


def getNamesList( col, with_counts=False, orderBy=None ):
    """
    Gets a list of names from an atomized names column.
    
    Parameters
    ----------
    col : pandas.Series
        Atomized names column from which to retrieve names.
    
    with_counts : bool
        If set to True the result includes the number of records by each collector.
    
    orderBy : str
        If some rule is specified, the resulting list is sorted. Rules can be either
        to sort alphabetically ('alphabetic') or by the number of records by each
        collector ('counts').   
    """
    
    if orderBy not in [ None, "alphabetic", "counts"]:
        raise ValueError("Invalid argument for 'orderBy': {}".format(orderBy))
    
    if with_counts or orderBy=="counts":
        l = [ (n,c) for (n,c) in Counter( n for nlst in col for n in nlst ).items() ] 
        if orderBy=="alphabetic":
            return sorted( l, key=lambda x: x[0] )
        elif orderBy=="counts":
            sorted_l = sorted( l, key=lambda x: x[1], reverse=True )
            if with_counts:
                return sorted_l
            else:
                return [ n for (n,c) in sorted_l ]
        else:
            return l
                
    else:
        if orderBy=="alphabetic":
            return sorted(list(set( n for nlst in col for n in nlst )))
        else:
            return list(set( n for nlst in col for n in nlst ))

class NamesAtomizer:
    
    def __init__(self, atomizeOp, replaces=None):
        """
        The NamesAtomizer is built with an atomizing operation to be defined
        as the instance's default and an optional list with names to be replaced.
        Names to be replaced must be passed in a list of tuples, in any of the 
        following ways:
        
        >> rep = [('n1', 'correct_n1'), ('n2', 'correct_n2')]
        or
        >> rep = [(['n1','n1_2], 'correct_n1'), (['n2'], 'correct_n2')]
        or
        >> expr1 = lambda x: x.replace(';', '_')
        >> expr2 = lambda x: x.replace('&', '_')
        >> rep = [(['n1;1', 'n1;2'], expr1), (['n2&1', 'n2&2'], expr2)]
        
        Note that if you pass an expression as the second item of the tuple this expression
        must evaluate in a string!
        """
        self._replaces = self._buildReplaces(replaces)
        self._operation = atomizeOp
        self._cache = None

    def _buildReplaces(self, replacesList):
        """
        Builds a replaces dict from a list. The input list must contain tuples 
        in which the first element is a list of names strings that must be replaced
        by the string in the tuple's second element. The second element can alternatively
        be an expression that results in a names string.
        """
        res = {}
        if replacesList is None:
            return res
        
        for srcs,tgt in replacesList:
            if isinstance(srcs, str):
                src = srcs
                res.update( {src:tgt(src)} if callable(tgt) else {src:tgt})
                
            elif isinstance(srcs, (list,tuple,set)):
                for src in srcs:
                    res.update( {src:tgt(src)} if callable(tgt) else {src:tgt} )

            else:
                raise ValueError("Invalid value '{0}' in '({0},{1})'. Must be either string or iterable".format(srcs, tgt))
                
        return res                 
    
    def atomize(self, col, operation=None, withReplacing=True, cacheResult=True):
        """
        This method takes a column with names strings and atomizes them
        
        Parameters
        ----------
        
        col : pandas.Series
            A column containing names strings to be atomized.   
        
        operation : function
            If an operation is passed in it is used to atomize the column instead
            of the instance's default operation.
        
        withReplacing : bool, default True
            If set to True some names replacing is performed before atomization. 
        
        cacheResult : bool, default True
            If set to True the resulting series is cached for later use.
        """
        if operation is None:
            operation = self._operation
        
        if withReplacing:
            col=col.replace(self._replaces)
            
        atomizedCol = col.apply(operation)
        if cacheResult:
            self._cache = (col, atomizedCol)
        return atomizedCol
    
    def addReplaces(self, replacesList):
        replacesDict = self._buildReplaces(replacesList)
        self._replaces.update(replacesDict)
    
    def write_replaces(self, filename):
        """
        Writes replaces to a json file
        """
        with open(filename,'w') as f:
            d = {'_replaces':self._replaces}
            json.dump(d, f, sort_keys=True, indent=4, ensure_ascii=False)
            
    def read_replaces(self, filepath, update=True):
        """
        Reads replaces from a json file
        """
        with open(filepath, 'r') as f:
            data = json.load(f)
            if update:
                self._replaces.update( data['_replaces'] )
            else:
                self._replaces = data['_replaces']
        
    def getCachedNames(self, namesToFilter=['et al.'], sortingExp=lambda x: [len(x[0]),-x[2]]):
        """
        This method uses data in the instance's cache.
        Returns atomized names from the instance's cache. Names are associated to 
        their original namestring as well as the number of records they appear 
        in the dataset. The result is structured as a 3-tuple, with elements in 
        the same order stated above.
        
        Parameters
        ----------
        
        namesToFilter : list
            Names that should be ignored by the method. By default it ignores 'et al.'.
        
        sortingExp : function
            An expression to be passed as key to sort the final result.
            
        Returns
        -------
        
        A 3-tuple (u,v,w) where:
          u = atomized name;
          v = original name string that was used to atomize names;
          w = count of the total occurrences of an atomized name in the dataset.
        """
        c = self._cache
        l = [ (n,nstr) for nstr,norm in zip(c[0],c[1]) for n in norm if n not in namesToFilter ]
        ctr = Counter(i[0] for i in l)
        return sorted([ (u,v,ctr[u]) for u,v in set(l) ],key=sortingExp)


# ===================
# Names normalization
# -------------------

def normalize(name, normalizationForm='NFKD'):
    """
    A simple normalization function.
    """
    name = name.lower() # to lowecase
    name = name.replace('.','') # remove periods
    name_ls = tuple( part.strip() for part in name.split(',') ) # split and strip names into tuples

    normalize = lambda s: ''.join( x for x in unicodedata.normalize(normalizationForm, s) if x in string.ascii_letters ) # remove accents
    name_ls = tuple( normalize(name) for name in name_ls )
    
    return ','.join(name_ls)


# ------------------
# The NamesMap class
# ------------------

class NamesMap:
    """
    The class which describes NamesMap objects. Names maps store both name primitives and normalized 
    names. Primitives are the "original names", as they're given as input to the class constructor. 
    When the class is instanced each name primitive is mapped to its normalized form through a
    normalization function. Normalized maps can then be remapped to other names by following a 
    remapping index.
    """
    
    def __init__(self, names, normalizationFunc, remappingIndex=None, *args, **kwargs):
        """
        The NamesMap constructor
        
        Parameters
        ----------
        names : list
            A list with names to be normalized
        
        normalizationFunc : function
            A function or expression for names normalization
            
        remappingIndex : dict
            A dictionary with mapped names to initialize the instance's remapping index
        """
        self._normalizationFunc = normalizationFunc
        
        load_map_prim_norm = kwargs.get('_map_prim_norm',None)
        load_remappingIndex = kwargs.get('_remappingIndex',None)
        
        normNamesDict = lambda nlst: dict( (n,self._normalizationFunc(n)) for n in nlst )
        self._map_prim_norm = normNamesDict(names) if load_map_prim_norm is None else load_map_prim_norm 
        self._remappingIndex = remappingIndex if load_remappingIndex is None else load_remappingIndex
    

    def _getRef(self,n):
        """
        Follows all chained references for a name in the remapping index
        
        Parameters
        ----------
        n : str
            The name to be de-referenced
        """
        start=n
        chain=[]
        remappingKeys = self._remappingIndex.keys()
        while n in remappingKeys:
            chain.append(n)
            n = self._remappingIndex[n]
            if n in chain:
                chain.append(n)
                raise RuntimeError("Loopback detected", start, chain)
        return n
    
    
    def _get_loopback_inconsistencies(self):
        """
        Detects loopbacks in in mapping chains
        """
        inconsistencies = {}
        for k in self._remappingIndex.keys():
            try:
                self._getRef(k)
                
            except RuntimeError as e:
                inconsistencies['mes'] = inconsistencies.get('mes',[]) + [e.args[0]]
                inconsistencies['key'] = inconsistencies.get('key',[]) + [e.args[1]]
                inconsistencies['chain'] = inconsistencies.get('chain',[]) + [e.args[2]]
        
        if len(inconsistencies)==0:
            return None
        else:
            return inconsistencies
    
    
    def _remove_selfloops(self):
        keys_to_remove = [ k for k in self._remappingIndex.keys() if k==self._remappingIndex[k]]       
        for k in keys_to_remove:
            self._remappingIndex.pop(k)
                  
            
    def getInconsistencies(self, prettyPrint=True):
        d = {}
        d['loopback_inconsistencies'] = self._get_loopback_inconsistencies()
        
        if any( True if v is not None else False for v in d.values()  ):
            if prettyPrint:
                mes = "INCONSISTENCIES\n===============\n"

                # loopback inconsistencies
                if d['loopback_inconsistencies'] is not None:
                    mes += "Loopback Inconsistencies\n"
                    data = list(zip( *d['loopback_inconsistencies'].values() ))
                    dataStr = lambda t: "  > {}: Starting from key '{}' got chain {}\n".format(*t)
                    mes += ''.join( dataStr(t) for t in data )
                    mes += '---------------'

                return mes
            
            return d
        
        return None
                
        
    def getMap(self, remap=True):
        """
        Returns a COPY of the names map.
        
        Parameters
        ----------
        remap : bool, default True
            If set to True, the names map is buit by first de-referencing
            remaps in the remapping index. Otherwise all remaps will not
            be considered for building the names map.
        """
        res = deepcopy(self._map_prim_norm)
        if remap and self._remappingIndex is not None:
            for s,t in self._map_prim_norm.items():
                try:
                    res[s] = self._getRef(t)
                except RuntimeError as e:
                    raise(e)
        return res
    
    def addNames(self, names, normalizationFunc=None, updateExistingKeys=False):
        """
        Updates the names map using a list of primitive names, which are stored as references
        to their normalized forms.
        
        Parameters
        ----------
        names : list
            List with names to be inserted or updated in the map. They are stored as primitives,
            mapping to their normalized forms.
            
        normalizationFunc : function
            By default the instance's normalization function is used to normalize the new names 
            primitives. If an alternative expression is passed in it will be used to perform the
            normalization instead.
            
        updateExistingKeys : bool, default False
            By default only names that still do not exist as keys in the primitives-normalized 
            names map are normalized and updated. If set to True all input names will be 
            updated in the names map.  
        """
        normFunc = self._normalizationFunc if normalizationFunc is None else normalizationFunc
        d = dict( ( n,normFunc(n) ) for n in names )
        
        if not updateExistingKeys:
            d = dict( (k,v) for k,v in d.items() if k not in self._map_prim_norm.keys() )
        
        self._map_prim_norm.update(d) 
            
    
    def remap(self, remaps, fromScratch=False):
        """
        Updates the remapping dictionary using a list of tuples as input.
        
        Parameters
        ----------
        remaps : list of tuples
            Remaps values from tuples (s,t), where a normalized name s remaps to a
            normalized name t.
        
        fromScratch : bool
            If set to True the remapping dict becomes the one passed in. All other previous
            remaps are discarded.
            
        Note
        ----
        If the list of tuples passed in contains duplicated keys a warning is issued, and the
        latest (key,value) pair is the one which will persist.
        """        
        # check for duplicated keys
        duplicatedKeys = [ s for s,cnts in Counter( s for s,t in remaps ).items() if cnts>1 ]
        if len(duplicatedKeys)>0: 
            warningMsg = "Some keys from input are duplicated: {}.".format(str(duplicatedKeys))
            warn(warningMsg)
        
        # update remapping index
        if fromScratch: self._remappingIndex=None
        if self._remappingIndex is None: self._remappingIndex={}
        
        for s,t in remaps:
            self._remappingIndex[s] = t
        
        self._remove_selfloops()
        return self.getInconsistencies()
    
    
    def setEndpoint(self, key):
        """
        This method sets a name as the endpoint of a chain. This method is used for resolving loopbacks
        in the remapping chain. The name is set to be the latest reference in the chain, and therefore
        does not map to any other name.
        
        Parameter
        ---------
        key : str
            The name to be set as the latest reference.
        """
        return self._remappingIndex.pop(key)
    
    
    def write_toJson(self, filename, flatten=False):
        """
        Creates a json file to store a NamesMap's primitive-to-normalized names map and remapping index.
        Data is stored as json object arguments `_map_prim_norm` and `_remappingIndex`.
        
        Parameters
        ----------
        filename : str
            Path to the file to be created.
        
        flatten : bool, default False
            If set to true, all remappings are consolidated into the `_map_prim_norm` map. In other words,
            the remapping index is used to assign each name primitive to its final reference. All references
            are then removed from the remapping index.
        
        """
        json_dict = dict([ ('_map_prim_norm', self.getMap() if flatten else self._map_prim_norm),
                           ('_remappingIndex', {} if flatten else self._remappingIndex) ])
        
        with open(filename, 'w') as output_file:
            json.dump( json_dict, output_file, sort_keys=True, indent=4, ensure_ascii=False)           

            

def read_NamesMap_fromJson(filepath, normalizationFunc=None):
    """
    Creates a NamesMap instance from a json file containing both a primitive-to-normalized names map
    and a remapping index. The json object must have both attributes `map_prim_norm` and 
    `_remappingIndex`, which stores data used to instance NamesMap class.
    
    Parameters
    ----------
    filepath : str
        Path to the json file containing the map
        
    normalizationFunc : function
        A normalization function to be passed to the NamesMap constructor. If it is 
        not set a warning is issued, as the NamesMap will not be assigned to any
        normalization rule.
    """
    if normalizationFunc is None:
        warn("A names map was created without a normalization function!")
        
    with open(filepath,'r') as f:
        d = json.load(f)
        nm = NamesMap( names=None, normalizationFunc=normalizationFunc, 
                       _map_prim_norm=d['_map_prim_norm'], 
                       _remappingIndex=d['_remappingIndex'])
    
    return nm



# ==============
# Names indexing
# --------------

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
