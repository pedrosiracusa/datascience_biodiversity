"""
Names Cleaning module
"""

import json, re
import unicodedata, string
from collections import Counter
from copy import deepcopy





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


"""
The NamesMap class
===================
"""
class NamesMap:
    _map=None
    _normalizationFunction=None
    _remappingDict=None
    
    def __init__(self, names, normalizationFunction, *args, **kwargs):  
        self._normalizationFunction = normalizationFunction    
        if 'jsondata' in kwargs:
            self._map = kwargs['jsondata']['_map']
            self._remappingDict = kwargs['jsondata']['_remappingDict']
        else:
            self._map = dict( (n, self._normalizationFunction(n)) for n in names )
        return
    
    def clearMap(self):
        """
        Resets the map to an empty dict
        """
        self._map={}
        return
    
    def remap(self, remappingDict, fromScratch=False, preventOverwriting=True):
        """
        Updates the remapping dictionary.
        
        Parameters
        ----------
        remappingDict : dict
            Remaps values in the map attribute.
        
        fromScratch : bool
            If set to True the remapping dict becomes the one passed in. All other previous
            remaps are discarded.
        
        preventOverwriting : bool
            If set to True (default), then a key cannot be remapped if it already exists
            in the remapping dict.
        """
        remappingDict = deepcopy(remappingDict)
        if fromScratch:
            self._remappingDict = None
            
        if self._remappingDict is None:
            self._remappingDict = remappingDict
        
        else:
            if preventOverwriting:
                existantKeys = self._remappingDict.keys()
                for k in remappingDict.keys():
                    if k in existantKeys:
                        raise ValueError("Cannot overwrite key '{}' in the remapping dict.".format(k))
                    
            self._remappingDict.update(remappingDict)
        return    
    
    def remove_fromRemap(self, key):
        """
        Removes element from the remapping dict
        
        Parameters
        ----------
        Key : dict key
          Key of the element to be removed
          
        Returns
        -------
        The value associated to the removed key
        """
        return self._remappingDict.pop(key)

    def remap_fromJson(self, filepath, fromScratch=True):
        with open(filepath, 'r') as f:
            data = json.load(f)
            remappingDict = data['_remappingDict']
            self.remap(remappingDict,fromScratch)
            return
    
    def setNormalizationFunc(self,normalizationFunction):
        self._normalizationFunction = normalizationFunction
        return
    
    def insertNames(self, names, normalizationFunction=None, rebuild=False):
        # if rebuild is true, the entire map (but not the remapping dict) is rebuilt from scratch
        if rebuild==True:
            self.clearMap()
        if normalizationFunction is None: 
            if self._normalizationFunction is None:
                raise ValueError("a normalization function must be defined")
            normalizationFunction = self._normalizationFunction
        self._map.update( dict( (n,normalizationFunction(n)) for n in names ) )
        return
    
    def getMap(self, remap=True):
        """
        Returns a COPY of the names dictionary
        If remap is set to True the remapping dictionary
        is used to remap some names.
        """
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
    
    def write_toJson(self, filename="names_map.json"):
        json_dict = dict( (k,v) for (k,v) in vars(self).items() if k!='_normalizationFunction')
        with open(filename, 'w') as output_file:
            json.dump( json_dict, output_file, sort_keys=True, indent=4, ensure_ascii=False)
        return
    
    def reportRemappingInconsistencies(self, returnFormatted=True):
        """
        Reports inconsistencies in the remapping dictionary. Possible inconsistencies
        classes are:
          1. Keys that also appear as values;
        """
        inconsistencies_dict = {'keys_in_vals':('These names appear both as keys and values:', [])}
        dictKeys = self._remappingDict.keys()
        dictVals = self._remappingDict.values()
        
        # Keys_in_vals inconsistency
        for k in dictKeys:
            if k in list(dictVals):
                inconsistencies_dict['keys_in_vals'][1].append(k)
        
        if sum( len(vals) for k,(desc,vals) in inconsistencies_dict.items() )==0:
            return None
        
        else:
            if returnFormatted:
                resStr = ""
                for k,(desc, vals) in inconsistencies_dict.items():
                    if len(vals)!=0:
                        resStr += "{}\n{}\n".format(desc, ''.join("=" for i in range(len(desc))) )
                        resStr += ''.join( "  {}\n".format(v) for v in vals )
                        resStr += "{}\n".format(''.join("-" for i in range(len(desc))) ) 
                    return resStr

            else:
                return inconsistencies_dict
            
            
            
            
def read_namesMap(filepath, fileType='json', *args, **kwargs):
    """
    Reads a names map from a file and returns a NamesMap instance.
    Currently only json files are supported.
    
    Note
    ----
    The normalization function cannot be stored in JSON, and therefore it 
    must be passed as an optional keyword argument 'normalizationFunction'. If no 
    normalization function is passed new names cannot be inserted into the map,
    although remapping can still be done.
    """
    if fileType=='json':
        with open(filepath, 'r') as f:
            data=json.load(f)
            nm = NamesMap(names=None, normalizationFunction=kwargs.get('normalizationFunction', None), jsondata=data)
        return nm
    else:
        raise ValueError("Unsupported file type '{}'.".format(fileType))



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
