
import dbi
import pyodbc as odbc
from os.path import dirname, isfile
from genshi.template import TemplateLoader, TextTemplate
from util import joinPath

_SqlTemplateDir = joinPath(dirname(__file__), 'sql')
_SqlLoader = TemplateLoader(_SqlTemplateDir, 
                            default_class=TextTemplate,
                            auto_reload=True)

def _findSql(session, classOrModuleName, methodName, *args, **kwds):
    vendor = session.getDbVendorName()
    fileName = '%s.%s.%s.sql' % (classOrModuleName, methodName, vendor)
    if not isfile(joinPath(_SqlTemplateDir, fileName)):
        fileName = '%s.%s.sql' % (classOrModuleName, methodName)
    
    return _SqlLoader.load(fileName) \
                     .generate(args=args, **kwds) \
                     .render('text')

def sql(mf):
    def decorator(f):
        def _newf(*args, **kwds):
            self = args[0]
            
            session = self.session
            className = self.__class__.__name__
            m = f(*args, **kwds)
            if isinstance(m, dict):
                kwds.update(m)
            kwds['this'] = self
            sql = _findSql(session, className, f.func_name, *args, **kwds)
            return getattr(session.db(), mf.func_name)(sql)
        return _newf
    return decorator

@sql
def select(f): pass

@sql
def selectSingle(f): pass

@sql
def selectAll(f): pass

@sql
def execute(f): pass

class Connection(object):
    def __init__(self, connectString):
        self._connectString = connectString
        self._con = odbc.connect(connectString)
    
    def _execute(self, sql):
        cursor = self._con.cursor()
        try:
            cursor.execute(sql)
        except (dbi.dataError,
                dbi.integrityError,
                dbi.internalError,
                dbi.opError,
                dbi.progError), details:
            raise DatabaseError(details, sql)
        return cursor
    
    def select(self, sql):
        cursor = self._execute(sql)
        single = len(cursor.description) == 1
        for row in iter(lambda: cursor.fetchone(), None):
            yield row[0] if single else row        
    
    def selectAll(self, sql):
        cursor = self._execute(sql)
        single = len(cursor.description) == 1
        results = cursor.fetchall()
        return results if not single else [ row[0] for row in results ]
    
    def selectSingle(self, sql):
        cursor = self._execute(sql)
        try:
            return cursor.fetchone()[0]
        except TypeError:
            return None
    
    def execute(self, sql):
        if self._execute(sql) == -1:
            raise DatabaseError('execute returned -1', sql)
    
    def __getattr__(self, attr):
        return getattr(self._con, attr)