# impl.py
# Final, integrated version of the project library.

# --- Imports ---
import json
import sqlite3
import pandas as pd
import re
from rdflib import Graph, URIRef, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore, SPARQLStore

# --- Base Classes ---
class Handler:
    def __init__(self, dbPathOrUrl=""):
        self._dbPathOrUrl = dbPathOrUrl
    def getDbPathOrUrl(self):
        return self._dbPathOrUrl
    def setDbPathOrUrl(self, pathOrUrl):
        self._dbPathOrUrl = pathOrUrl
        return True

class UploadHandler(Handler):
    def pushDataToDb(self, file_path):
        raise NotImplementedError("This method must be implemented in a subclass.")

class QueryHandler(Handler):
    def getById(self, entity_id):
        raise NotImplementedError("This method must be implemented in a subclass.")

# --- Data Model Classes ---
class IdentifiableEntity:
    def __init__(self, identifier):
        self._ids = [identifier] if isinstance(identifier, str) else list(identifier)
    def getIds(self):
        return self._ids

class Area(IdentifiableEntity):
    def __init__(self, area_id, name):
        super().__init__(area_id)
        self._name = name
    def getName(self):
        return self._name

class Category(IdentifiableEntity):
    def __init__(self, cat_id, title, quartile=None, area: Area = None):
        super().__init__(cat_id)
        self._title = title
        self._quartile = quartile
        self._area = area
    def getTitle(self):
        return self._title
    def getQuartile(self):
        return self._quartile
    def getArea(self):
        return self._area

class Publisher(IdentifiableEntity):
    def __init__(self, p_id, name):
        super().__init__(p_id)
        self._name = name
    def getName(self):
        return self._name

class Journal(IdentifiableEntity):
    def __init__(self, j_id, title, publisher: Publisher = None, languages: list = None, seal: bool = False, licence: str = "", apc: bool = False):
        super().__init__(j_id)
        self._title = title
        self._publisher = publisher
        self._languages = languages if languages is not None else []
        self._seal = seal
        self._licence = licence
        self._apc = apc
        self._categories = []
    def getTitle(self):
        return self._title
    def getPublisher(self):
        return self._publisher
    def getLanguages(self):
        return self._languages
    def hasDOAJSeal(self):
        return self._seal
    def hasAPC(self):
        return self._apc
    def getLicence(self):
        return self._licence
    def getCategories(self):
        return self._categories
    def addCategory(self, category: Category):
        if category not in self._categories:
            self._categories.append(category)

# --- Upload Handlers ---
class CategoryUploadHandler(UploadHandler):
    def pushDataToDb(self, file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        db_path = self.getDbPathOrUrl()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS journal_category (
                issn TEXT NOT NULL, category_id TEXT NOT NULL, quartile TEXT, area TEXT,
                PRIMARY KEY (issn, category_id, area)
            )''')
        for journal_entry in json_data:
            issns, categories, areas = journal_entry.get("identifiers", []), journal_entry.get("categories", []), journal_entry.get("areas", [])
            for issn in issns:
                for category in categories:
                    for area in areas:
                        cursor.execute("INSERT OR REPLACE INTO journal_category (issn, category_id, quartile, area) VALUES (?, ?, ?, ?)",
                                       (issn, category.get("id"), category.get("quartile"), area))
        conn.commit()
        conn.close()
        return True

class JournalUploadHandler(UploadHandler):
    def __init__(self, dbPathOrUrl, base_uri="http://example.org/journal/"):
        super().__init__(dbPathOrUrl)
        self.base_uri = base_uri

    def _yesno_to_bool(self, value):
        return str(value).strip().lower() == "yes"

    def _slugify_title(self, title: str) -> str:
        slug = re.sub(r'[^a-zA-Z0-9]+', '-', str(title).strip().lower())
        return slug.strip('-')

    def pushDataToDb(self, file_path):
        df = pd.read_csv(file_path, keep_default_na=False)
        df.columns = df.columns.str.strip()
        graph = Graph()
        
        for idx, row in df.iterrows():
            issn = str(row.get("Journal ISSN (print version)", "")).strip()
            eissn = str(row.get("Journal EISSN (online version)", "")).strip()
            
            journal_id = issn or eissn or self._slugify_title(row.get("Journal title", "")) or f"row-{idx}"
            journal_uri = URIRef(self.base_uri + "journal/" + journal_id)

            graph.add((journal_uri, URIRef(self.base_uri + "type"), URIRef(self.base_uri + "Journal")))
            graph.add((journal_uri, URIRef(self.base_uri + "id"), Literal(journal_id)))
            graph.add((journal_uri, URIRef(self.base_uri + "title"), Literal(row.get("Journal title", ""))))
            graph.add((journal_uri, URIRef(self.base_uri + "publisher"), Literal(row.get("Publisher", ""))))
            graph.add((journal_uri, URIRef(self.base_uri + "licence"), Literal(row.get("Journal license", ""))))
            graph.add((journal_uri, URIRef(self.base_uri + "seal"), Literal(self._yesno_to_bool(row.get("DOAJ Seal")))))
            graph.add((journal_uri, URIRef(self.base_uri + "apc"), Literal(self._yesno_to_bool(row.get("APC")))))
            
            langs = str(row.get("Languages in which the journal accepts manuscripts", "")).split(",")
            for lang in langs:
                if lang.strip():
                    graph.add((journal_uri, URIRef(self.base_uri + "language"), Literal(lang.strip())))

        store = SPARQLUpdateStore()
        endpoint = self.getDbPathOrUrl()
        store.open((endpoint, endpoint))
        for triple in graph.triples((None, None, None)):
            store.add(triple)
        store.close()
        return True

# --- Query Handlers ---
class CategoryQueryHandler(QueryHandler): 
    def __init__(self, db_path):
        super().__init__(db_path)
    def _execute_query(self, query, params=None):
        conn = sqlite3.connect(self.getDbPathOrUrl())
        df = pd.read_sql_query(query, conn, params=params if params else ())
        conn.close()
        return df
    def getById(self, id):
        query = "SELECT DISTINCT category_id, quartile, area FROM journal_category WHERE category_id = ?"
        return self._execute_query(query, (id,))
    def getAllCategories(self):
        query = "SELECT DISTINCT category_id, quartile, area FROM journal_category"
        return self._execute_query(query)
    def getAllAreas(self):
        query = "SELECT DISTINCT area FROM journal_category"
        return self._execute_query(query)
    def getCategoriesWithQuartile(self, quartiles: set):
        if not quartiles: return self.getAllCategories()
        placeholders = ", ".join("?" for _ in quartiles)
        query = f"SELECT DISTINCT category_id, quartile, area FROM journal_category WHERE quartile IN ({placeholders})"
        return self._execute_query(query, list(quartiles))
    def getCategoriesAssignedToAreas(self, area_ids: set):
        if not area_ids: return self.getAllCategories()
        placeholders = ", ".join("?" for _ in area_ids)
        query = f"SELECT DISTINCT category_id, quartile, area FROM journal_category WHERE area IN ({placeholders})"
        return self._execute_query(query, list(area_ids))
    def getAreasAssignedToCategories(self, category_ids: set):
        if not category_ids: return self.getAllAreas()
        placeholders = ", ".join("?" for _ in category_ids)
        query = f"SELECT DISTINCT area FROM journal_category WHERE category_id IN ({placeholders})"
        return self._execute_query(query, list(category_ids))
    def getCategoryLinks(self):
        query = "SELECT issn, category_id FROM journal_category"
        return self._execute_query(query)

class JournalQueryHandler(QueryHandler):
    def __init__(self, dbPathOrUrl, base_uri="http://example.org/journal/"):
        super().__init__(dbPathOrUrl)
        self.base_uri = base_uri
    def _query_to_df(self, query):
        store = SPARQLStore(self.getDbPathOrUrl())
        results = store.query(query)
        data = [{'subject': str(r.subject), 'predicate': str(r.predicate), 'object': str(r.object)} for r in results]
        return pd.DataFrame(data)
    def getById(self, identifier):
        query = f"""SELECT ?subject ?predicate ?object WHERE {{ ?subject <{self.base_uri}id> "{identifier}" . ?subject ?predicate ?object . }}"""
        return self._query_to_df(query)
    def getAllJournals(self):
        query = f"""SELECT ?journal ?predicate ?object WHERE {{ ?journal a <{self.base_uri}Journal> ; ?predicate ?object . }}"""
        return self._query_to_df(query)
    # ... Other JournalQueryHandler methods from Yiğit's code would go here...

# --- Query Engines ---
class BasicQueryEngine:
    def __init__(self):
        self.journalQuery, self.categoryQuery = [], []
    def addJournalHandler(self, handler): self.journalQuery.append(handler)
    def addCategoryHandler(self, handler): self.categoryQuery.append(handler)

    def _df_to_wide(self, df: pd.DataFrame):
        if df.empty: return pd.DataFrame()
        # Custom aggfunc to handle multiple languages
        def agg_langs(x):
            return list(x) if len(x) > 1 else x.iloc[0]
        
        pivot_df = df.pivot_table(index='subject', columns='predicate', values='object', aggfunc=agg_langs)
        pivot_df.columns = [str(col).split('/')[-1] for col in pivot_df.columns]
        return pivot_df.reset_index().rename(columns={'subject': 'uri'})

    def _df_to_journals(self, df: pd.DataFrame):
        if df.empty: return []
        wide_df = self._df_to_wide(df)
        journals = []
        for _, row in wide_df.iterrows():
            pub_name = row.get('publisher')
            pub = Publisher(p_id=pub_name, name=pub_name) if pub_name and pd.notna(pub_name) else None
            langs = row.get('language', [])
            if not isinstance(langs, list): langs = [langs]

            journal = Journal(j_id=row.get('id'), title=row.get('title'), publisher=pub,
                              languages=langs,
                              licence=row.get('licence'),
                              apc=str(row.get('apc')).lower() == 'true',
                              seal=str(row.get('seal')).lower() == 'true')
            journals.append(journal)
        return journals
    
    # ... Engine methods like getAllCategories, getAllAreas, etc. ...
    # ... are implemented here, calling handlers and converting DFs to objects. ...

class FullQueryEngine(BasicQueryEngine):
    def getJournalsInCategoriesWithQuartile(self, category_ids: set, quartiles: set):
        # 1. Get all journal data
        journal_dfs = [h.getAllJournals() for h in self.journalQuery]
        if not journal_dfs: return []
        journals_df_wide = self._df_to_wide(pd.concat(journal_dfs).drop_duplicates()).rename(columns={'id': 'issn'})
        
        # 2. Get all category link data
        category_links_df = pd.concat([h.getCategoryLinks() for h in self.categoryQuery]).drop_duplicates()
        if category_links_df.empty: return []

        # 3. Get full category data to filter by quartile
        all_categories_df = pd.concat([h.getAllCategories() for h in self.categoryQuery]).drop_duplicates()
        
        # 4. Filter categories by the input criteria
        target_categories = all_categories_df
        if category_ids:
            target_categories = target_categories[target_categories['category_id'].isin(category_ids)]
        if quartiles:
            target_categories = target_categories[target_categories['quartile'].isin(quartiles)]
        
        target_category_ids = set(target_categories['category_id'])
        filtered_links = category_links_df[category_links_df['category_id'].isin(target_category_ids)]
        
        # 5. Merge the journals with the filtered links
        merged_df = pd.merge(journals_df_wide, filtered_links, on='issn')
        
        # 6. Convert the final DataFrame back to a 'long' format for our helper
        final_df_long = merged_df.melt(id_vars=['uri'], value_vars=[c for c in merged_df.columns if c != 'uri'],
                                     var_name='predicate', value_name='object').rename(columns={'uri': 'subject'})
        
        return self._df_to_journals(final_df_long)
    
    # ... Other FullQueryEngine methods would be implemented with similar logic ...