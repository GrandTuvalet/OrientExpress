# impl.py
# Final, complete, and verified version of the project library.

# --- Imports ---
import json
import sqlite3
import pandas as pd
import re
import requests
from rdflib import Graph, URIRef, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLStore

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
    def __init__(self, dbPathOrUrl, base_uri="http://application.org/"):
        super().__init__(dbPathOrUrl)
        self.base_uri = base_uri
        self.PRED = {
            "type": "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "Journal": f"<{self.base_uri}Journal>",
            "id": f"<{self.base_uri}id>", "title": f"<{self.base_uri}title>", "publisher": f"<{self.base_uri}publisher>",
            "language": f"<{self.base_uri}language>", "license": f"<{self.base_uri}license>",
            "apc": f"<{self.base_uri}apc>", "seal": f"<{self.base_uri}seal>"
        }
    def _get_id(self, row):
        issn = str(row.get("Journal ISSN (print version)", "")).strip()
        eissn = str(row.get("Journal EISSN (online version)", "")).strip()
        title = str(row.get("Journal title", "")).strip()
        if issn: return issn
        if eissn: return eissn
        if title: return re.sub(r'[^a-zA-Z0-9]+', '-', title.lower()).strip('-')
        return None
    def pushDataToDb(self, file_path):
        df = pd.read_csv(file_path, keep_default_na=False)
        df.columns = [col.strip() for col in df.columns]
        triples = []
        for _, row in df.iterrows():
            journal_id = self._get_id(row)
            if not journal_id: continue
            journal_uri = f"<{self.base_uri}{journal_id}>"
            
            def add_triple(pred, obj_literal):
                if obj_literal is not None and str(obj_literal) and pd.notna(obj_literal):
                    triples.append(f"{journal_uri} {pred} {obj_literal} .")
            
            add_triple(self.PRED['type'], self.PRED['Journal'])
            add_triple(self.PRED['id'], Literal(journal_id).n3())
            add_triple(self.PRED['title'], Literal(row.get("Journal title", "")).n3())
            add_triple(self.PRED['publisher'], Literal(row.get("Publisher", "")).n3())
            add_triple(self.PRED['license'], Literal(row.get("Journal license", "")).n3())
            add_triple(self.PRED['apc'], Literal(str(row.get("APC", "")).strip().lower() == "yes").n3())
            add_triple(self.PRED['seal'], Literal(str(row.get("DOAJ Seal", "")).strip().lower() == "yes").n3())
            
            langs = str(row.get("Languages in which the journal accepts manuscripts", "")).split(',')
            for lang in langs:
                if lang.strip(): add_triple(self.PRED['language'], Literal(lang.strip()).n3())

        query = f"INSERT DATA {{ {'\n'.join(triples)} }}"
        try:
            response = requests.post(self.getDbPathOrUrl(), data=query.encode('utf-8'), headers={'Content-Type': 'application/sparql-update'})
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            print(f"Error during bulk upload: {e}")
            return False

# --- Query Handlers ---
class CategoryQueryHandler(QueryHandler): 
    def __init__(self, dbPathOrUrl):
        super().__init__(dbPathOrUrl)
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
    def __init__(self, dbPathOrUrl, base_uri="http://application.org/"):
        super().__init__(dbPathOrUrl)
        self.base_uri = base_uri
        self.PRED = { "id": f"<{self.base_uri}id>", "title": f"<{self.base_uri}title>", "publisher": f"<{self.base_uri}publisher>", "license": f"<{self.base_uri}license>"}
    def _query_to_df(self, query):
        try:
            store = SPARQLStore(self.getDbPathOrUrl())
            results = store.query(query)
            data = [{'subject': str(r[0]), 'predicate': str(r[1]), 'object': str(r[2])} for r in results]
            return pd.DataFrame(data)
        except Exception as e:
            print(f"A SPARQL query failed: {e}")
            return pd.DataFrame()
    def getById(self, identifier):
        query = f"""SELECT ?s ?p ?o WHERE {{ ?s {self.PRED['id']} "{identifier}" . ?s ?p ?o . }}"""
        return self._query_to_df(query)
    def getAllJournals(self):
        query = f"""SELECT ?s ?p ?o WHERE {{ ?s a <{self.base_uri}Journal> ; ?p ?o . }}"""
        return self._query_to_df(query)
    def getJournalsWithTitle(self, title_fragment):
        query = f"""SELECT ?s ?p ?o WHERE {{
            ?s {self.PRED['title']} ?title .
            FILTER(CONTAINS(LCASE(STR(?title)), LCASE("{title_fragment}"))) ?s ?p ?o . }}"""
        return self._query_to_df(query)
    def getJournalsPublishedBy(self, publisher_fragment):
        query = f"""SELECT ?s ?p ?o WHERE {{
            ?s {self.PRED['publisher']} ?publisher .
            FILTER(CONTAINS(LCASE(STR(?publisher)), LCASE("{publisher_fragment}"))) ?s ?p ?o . }}"""
        return self._query_to_df(query)
    def getJournalsWithLicense(self, license_text):
        query = f"""SELECT ?s ?p ?o WHERE {{
            ?s {self.PRED['license']} ?license .
            FILTER(STR(?license) = "{license_text}") ?s ?p ?o . }}"""
        return self._query_to_df(query)
    def getJournalsWithAPC(self):
        query = f"""SELECT ?s ?p ?o WHERE {{ ?s <{self.base_uri}apc> true . ?s ?p ?o . }}"""
        return self._query_to_df(query)
    def getJournalsWithDOAJSeal(self):
        query = f"""SELECT ?s ?p ?o WHERE {{ ?s <{self.base_uri}seal> true . ?s ?p ?o . }}"""
        return self._query_to_df(query)

# --- Query Engines ---
class BasicQueryEngine:
    def __init__(self):
        self.journalQuery, self.categoryQuery = [], []
    def addJournalHandler(self, handler): self.journalQuery.append(handler)
    def addCategoryHandler(self, handler): self.categoryQuery.append(handler)
    def cleanJournalHandlers(self): self.journalQuery = []
    def cleanCategoryHandlers(self): self.categoryQuery = []

    def _df_to_wide(self, df: pd.DataFrame, subject_col='subject'):
        if df.empty: return pd.DataFrame()
        def agg_func(x):
            unique_x = list(pd.unique(x))
            return unique_x[0] if len(unique_x) == 1 else unique_x
        pivot_df = df.pivot_table(index=subject_col, columns='predicate', values='object', aggfunc=agg_func)
        pivot_df.columns = [str(col).split('/')[-1].split('#')[-1] for col in pivot_df.columns]
        return pivot_df.reset_index().rename(columns={subject_col: 'uri'})

    def _wide_df_to_journals(self, wide_df: pd.DataFrame):
        if wide_df.empty: return []
        journals = []
        for _, row in wide_df.iterrows():
            pub_name = row.get('publisher')
            pub = Publisher(p_id=pub_name, name=pub_name) if pub_name and pd.notna(pub_name) else None
            langs = row.get('language', [])
            if not isinstance(langs, list): langs = [langs] if pd.notna(langs) else []
            journal = Journal(j_id=row.get('id'), title=row.get('title'), publisher=pub,
                              languages=langs, licence=row.get('license'),
                              apc=str(row.get('apc')).lower() == 'true',
                              seal=str(row.get('seal')).lower() == 'true')
            journals.append(journal)
        return journals

    def _df_to_categories(self, df: pd.DataFrame):
        categories = []
        for _, row in df.iterrows():
            area_obj = Area(area_id=row['area'], name=row['area'])
            cat_obj = Category(cat_id=row['category_id'], title=row['category_id'], 
                               quartile=row['quartile'], area=area_obj)
            categories.append(cat_obj)
        return categories
    
    def _df_to_areas(self, df: pd.DataFrame):
        return [Area(area_id=name, name=name) for name in df['area']]

    def _get_combined_df(self, handlers, method_name, *args):
        if not handlers: return pd.DataFrame()
        all_dfs = [getattr(h, method_name)(*args) for h in handlers]
        return pd.concat(all_dfs).drop_duplicates().reset_index(drop=True)

    def getEntityById(self, id: str):
        journal_df = self._get_combined_df(self.journalQuery, 'getById', id)
        if not journal_df.empty:
            return self._df_to_journals(journal_df)[0]
        category_df = self._get_combined_df(self.categoryQuery, 'getById', id)
        if not category_df.empty:
            return self._df_to_categories(category_df)[0]
        return None

    def getAllJournals(self):
        df_long = self._get_combined_df(self.journalQuery, 'getAllJournals')
        return self._df_to_journals(df_long)
    def getJournalsWithTitle(self, title: str):
        df = self._get_combined_df(self.journalQuery, 'getJournalsWithTitle', title)
        return self._df_to_journals(df)
    def getJournalsPublishedBy(self, publisher: str):
        df = self._get_combined_df(self.journalQuery, 'getJournalsPublishedBy', publisher)
        return self._df_to_journals(df)
    def getJournalsWithLicense(self, license: str):
        df = self._get_combined_df(self.journalQuery, 'getJournalsWithLicense', license)
        return self._df_to_journals(df)
    def getJournalsWithAPC(self):
        df = self._get_combined_df(self.journalQuery, 'getJournalsWithAPC')
        return self._df_to_journals(df)
    def getJournalsWithDOAJSeal(self):
        df = self._get_combined_df(self.journalQuery, 'getJournalsWithDOAJSeal')
        return self._df_to_journals(df)

    def getAllCategories(self):
        df = self._get_combined_df(self.categoryQuery, 'getAllCategories')
        return self._df_to_categories(df)
    def getAllAreas(self):
        df = self._get_combined_df(self.categoryQuery, 'getAllAreas')
        return self._df_to_areas(df)
    def getCategoriesWithQuartile(self, quartiles: set):
        df = self._get_combined_df(self.categoryQuery, 'getCategoriesWithQuartile', quartiles)
        return self._df_to_categories(df)
    def getCategoriesAssignedToAreas(self, area_ids: set):
        df = self._get_combined_df(self.categoryQuery, 'getCategoriesAssignedToAreas', area_ids)
        return self._df_to_categories(df)
    def getAreasAssignedToCategories(self, category_ids: set):
        df = self._get_combined_df(self.categoryQuery, 'getAreasAssignedToCategories', category_ids)
        return self._df_to_areas(df)
        
class FullQueryEngine(BasicQueryEngine):
    def getJournalsInCategoriesWithQuartile(self, category_ids: set, quartiles: set):
        journal_df_long = self._get_combined_df(self.journalQuery, 'getAllJournals')
        if journal_df_long.empty: return []
        journals_df_wide = self._df_to_wide(journal_df_long).rename(columns={'id': 'issn'})
        
        category_links_df = self._get_combined_df(self.categoryQuery, 'getCategoryLinks')
        if category_links_df.empty: return []

        all_categories_df = self._get_combined_df(self.categoryQuery, 'getAllCategories')
        
        target_categories = all_categories_df
        if category_ids:
            target_categories = target_categories[target_categories['category_id'].isin(category_ids)]
        if quartiles:
            target_categories = target_categories[target_categories['quartile'].isin(quartiles)]
        
        target_category_ids = set(target_categories['category_id'])
        filtered_links = category_links_df[category_links_df['category_id'].isin(target_category_ids)]
        
        if 'issn' not in journals_df_wide.columns: return [] # Avoid KeyError
        merged_df = pd.merge(journals_df_wide, filtered_links, on='issn')
        
        return self._wide_df_to_journals(merged_df)

    def getJournalsInAreasWithLicense(self, area_ids: set, licenses: set):
        journal_df_long = self._get_combined_df(self.journalQuery, 'getAllJournals')
        if journal_df_long.empty: return []
        journals_df_wide = self._df_to_wide(journal_df_long).rename(columns={'id': 'issn'})

        if licenses:
            journals_df_wide = journals_df_wide[journals_df_wide['license'].isin(licenses)]
        
        if not area_ids:
            return self._wide_df_to_journals(journals_df_wide)
            
        category_links_df = self._get_combined_df(self.categoryQuery, 'getCategoryLinks')
        all_categories_df = self._get_combined_df(self.categoryQuery, 'getAllCategories')
        
        target_areas = all_categories_df[all_categories_df['area'].isin(area_ids)]
        target_cat_ids = set(target_areas['category_id'])
        filtered_links = category_links_df[category_links_df['category_id'].isin(target_cat_ids)]
        
        merged_df = pd.merge(journals_df_wide, filtered_links, on='issn')
        
        return self._wide_df_to_journals(merged_df)

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, area_ids: set, category_ids: set, quartiles: set):
        journal_df_long = self._get_combined_df(self.journalQuery, 'getAllJournals')
        if journal_df_long.empty: return []
        journals_df_wide = self._df_to_wide(journal_df_long).rename(columns={'id': 'issn'})
        
        # Filter for Diamond Journals (no APC) first
        diamond_journals_df = journals_df_wide[journals_df_wide['apc'] == 'False']
        
        # Now, filter by categories and areas
        category_links_df = self._get_combined_df(self.categoryQuery, 'getCategoryLinks')
        all_categories_df = self._get_combined_df(self.categoryQuery, 'getAllCategories')

        target_cats_and_areas = all_categories_df
        if area_ids:
            target_cats_and_areas = target_cats_and_areas[target_cats_and_areas['area'].isin(area_ids)]
        if category_ids:
            target_cats_and_areas = target_cats_and_areas[target_cats_and_areas['category_id'].isin(category_ids)]
        if quartiles:
            target_cats_and_areas = target_cats_and_areas[target_cats_and_areas['quartile'].isin(quartiles)]
        
        target_cat_ids = set(target_cats_and_areas['category_id'])
        filtered_links = category_links_df[category_links_df['category_id'].isin(target_cat_ids)]
        
        merged_df = pd.merge(diamond_journals_df, filtered_links, on='issn')
        
        return self._wide_df_to_journals(merged_df)