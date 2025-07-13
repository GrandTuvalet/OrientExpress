import pandas as pd
import re
from rdflib import Graph, URIRef, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore, SPARQLStore

class Handler:
    def __init__(self, dbPathOrUrl):
        self._dbPathOrUrl = dbPathOrUrl

    def getDbPathOrUrl(self):
        return self._dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl):
        self._dbPathOrUrl = pathOrUrl
        return True


class UploadHandler(Handler):
    def pushDataToDb(self, file_path):
        raise NotImplementedError("Must be implemented in subclass")


class JournalUploadHandler(UploadHandler):
    def __init__(self, dbPathOrUrl, base_uri="http://example.org/journal/"):
        super().__init__(dbPathOrUrl)
        self.base_uri = base_uri

    def yesno_to_bool(self, value):
        return str(value).strip().lower() == "yes"

    def slugify_title(self, title: str) -> str:
        slug = re.sub(r'[^a-zA-Z0-9]+', '-', str(title).strip().lower())
        return slug.strip('-')

    def pushDataToDb(self, file_path):
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip()  # Clean whitespace in headers

        graph = Graph()
        missing_issn_rows = []

        for idx, row in df.iterrows():
            issn = str(row.get("Journal ISSN (print version)", "")).strip()
            eissn = str(row.get("Journal EISSN (online version)", "")).strip()

            if issn:
                journal_id = issn
            elif eissn:
                journal_id = eissn
            elif pd.notna(row.get("Journal title", "")):
                journal_id = self.slugify_title(row["Journal title"])
            else:
                journal_id = f"row-{idx}"

            if not issn and not eissn:
                missing_issn_rows.append(idx)

            journal_uri = URIRef(self.base_uri + "journal/" + journal_id)

            graph.add((journal_uri, URIRef(self.base_uri + "type"), URIRef(self.base_uri + "Journal")))
            graph.add((journal_uri, URIRef(self.base_uri + "id"), Literal(journal_id)))
            graph.add((journal_uri, URIRef(self.base_uri + "title"), Literal(row.get("Journal title", ""))))
            graph.add((journal_uri, URIRef(self.base_uri + "licence"), Literal(row.get("Journal license", ""))))
            graph.add((journal_uri, URIRef(self.base_uri + "seal"), Literal(self.yesno_to_bool(row.get("DOAJ Seal", "")))))
            graph.add((journal_uri, URIRef(self.base_uri + "apc"), Literal(self.yesno_to_bool(row.get("APC", "")))))

            if pd.notna(row.get("Publisher", "")):
                graph.add((journal_uri, URIRef(self.base_uri + "publisher"), Literal(row["Publisher"])))

            langs = str(row.get("Languages in which the journal accepts manuscripts", "")).split(",")
            for lang in langs:
                lang_clean = lang.strip()
                if lang_clean:
                    graph.add((journal_uri, URIRef(self.base_uri + "language"), Literal(lang_clean)))

        # Warn on invalid URIs
        for s, _, _ in graph:
            if str(s).endswith("/journal/"):
                print(f"Invalid journal URI (missing ID): {s}")

        # Upload to Blazegraph
        store = SPARQLUpdateStore()
        endpoint = self.getDbPathOrUrl()
        store.open((endpoint, endpoint))

        for triple in graph.triples((None, None, None)):
            store.add(triple)

        store.close()

        print(f"\nData successfully pushed to Blazegraph.")
        print(f"Total triples uploaded: {len(graph)}")
        print(f"Rows missing both ISSN and EISSN: {len(missing_issn_rows)}")
        if missing_issn_rows:
            print(f"Affected row indices: {missing_issn_rows}")
        return graph

    
# Create the handler with your Blazegraph endpoint
#handler = JournalUploadHandler("http://192.168.1.4:9999/blazegraph/sparql")

# Call the method with your CSV path
#handler.pushDataToDb("doaj.csv")

class QueryHandler(Handler):
    def __init__(self, dbPathOrUrl):
        super().__init__(dbPathOrUrl)

        self.store = SPARQLStore()
        self.store.open(self._dbPathOrUrl)
        self.graph = Graph(store=self.store)

    def getById(self, identifier):
        query = f"""
        SELECT ?subject ?predicate ?object WHERE {{
            ?subject <http://example.org/journal/id> "{identifier}" .
            ?subject ?predicate ?object .
        }}
        """
        results = self.graph.query(query)

        data = []
        for row in results:
            data.append({
                "subject": str(row.subject),
                "predicate": str(row.predicate),
                "object": str(row.object)
            })

        return pd.DataFrame(data)

class JournalQueryHandler(QueryHandler):
    def __init__(self, dbPathOrUrl, base_uri="http://example.org/journal/"):
        super().__init__(dbPathOrUrl)
        self.base_uri = base_uri
    
    def getAllJournals(self):
        query = f"""
        SELECT ?journal ?predicate ?object WHERE {{
            ?journal ?predicate ?object .
            FILTER(CONTAINS(STR(?journal), "{self.base_uri}"))
        }}

        """
        results = self.graph.query(query)
        return self._resultsToDataFrame(results)

    def getJournalsWithTitle(self, title_fragment):
        query = f"""
        SELECT ?journal ?predicate ?object
        WHERE {{
            ?subject <http://example.org/journal/title> ?title .
            FILTER(CONTAINS(LCASE(STR(?title)), LCASE("{title_fragment}"))) .
            ?subject ?predicate ?object .
            BIND(?subject AS ?journal)
        }}
        """
        results = self.graph.query(query)
        return self._resultsToDataFrame(results)
    
    def getJournalsPublishedBy(self, publisher_fragment):
        query = f"""
        SELECT ?journal ?predicate ?object
        WHERE {{
            ?subject <http://example.org/journal/publisher> ?publisher .
            FILTER(CONTAINS(LCASE(STR(?publisher)), LCASE("{publisher_fragment}"))) .
            ?subject ?predicate ?object .
            BIND(?subject AS ?journal)
        }}
        """
        results = self.graph.query(query)
        return self._resultsToDataFrame(results)
    
    def getJournalsWithLicense(self, license_text):
        query = f"""
        SELECT ?journal ?predicate ?object WHERE {{
            ?subject <http://example.org/journal/licence> ?licence .
            FILTER(CONTAINS(LCASE(STR(?licence)), LCASE("{license_text}"))) .
            ?subject ?predicate ?object .
            BIND(?subject AS ?journal)
        }}
        """
        results = self.graph.query(query)
        return self._resultsToDataFrame(results)
    
    def getJournalsWithAPC(self, apc_value):
        query = f"""
        SELECT ?journal ?predicate ?object WHERE {{
            ?subject <http://example.org/journal/apc> ?apc .
            FILTER(LCASE(STR(?apc)) = LCASE("{apc_value}")) .
            ?subject ?predicate ?object .
            BIND(?subject AS ?journal)
        }}
        """
        results = self.graph.query(query)
        return self._resultsToDataFrame(results)
    
    def getJournalsWithDOAJSeal(self, seal_value):
        query = f"""
        SELECT ?journal ?predicate ?object WHERE {{
            ?subject <http://example.org/journal/seal> ?seal .
            FILTER(LCASE(STR(?seal)) = LCASE("{seal_value}")) .
            ?subject ?predicate ?object .
            BIND(?subject AS ?journal)
        }}
        """
        results = self.graph.query(query)
        return self._resultsToDataFrame(results)
    
    
    def _resultsToDataFrame(self, results):
        data = []
        for row in results:
            data.append({
                "journal": str(row.journal),
                "predicate": str(row.predicate),
                "object": str(row.object)
            })
        return pd.DataFrame(data)


endpoint_url = "http://localhost:9999/blazegraph/sparql"
base_uri = "http://example.org/journal/"

handler = JournalQueryHandler(endpoint_url, base_uri=base_uri)

df = handler.getJournalsPublishedBy('Springer')
print(df)
