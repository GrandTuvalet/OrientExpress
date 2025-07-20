# main.py
# FINAL CORRECTED VERSION

# 1. DÜZELTME: Eksik olan veri modeli sınıflarını (Journal, Category) import ediyoruz.
from impl import (
    CategoryUploadHandler, 
    CategoryQueryHandler,
    JournalUploadHandler, 
    JournalQueryHandler,
    FullQueryEngine,
    Journal,
    Category
)

# --- 1. Define all paths and endpoints ---
RELATIONAL_DB_PATH = "relational.db"
JSON_DATA_FILE = "data/scimago.json"
CSV_DATA_FILE = "data/doaj.csv"
GRAPH_DB_ENDPOINT = "http://localhost:9999/bigdata/sparql" # Correct endpoint

# --- 2. Upload Data to Both Databases ---
print("--- Starting Data Uploads ---")

print("Uploading relational data from JSON...")
category_uploader = CategoryUploadHandler()
category_uploader.setDbPathOrUrl(RELATIONAL_DB_PATH)
category_uploader.pushDataToDb(JSON_DATA_FILE)
print("Relational data upload complete.")

print("\nUploading graph data from CSV...")
journal_uploader = JournalUploadHandler(GRAPH_DB_ENDPOINT)
journal_uploader.pushDataToDb(CSV_DATA_FILE)
print("Graph data upload complete.")

print("\n--- Data Uploads Finished ---\n")


# --- 3. Set Up the Full Query Engine ---
print("--- Setting Up Query Engine ---")
category_querier = CategoryQueryHandler(RELATIONAL_DB_PATH)
journal_querier = JournalQueryHandler(GRAPH_DB_ENDPOINT)

engine = FullQueryEngine()
engine.addCategoryHandler(category_querier)
engine.addJournalHandler(journal_querier)
print("Query Engine is ready.\n")


# --- 4. Run Queries and See the Results! ---
print("--- Running Test Queries ---")

print("\n[Query 1] Getting all journals...")
all_journals = engine.getAllJournals()
print(f"-> Found {len(all_journals)} journals in total.")
print("Showing first 3:")
for journal in all_journals[:3]:
    if journal and journal.getPublisher():
        print(f"  - Title: {journal.getTitle()}, Publisher: {journal.getPublisher().getName()}")

test_journal_id = "1983-9979"
print(f"\n[Query 2] Getting entity with ID: '{test_journal_id}'...")
found_entity = engine.getEntityById(test_journal_id)
if found_entity:
    # 2. DÜZELTME: isinstance kontrolünü standart ve basit hale getiriyoruz.
    if isinstance(found_entity, Journal):
         print(f"-> Found a Journal: {found_entity.getTitle()}")
    elif isinstance(found_entity, Category):
         print(f"-> Found a Category: {found_entity.getTitle()}")

print("\n[Query 3] Getting 'Q1' journals in the 'Oncology' category...")
results_mashup = engine.getJournalsInCategoriesWithQuartile(category_ids={"Oncology"}, quartiles={"Q1"})
print(f"-> Found {len(results_mashup)} journals matching the criteria.")
print("Showing first 3:")
for journal in results_mashup[:3]:
    if journal:
        print(f"  - Title: {journal.getTitle()}")

print("\n--- All Tests Finished ---")