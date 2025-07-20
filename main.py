# main.py
# This script runs the entire project workflow from start to finish.

from impl import (
    CategoryUploadHandler, 
    CategoryQueryHandler,
    JournalUploadHandler, 
    JournalQueryHandler,
    FullQueryEngine
)

# --- 1. Define all paths and endpoints ---
RELATIONAL_DB_PATH = "relational.db"
JSON_DATA_FILE = "data/scimago.json"
CSV_DATA_FILE = "data/doaj.csv"
GRAPH_DB_ENDPOINT = "http://localhost:9999/bigdata/sparql" # Default for Blazegraph

# --- 2. Upload Data to Both Databases ---
print("--- Starting Data Uploads ---")

# Upload relational data
print("Uploading relational data from JSON...")
category_uploader = CategoryUploadHandler()
category_uploader.setDbPathOrUrl(RELATIONAL_DB_PATH)
category_uploader.pushDataToDb(JSON_DATA_FILE)
print("Relational data upload complete.")

# Upload graph data
print("\nUploading graph data from CSV...")
journal_uploader = JournalUploadHandler(GRAPH_DB_ENDPOINT)
journal_uploader.pushDataToDb(CSV_DATA_FILE)
print("Graph data upload complete.")

print("\n--- Data Uploads Finished ---\n")


# --- 3. Set Up the Full Query Engine ---
print("--- Setting Up Query Engine ---")
# Create query handlers for each database
category_querier = CategoryQueryHandler(RELATIONAL_DB_PATH)
journal_querier = JournalQueryHandler(GRAPH_DB_ENDPOINT)

# Create the engine and add the handlers
engine = FullQueryEngine()
engine.addCategoryHandler(category_querier)
engine.addJournalHandler(journal_querier)
print("Query Engine is ready.\n")


# --- 4. Run Queries and See the Results! ---
print("--- Running Test Queries ---")

# Query 1: Get all journals
print("\n[Query 1] Getting all journals...")
all_journals = engine.getAllJournals()
print(f"-> Found {len(all_journals)} journals in total.")
print("Showing first 3:")
for journal in all_journals[:3]:
    if journal:
        print(f"  - Title: {journal.getTitle()}, Publisher: {journal.getPublisher().getName() if journal.getPublisher() else 'N/A'}")

# Query 2: Get a specific entity by its ID
test_journal_id = "1983-9979" # Example ISSN
print(f"\n[Query 2] Getting entity with ID: '{test_journal_id}'...")
found_entity = engine.getEntityById(test_journal_id)
if found_entity:
    # This shows how we can check the type of object returned
    if isinstance(found_entity, globals().get('Journal')):
         print(f"-> Found a Journal: {found_entity.getTitle()}")
    elif isinstance(found_entity, globals().get('Category')):
         print(f"-> Found a Category: {found_entity.getTitle()}")

# Query 3: A complex "mashup" query from FullQueryEngine
print("\n[Query 3] Getting 'Q1' journals in the 'Oncology' category...")
# NOTE: This is a simplified call for testing. The full implementation would use IDs.
results_mashup = engine.getJournalsInCategoriesWithQuartile(category_ids={"Oncology"}, quartiles={"Q1"})
print(f"-> Found {len(results_mashup)} journals matching the criteria.")
print("Showing first 3:")
for journal in results_mashup[:3]:
    if journal:
        print(f"  - Title: {journal.getTitle()}")

print("\n--- All Tests Finished ---")