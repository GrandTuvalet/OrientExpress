# Supposing that all the classes developed for the project
# are contained in the file 'impl.py', then:

# 1) Importing all the classes for handling the relational database
from impl import CategoryUploadHandler, CategoryQueryHandler

# 2) Importing all the classes for handling graph database
from impl import JournalUploadHandler, JournalQueryHandler

# 3) Importing the class for dealing with mashup queries
from impl import FullQueryEngine
from impl import Journal, Category, Area
# Once all the classes are imported, first create the relational
# database using the related source data
rel_path = "relational.db"
cat = CategoryUploadHandler()
cat.setDbPathOrUrl(rel_path)
cat.pushDataToDb("data/scimago.json")
# Please remember that one could, in principle, push one or more files
# calling the method one or more times (even calling the method twice
# specifying the same file!)

# Then, create the graph database (remember first to run the
# Blazegraph instance) using the related source data
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
jou = JournalUploadHandler()
jou.setDbPathOrUrl(grp_endpoint)
jou.pushDataToDb("data/doaj.csv")
# Please remember that one could, in principle, push one or more files
# calling the method one or more times (even calling the method twice
# specifying the same file!)

# In the next passage, create the query handlers for both
# the databases, using the related classes
cat_qh = CategoryQueryHandler()
cat_qh.setDbPathOrUrl(rel_path)

jou_qh = JournalQueryHandler()
jou_qh.setDbPathOrUrl(grp_endpoint)

# Finally, create a advanced mashup object for asking
# about data
engine = FullQueryEngine()
engine.addCategoryHandler(cat_qh)
engine.addJournalHandler(jou_qh)


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