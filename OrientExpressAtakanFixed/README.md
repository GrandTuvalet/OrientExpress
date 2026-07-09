# OrientExpress

A software to process data stored in different formats and to upload them into two distinct databases to query these databases simultaneously according to predefined operations.


100 & 103 comments

Added function: getJournalNotPublishedBy(publishers: list) - JournalQueryHandler - returns a dataframe containing all journals not published by the publishers specified in the input.

Added function: getJournalWithCategories(num_categories: int) - JournalQueryHandler - returns a dataframe containing all journals with at least that number of categories associated.

Added function: getMultiCategoriesJournalsAvoidingPuglishers(publishers: list, num_categories: int) - FullQueryEngine - returns a list of Journal objects that have not been published by any of the publishers in input, and that contain at least num_categories categories.
