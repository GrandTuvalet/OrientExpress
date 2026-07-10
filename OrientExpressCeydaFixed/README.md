# OrientExpress

A software to process data stored in different formats and to upload them into two distinct databases to query these databases simultaneously according to predefined operations.


Added function: getAreasByName(area_partial_name: string) - CategoryQueryHandler - returns a dataframe containing all the areas that match, even partially, with the name specified in input, with no repetitions.

Added function: getJournalsWithSealAndNoAPC() - JournalQueryHandler - returns a dataframe containing all the journals that have a DOAJ Seal and do not specify any APC.

Added function: getDiamondJournalsInAreas(area_partial_name: string) - FullQueryEngine - returns a list of Journal objects that have a DOAJ Seal and do not specify any APC, and that have at least one area matching, even partially, the name in input.
