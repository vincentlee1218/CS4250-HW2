#-------------------------------------------------------------------------
# AUTHOR: Vincent Lee
# FILENAME: db_connection_mongo_solution.py
# SPECIFICATION: Connect to a MongoDB database and perform operations on data
# FOR: CS 4250 - Assignment #2
# TIME SPENT: 6 hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
import datetime  # for ISODate

from pymongo import MongoClient  # import mongo client to connect

# Catch-all values if no database is specified
main_client = None
main_database = "doccorpse"

def connectDataBase():
    """Create a database connection object via PyMongo.

    Return a pymongo.database.Database object for the specified database."""

    # Initialize a MongoClient and set as default client if unset
    global main_client
    client = MongoClient()
    if main_client is None:
        main_client = client

    # Create a database connection object using pymongo
    # and return to the caller program
    # Any database name will work as it is created once a document is stored
    return client[main_database]

def createDocument(col, docId, docText, docTitle, docDate, docCat):
    """Add a document into the specified MongoDB collection.

    col
      pymongo.collection.Collection or string
    docId
      the document's id, must be unique
    docText
      the text for this document
    docTitle
      give your document a name to make it easy to find later
    docDate
      date is in yyyy-mm-dd format or datetime.datetime (recommended)
    docCat
      category for the document
    """

    # Select the collection based on the argument provided
    # Use the default database if not specified in col argument
    if type(col) is str:
        col = getDefaultCol(col)
    if type(docId) is str:
        if docId.isdecimal():
            docId = int(docId)

    # create a dictionary (document) to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    preprocess_text = []
    for i in docText:
        if i.isalnum():
            preprocess_text.append(i)
        else:
            preprocess_text.append(' ')
    preprocess_text = "".join(preprocess_text).lower()
    text_char_length = 0
    preprocess_terms = preprocess_text.split(" ")
    term_counts = {}
    for term in preprocess_terms:
        if len(term) == 0:
            continue
        if term in term_counts:
            term_counts[term] += 1
        else:
            term_counts[term] = 1
        text_char_length += len(term)

    # create a list of dictionaries (documents) with each entry including a term, its occurrences, and its num_chars. Ex: [{term, count, num_char}]
    terms = []
    for term, tf in term_counts.items():
        terms.append({"term": term, "count": tf, "num_chars": len(term)})

    #Producing a final document as a dictionary including all the required fields
    result_dict = {
        "_id": docId, "title": docTitle, "text": docText,
        "num_chars": text_char_length, "date": {"$date": docDate},
        "category": docCat, "terms": terms
    }

    # Insert the document
    insert_result = col.insert_one(result_dict)
    if not insert_result.acknowledged:
        print("Could not add document " + docTitle)

def deleteDocument(col, docId):
    """Remove a document from the specified MongoDB collection by id.

    col
      pymongo.collection.Collection or string
    docId
      the existing document's id
    """

    # Select the collection based on the argument provided
    # Use the default database if not specified in col argument
    if type(col) is str:
        col = getDefaultCol(col)
    if type(docId) is str:
        if docId.isdecimal():
            docId = int(docId)

    # Delete the document from the database
    delete_result = col.delete_one({"_id": docId})
    if not delete_result.acknowledged:
        print("Delete operation failed")
    elif delete_result.deleted_count == 0:
        print("No document with _id " + str(docId) + " was found.")

def updateDocument(col, docId, docText, docTitle, docDate, docCat):
    """Update a document in the specified MongoDB collection.

    col
      pymongo.collection.Collection or string
    docId
      the document's id, must be unique
    docText
      the text for this document
    docTitle
      give your document a name to make it easy to find later
    docDate
      date is in yyyy-mm-dd format or datetime.datetime (recommended)
    docCat
      category for the document
    """

    # Select the collection based on the argument provided
    # Use the default database if not specified in col argument
    if type(col) is str:
        col = getDefaultCol(col)
    if type(docId) is str:
        if docId.isdecimal():
            docId = int(docId)

    express = 0
    if express:  # The easy way
        # Delete the document
        deleteDocument(col, docId)

        # Create the document with the same id
        createDocument(col, docId, docText, docTitle, docDate, docCat)
    else:  # Full update method (just wait until an index gets involved)
        # Evaluate the document's text and insert results
        text_results = evaluateDocText(docText)
        new_dict = {
            "title": docTitle, "text": docText,
            "num_chars": text_results["num_chars"], "date": {"$date": docDate},
            "category": docCat, "terms": text_results["terms"]
        }

        # Update the document and display error messages if it occurs
        update_result = col.update_one({"_id": docId}, {"$set": new_dict})
        if not update_result.acknowledged:
            print("Update operation failed")
        else:
            if update_result.matched_count == 0:
                print("No documents matched query")
            elif update_result.modified_count == 0:
                print("No documents were updated")

def getIndex(col):
    """Return a dictionary representing the inverted index in memory.

    col
      pymongo.collection.Collection or string
    """

    # Select the collection based on the argument provided
    # Use the default database if not specified in col argument
    if type(col) is str:
        col = getDefaultCol(col)

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3', ...}
    # We are simulating an inverted index here in memory.
    every_term = {}     # Nested dictionary
    reverse_index = {}  # Dictionary containing strings as pairs
    for e in col.find():
        data_title = e["title"]
        data_terms = e["terms"]
        for term in data_terms:
            term_name = term["term"]
            if term_name in every_term:
                if data_title in every_term[term_name]:
                    every_term[term_name][data_title] += term["count"]
                else:
                    every_term[term_name][data_title] = term["count"]
            else:
                every_term[term_name] = {data_title: term["count"]}

    # The index needs to be formatted as a string
    for index_term in sorted(every_term):
        term_frequencies = sorted(every_term[index_term].items(),
                                  key=lambda x: x[0])
        field_strings = []
        for title, freq in sorted(term_frequencies,
                                  key=lambda x: x[0]):
            field_strings.append(title + ":" + str(freq))
        reverse_index[index_term] = ", ".join(field_strings)
    return reverse_index

def getDefaultCol(collection_name):
    """Return the collection corresponding to the provided string
    in the default database."""

    # Use the default database
    if not main_client:
        raise RuntimeError("MongoClient is not running")
    col = main_client[main_database][collection_name]
    return col
        
def evaluateDocText(docText):
    """Given a document text, return a dictionary term containing:
    term counts and total text length. Can be used in an update
    statement when inserting final dictionary

    docText
      the full document text to evaluate
    """

    # Using a list to filter out punctuation
    preprocess_text = []
    for i in docText:
        if i.isalnum():
            preprocess_text.append(i)
        else:
            preprocess_text.append(' ')

    # The problem calls for all terms to be lowercase
    preprocess_text = "".join(preprocess_text).lower()
    preprocess_terms = preprocess_text.split(" ")

    # This data structure is the heart of the document corpse
    term_counts = {}
    text_char_length = 0
    for term in preprocess_terms:
        # Ignore spaces
        if len(term) == 0:
            continue

        # Add 1 to the frequency count for the corresponding term
        if term in term_counts:
            term_counts[term] += 1
        else:
            term_counts[term] = 1
        text_char_length += len(term)

    # Build the final array to be used in the MongoDB collection
    terms = []
    for term, tf in term_counts.items():
        terms.append({"term": term, "count": tf, "num_chars": len(term)})
    return {"terms": terms, "num_chars": text_char_length}
