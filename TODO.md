# MONGODB_TO_SQL

Algorithme pour convertir une base de données mongoDB en base de données relationnelle

## Execution

    Connect to a mongodb database
        Get database data
            as JSON |
            as link credentials |
            as zip |
        Parse data as an array of documents
        Flatten the documents
    Retrieve the documents
    Analyse documents structure
        For each document
            Retrieve keys 
    Determine SQL db schema
    Create SQL statements to create tables and their attributes
    Keep in mind indexes and primary keys
    Iterate thru the documents and generate insert statements
    Write the sql statements to a file to be executed as a dump

## TODO

    [] read database name
    [] read maintable name
    [] read collection json file
    [] flatten collection
    [] extract simple primitive attributes
    [] generate sql type query
    [] extract complex object attributes
    [] extract complex object attributes

### Features

    add primary key if doesn't exist
