# bulkfhirloader
Loads FHIR bundles directly into Mongo database backing a FHIR server

The commandline arguements are:
  - Path to the FHIR files
    - Assumes that these files are JSON
  - Name of the Mongo DD server
  - Name of the databse
  - Postgres connection string
    - i.e. user:pwd@somehost/dname

The bulkloader assumes that its target databases are empty.  See the **clear_mongo_collections.js** and **clear_postgres_tables.sql** scripts.
