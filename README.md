# bulkfhirloader
Loads FHIR bundles directly into Mongo database backing a FHIR server

## About
SyntheticMass consists of two databases.  A Mongo database which stores the detailed patient information and a Postgres database that is used for analytics.  The bulkloader is designed to append data to the Mongo database and truncated and replace the data in Postgres.  This allows SyntheticMass to be loaded in increments.  What the bulkloader **can not** do is an update of the data in Mongo.  


## Running the bulkloader
The bulkloader is executed from the commandline.  It expects the following four arguments:
  - Path to the FHIR files
    - Assumes that these files are JSON and are one the same machine that is running the bulkloader
  - Name of the Mongo database server
  - Name of the databse
  - Postgres connection string
    - i.e. user:pwd@somehost/dname

Given that a load could take several hours it is best to run the bulkloader as a background process with *nohup*.  Nohup is a linux/unix command that allows a process to keep running even if the session that spawned that process ends.

A sample commandline would look like:
    nohup ./bulkloader /data/fhir localhost:27017 fhir pguser:pgpwd@localhost/fhir &

    - This assumes that both the Mongo and Postgres databases are called fhir and are on the localhost
    - The database servers do not have to be on localhost but they do need to be reachable without requiring an SSH tunnel
    - Note the "nohup" and "&" indicating that you want to have this session run in the background even if you disconnect

As the bulkloader processes each FHIR bundle it will create a summary record in the rawstat collection of the Mongo database.  This collection forms the basis of the data used to build the Postgres facts.


### Typical bullkload procedure
1. Clear the Mongo database
    - This can be done with the script "clear\_mongo\_collections.js" or by dropping and recreating the FHIR database.
    - If you are appending data (i.e. adding a new town or county) you would not want to do this as it would mean wiping out all of the information you have already loaded.
2. Clear the facts tables in Postgres
    - This step is optional.  Immediately prior to loading the Postgres database the bulkloader will truncate the necessary tables to ensure a clean load.
    - It may be desireable to clear the facts tables though to indicate that a load is going to take place.
3. Ensure that the JSON FHIR bundles are accessible to the bulkloader and that they don't contain duplicates of what has already been loaded
    - The bulkloader walks every folder within the path listed on the commandline.  If you don't want the bulkloader uploading a file it needs to be moved outside of the path in the commandline argument.
4. Run the bulkloader




### Building the bulkloader
To build in the bulkloader you need to be in the /bulkfhirloader/cmd/bulkload directory.  Prior to the first build you should run a *go get* to ensure you have all of the necessary packagse.