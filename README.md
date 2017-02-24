# bulkfhirloader
Loads FHIR bundles directly into the Mongo database backing the GoFHIR Server for SyntheticMass and updates the relevant statistics in Postgres.

## Building the Bulkloader

### Prerequisites

* [Install Go](https://golang.org/doc/install)

### Testing Locally

If testing locally, you will need additional prerequisites outlined in the [install instructions](https://github.com/synthetichealth/gofhir) for the GoFHIR server. This includes:

* Install and Run Mongo
* Install and Run Postgres
* Install PostGIS Extensions

You will also need to setup the `synth_ma` statistics tables locally, see [pgstats](https://github.com/synthetichealth/pgstats).


### Clone the Bulkloader

Clone the bulkloader in your `GOPATH`:

```
$ cd $GOPATH/src/github.com/synthetichealth
$ git clone https://github.com/synthetichealth/bulkfhirloader.git
```

Use `go get` to get all of the bulkloader's dependencies:

```
$ cd bulkfhirloader
$ go get
```

### GoFHIR Server Dependency
The bulkloader depends on the [stu3_jan2017 branch](https://github.com/intervention-engine/fhir/tree/stu3_jan2017) of the intervention-engine/fhir GoFHIR server. By default `go get` clones the `master` branch of this repository, which is **not** the one we need, so let's fix it:

```
$ cd $GOPATH/src/github.com/intervention-engine/fhir
$ git checkout stu3_jan2017
$ git pull
```

### Build the Bulkloader

By default, `go build` gives the binary the name of the top level folder the project is built from, in this case "bulkfhirloader". This is a bit long, so specify a custom name for the binary:

```
$ cd $GOPATH/src/github.com/synthetichealth/bulkfhirloader
$ go build -o bulkload github.com/synthetichealth/bulkfhirloader 
```
This creates the `bulkload` binary in the project's top-level directory.

## Running the Bulkloader

### Command Line Arguments

You can get a list of command line arguments using the `-h` or `--help` flag:

```
$ ./bulkload --help
```
Outputs:

```
Usage of ./bulkload:                                                                                                                        
  -dbname string                                                                                                                            
        MongoDB database name, e.g. 'fhir' (default "fhir")                                                                                 
  -debug                                                                                                                                    
        Display additional debug output                                                                                                     
  -mongo string                                                                                                                             
        MongoDB server url, format: host:27017 (default "localhost:27017")                                                                  
  -path string                                                                                                                              
        Path to fhir bundles to upload                                                                                                      
  -pgurl string                                                                                                                             
        Postgres connection string, format: postgresql://username:password@host/dbname?sslmode=disable                                      
  -reset                                                                                                                                    
        Reset the FHIR collections in Mongo and reset the synth_ma statistics                                                               
  -workers int                                                                                                                              
        Number of concurrent workers to use (default 8) 
```

Minimally you will need the `-path` and `-pgurl` flags to run the bulkloader.

The `-reset` and `-debug` flags are optional boolean flags that enable a database reset and debugging output, respectively.

### The `rawstat` Collection

As the bulkloader processes each FHIR bundle it will create a summary record in the `rawstat` collection in the Mongo database. Each document in this collecton is organized by the subdivision the patient resides in. This collection forms the basis of the data used to build the Postgres statistics.

## Examples

### Run the Bulkloader Locally

Note: You'll probably need to disable SSL mode

```
$ ./bulkload -path /path/to/fhir/bundles -pgurl postgresql://username:password@localhost/fhir?sslmode=disable
```
	
By default this uses a mongo database called `fhir` running on `localhost` and **does not** reset the Mongo database or any of the Postgres statistics.

### Run the Bulkloader Against a Remote Server

```
$ ./bulkload -path /path/to/fhir/bundles -mongo syntheticmass-dev.mitre.org:27017 -dbname fhir -pgurl postgresql://username:password@syntheticmass-dev.mitre.org/fhir
```

Again, this **does not** reset the Mongo database or any of the Postgres statistics. The Mongo and Postgres database servers both have to be accessible over the open web.

### Resetting Statistics

Add the `-reset` flag. This will log a `[WARNING]` to the console before dumping the statistics.

### Debug Mode

Add the `-debug` flag. This will print out additional logging statements and error messages as they are encountered.


### Run the Bulkloader in the Background

Depending on the number of bundles you plan to upload this process may take several hours. We recommend running the bulkloader in the background using `nohup`:

```
nohup ./bulkload <your_various_cli_args> &
```
This will keep the process running if you close your terminal session.

## Adding a New Disease Statistic

You will need to add new rows representing your statistic to the `synth_ma.synth_condition_dim` and `synth_ma.synth_disease_dim` tables. These will get picked up automatically by the bulkloader and tracked for any patients that have the disease.

**Don't forget to update the schema in the [pgstats](https://github.com/synthetichealth/pgstats) repository!**


### An Example

For example, say we wanted to track the deadly disease "examplitis", which is identified by the conditions "examplation" and "excessive exampling":

1. Add examplitis to the `synth_ma.synth_disease_dim` table:

	```
	INSERT INTO synth_ma.synth_disease_dim (disease_id, disease_name)
    VALUES (4, 'examplitis');
	```

2. Add the conditions to the `synth_ma.synth_condition_dim` table:

	```
	INSERT INTO synth_ma.synth_condition_dim (condition_id, disease_id, condition_name, code_system, code) VALUES
    (5, 4, 'examplation', 'http://example.com/codes', '123'),
    (6, 4, 'excessive_exampling', 'http://example.com/codes', '456');
	```

3. Run the bulkloader again to pick up stats for your new disease


## License

Copyright 2016 The MITRE Corporation

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
