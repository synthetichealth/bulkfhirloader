package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/intervention-engine/fhir/models"
	_ "github.com/lib/pq"
	"github.com/synthetichealth/bulkfhirloader/bulkloader"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var debug *bool

// WorkerChannel coordinates the processing of FHIR bundles between several workers.
type WorkerChannel struct {
	bundleChannel chan (string)
}

// visit visits all the FHIR bundles in a specified path, adding each bundle to the
// bundleChannel that feeds the workers.
func (wc *WorkerChannel) visit(path string, f os.FileInfo, err error) error {

	if *debug {
		log.Printf("Visited: %s\n", path)
	}

	if !f.IsDir() && strings.HasSuffix(path, ".json") {

		// push bundle onto channel
		wc.bundleChannel <- path
		return nil
	}

	if *debug {
		log.Println("Processed directory path or non-json file....")
	}
	return nil
}

func main() {
	// required command line flags
	fhirBundlePath := flag.String("p", "", "Path to fhir bundles to upload")
	mongoServer := flag.String("mongo", "localhost:27017", "MongoDB server url, format: host:27017")
	mongoDBName := flag.String("dbname", "fhir", "MongoDB database name, e.g. 'fhir'")
	pgurl := flag.String("pgurl", "", "Postgres connection string, format: postgresql://username:password@host/dbname?sslmode=disable")

	// optional flags (with sensible defaults)
	numWorkers := flag.Int("workers", 8, "Number of concurrent workers to use")
	reset := flag.Bool("reset", false, "Reset the FHIR collections in Mongo and reset the synth_ma statistics")
	debug = flag.Bool("debug", false, "Display additional debug output")

	flag.Parse()

	if *fhirBundlePath == "" {
		fmt.Println("You must specify a path to the fhir bundles to upload")
		return
	}

	if *pgurl == "" {
		fmt.Println("You must specify a Postgres connection string")
		return
	}

	var err error

	// setup the MongoDB connection
	mongoSession, err := mgo.Dial(*mongoServer)
	if err != nil {
		log.Fatal(err)
	}
	defer mongoSession.Close()

	// setup the Postgres connection
	pgDB, err := sql.Open("postgres", *pgurl)
	if err != nil {
		log.Fatal("Failed to connect to Postgres")
	}

	// ping the Postgres db to ensure we connected successfully
	if err = pgDB.Ping(); err != nil {
		log.Fatal(err)
	}
	defer pgDB.Close()

	// optionally reset the data in postgres and mongo (if starting a clean upload)
	if *reset {
		bulkloader.ClearFactTables(pgDB)
		bulkloader.ClearMongoCollections(mongoSession, *mongoDBName)
	}

	// query Postgres for a list of the current subdivisions and diseases we track
	log.Println("Getting latest subdivision and disease information from Postgres...")

	cousubs, err := getCousubs(pgDB)
	if err != nil {
		if *debug {
			log.Println(err)
		}
		log.Fatal("Failed to get subdivision list from Postgres")
	}

	diseases, err := getDiseases(pgDB)
	if err != nil {
		if *debug {
			log.Println(err)
		}
		log.Fatal("Failed to get disease list from Postgres")
	}

	// create a new WorkerChannel to coordinate workers
	log.Printf("Reading FHIR bundles in %s\n", *fhirBundlePath)

	start := time.Now()
	workerChannel := new(WorkerChannel)
	workerChannel.bundleChannel = make(chan string, 256)

	var wg sync.WaitGroup
	var counter uint64 // total number of FHIR bundles processed

	// spawn workers
	for i := 0; i < *numWorkers; i++ {
		wg.Add(1)
		go worker(&wg, workerChannel.bundleChannel, mongoSession, *mongoDBName, cousubs, diseases, &counter)
	}

	err = filepath.Walk(*fhirBundlePath, workerChannel.visit)
	if err != nil {
		log.Println("An error occured while reading-in FHIR bundles:")
		log.Fatal(err)
	}

	// close the channel when done
	close(workerChannel.bundleChannel)

	// wait for all workers to shut down properly
	wg.Wait()
	log.Printf("%d FHIR bundles read in %f seconds\n", counter, getSecondsSince(start))

	// process the statistics for the uploaded bundles
	bulkloader.CalculatePopulationFacts(mongoSession, *mongoDBName, pgDB)
	log.Printf("Time elapsed: %f seconds\n", getSecondsSince(start))

	bulkloader.CalculateDiseaseFacts(mongoSession, *mongoDBName, pgDB)
	log.Printf("Time elapsed: %f seconds\n", getSecondsSince(start))

	bulkloader.CalculateConditionFacts(mongoSession, *mongoDBName, pgDB)
	log.Printf("Time elapsed: %f seconds\n", getSecondsSince(start))
}

// getCousubs queries the Postgres database for the latest list of subdivision in the
// synth_ma.synth_cousub_dim table.
func getCousubs(db *sql.DB) (*bulkloader.CousubMap, error) {

	rows, err := db.Query(`
		SELECT case when right(cd.cs_name, 5) = ' Town' then substring(cd.cs_name, 1, length(cd.cs_name)-5)
			else cs_name
			end
			, cd.ct_fips
			, cd.cs_fips 
		FROM synth_ma.synth_cousub_dim cd`)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cousubs := make(bulkloader.CousubMap)

	for rows.Next() {
		var csName, ctFips, csFips string
		var cousub bulkloader.Cousub

		err := rows.Scan(&csName, &ctFips, &csFips)
		if err != nil {
			return nil, err
		}
		cousub.CountyIDFips = ctFips
		cousub.SubCountyIDFips = csFips
		cousubs[csName] = cousub
	}
	return &cousubs, nil
}

// getDiseases queries the Postgres database for the latest list of diseases in the
// synth_ma.synth_condition_dim table.
func getDiseases(db *sql.DB) (*bulkloader.DiseaseMap, error) {

	rows, err := db.Query(`
		SELECT cd.condition_id, coalesce(cd.disease_id, -999), cd.code_system, cd.code
		FROM synth_ma.synth_condition_dim cd`)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	diseases := make(bulkloader.DiseaseMap)

	for rows.Next() {
		var conditionID, diseaseID int
		var system, code string
		var disease bulkloader.Disease

		err := rows.Scan(&conditionID, &diseaseID, &system, &code)
		if err != nil {
			return nil, err
		}
		disease.ConditionID = conditionID
		disease.DiseaseID = diseaseID
		key := bulkloader.DiseaseKey{
			CodeSystem:  system,
			CodeSysCode: code,
		}
		diseases[key] = disease
	}
	return &diseases, nil
}

// worker uses a WorkerChannel to process all of the resources in a single FHIR bundle, specified by the path to that bundle's JSON file.
func worker(wg *sync.WaitGroup, bundles <-chan string, mongoSession *mgo.Session, dbName string, cousubs *bulkloader.CousubMap, diseases *bulkloader.DiseaseMap, counter *uint64) {
	defer wg.Done()

	for {
		select {
		case path, ok := <-bundles:
			if !ok {
				return
			}

			jsonFile, err := os.Open(path)
			if err != nil && *debug {
				log.Println("Error opening JSON file:\n", err)
				continue
			}

			jsonData, err := ioutil.ReadAll(jsonFile)
			jsonFile.Close()
			if err != nil && *debug {
				log.Println("Error reading JSON data:\n", err)
				continue
			}

			var bundle models.Bundle
			json.Unmarshal(jsonData, &bundle)

			refMap := make(map[string]models.Reference)

			entries := make([]*models.BundleEntryComponent, len(bundle.Entry))
			for i := range bundle.Entry {
				entries[i] = &bundle.Entry[i]
			}

			for _, entry := range entries {
				// Create a new BSON ID and add it to the reference map
				id := bson.NewObjectId().Hex()
				refMap[entry.FullUrl] = models.Reference{
					Reference:    reflect.TypeOf(entry.Resource).Elem().Name() + "/" + id,
					Type:         reflect.TypeOf(entry.Resource).Elem().Name(),
					ReferencedID: id,
					External:     new(bool),
				}
				// Update the resource's UUID to the new BSON ID that was just generated
				bulkloader.SetID(entry.Resource, id)
			}

			// Update all the references to the entries (to reflect newly assigned IDs)
			bulkloader.UpdateAllReferences(entries, refMap)

			resources := make([]interface{}, len(entries))
			for i := range entries {
				resources[i] = entries[i].Resource
			}

			atomic.AddUint64(counter, 1)
			bulkloader.UploadResources(resources, mongoSession, dbName, *cousubs, *diseases)
		} // close the select
	} // close the for
}

func getSecondsSince(start time.Time) float64 {
	return time.Now().Sub(start).Seconds()
}
