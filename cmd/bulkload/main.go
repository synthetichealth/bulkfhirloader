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
	"time"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/intervention-engine/fhir/models"
	"github.com/synthetichealth/bulkfhirloader"

	_ "github.com/lib/pq"
)

var (
<<<<<<< HEAD
	root            = os.Args[1]
	mgoServer       = os.Args[2]
	mgoDB           = os.Args[3]
	pgConnectString = os.Args[4]
	pgFipsMap       map[string]bulkfhirloader.PgFips
	pgDiseases      map[bulkfhirloader.DiseaseKey]bulkfhirloader.DiseaseGroup
=======
	root      = os.Args[1]
	mgoServer = os.Args[2]
	mgoDB     = os.Args[3]
	pgFipsMap map[string]bulkfhirloader.PgFips
>>>>>>> 6ef3b8b... Add County/SubCounty info to rawstats
)

type WeirdAl struct {
	bundlechannel chan (string)
}

func (wa *WeirdAl) visit(path string, f os.FileInfo, err error) error {
	fmt.Printf("Visited: %s\n", path)

	if !f.IsDir() && strings.HasSuffix(path, ".json") {

		// push path onto channel
		wa.bundlechannel <- path
		return nil
	} else {
		fmt.Println("directory path or non-json file....")
		return nil
	}
}

func worker(bundles <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	// Create database session
	mgoSession, err := mgo.Dial(mgoServer)
	if err != nil {
		panic(err)
	}
	defer mgoSession.Close()

	for {
		select {
		case path, ok := <-bundles:
			if !ok {
				return
			}
			jsonFile, err := os.Open(path)
			if err != nil {
				fmt.Println("Error opening JSON file:", err)
				continue
			}
			// defer jsonFile.Close()
			jsonData, err := ioutil.ReadAll(jsonFile)
			jsonFile.Close()
			if err != nil {
				fmt.Println("Error reading JSON data:", err)
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
				// Create a new ID and add it to the reference map
				id := bson.NewObjectId().Hex()
				refMap[entry.FullUrl] = models.Reference{
					Reference:    reflect.TypeOf(entry.Resource).Elem().Name() + "/" + id,
					Type:         reflect.TypeOf(entry.Resource).Elem().Name(),
					ReferencedID: id,
					External:     new(bool),
				}
				// Update the UUID to the new bson id that was just generated
				bulkfhirloader.SetId(entry.Resource, id)
			}

			// Update all the references to the entries (to reflect newly assigned IDs)
			bulkfhirloader.UpdateAllReferences(entries, refMap)

			rsc := make([]interface{}, len(entries))
			for i := range entries {
				rsc[i] = entries[i].Resource
			}

<<<<<<< HEAD
			bulkfhirloader.UploadResources(rsc, mgoSession, mgoDB, pgFipsMap, pgDiseases)
=======
			bulkfhirloader.UploadResources(rsc, mgoSession, mgoDB, pgFipsMap)
>>>>>>> 6ef3b8b... Add County/SubCounty info to rawstats
		} // close the select
	} // close the for
}

<<<<<<< HEAD
func pgMaps(db *sql.DB) {
	var (
		csName         string
		ctFips         string
		csFips         string
		fipsRecord     bulkfhirloader.PgFips
		condID         int
		condCodeSystem string
		condCode       string
		condDiseaseID  int
	)
	pgFipsMap = make(map[string]bulkfhirloader.PgFips)

	rows, err := db.Query(`
SELECT case when right(cd.cs_name, 5) = ' Town' then substring(cd.cs_name, 1, length(cd.cs_name)-5)
	else cs_name
	end
	, cd.ct_fips
	, cd.cs_fips 
FROM synth_ma.synth_cousub_dim cd`)
	if err != nil {
		log.Fatal(err)
	}
=======
func pgMaps() {
	var (
		csName string
		ctFips string
		csFips string
		blah   bulkfhirloader.PgFips
	)
	pgFipsMap = make(map[string]bulkfhirloader.PgFips)

	pgURL := flag.String("pgurl", "postgres://fhir:fhir@syntheticmass-dev.mitre.org", "The PG connection URL (e.g., postgres://pqgotest:password@localhost/pqgotest?sslmode=verify-full)")

	// configure the GORM Postgres driver and database connection
	db, err := sql.Open("postgres", *pgURL)

	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	// ping the db to ensure we connected successfully
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query(`SELECT cousub_stats.cs_name, cousub_stats.ct_fips, cousub_stats.cs_fips FROM synth_ma.cousub_stats`)
>>>>>>> 6ef3b8b... Add County/SubCounty info to rawstats
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&csName, &ctFips, &csFips)
		if err != nil {
			log.Fatal(err)
		}
<<<<<<< HEAD
		fipsRecord.CountyIDFips = ctFips
		fipsRecord.SubCountyIDFips = csFips
		pgFipsMap[csName] = fipsRecord
	}

	pgDiseases = make(map[bulkfhirloader.DiseaseKey]bulkfhirloader.DiseaseGroup)
	var dg bulkfhirloader.DiseaseGroup

	// Changing the value in the coalesce will impact the remove dups logic
	rows2, err := db.Query(`SELECT cd.condition_id, coalesce(cd.disease_id, -999), cd.code_system, cd.code FROM synth_ma.synth_condition_dim cd`)
	if err != nil {
		fmt.Println(err)
	}
	defer rows2.Close()
	for rows2.Next() {
		err := rows2.Scan(&condID, &condDiseaseID, &condCodeSystem, &condCode)
		if err != nil {
			fmt.Println(err)
			log.Fatal(err)
		}
		dg.ConditionID = condID
		dg.DiseaseID = condDiseaseID
		pgDiseases[bulkfhirloader.DiseaseKey{condCodeSystem, condCode}] = dg
=======
		blah.CountyID = ctFips
		blah.SubCountyID = csFips
		pgFipsMap[csName] = blah
>>>>>>> 6ef3b8b... Add County/SubCounty info to rawstats
	}

	return
}

func main() {
<<<<<<< HEAD
	// configure the GORM Postgres driver and database connection
	pgDB, err := sql.Open("postgres", pgConnectString)
=======
	pgMaps()
>>>>>>> 6ef3b8b... Add County/SubCounty info to rawstats

	if err != nil {
		log.Fatal(err)
	}
	// defer pgDB.Close()
	// ping the db to ensure we connected successfully
	if err := pgDB.Ping(); err != nil {
		log.Fatal(err)
	}

	pgMaps(pgDB)
	//Won't need this connection again until done processing the bundles
	pgDB.Close()
	then := time.Now()

	carrotTop := new(WeirdAl)
	carrotTop.bundlechannel = make(chan string, 256)

	var wg sync.WaitGroup

	// spawn workers
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go worker(carrotTop.bundlechannel, &wg)
	}

	err = filepath.Walk(root, carrotTop.visit)
	fmt.Printf("filepath.Walk() returned %v\n", err)

	// Close the channel
	close(carrotTop.bundlechannel)

	// wait for all workers to shut down properly
	wg.Wait()

	now := time.Now()
	diff := now.Sub(then)
	fmt.Println("the final tally is: ", diff.Seconds(), "seconds.")

	mgoSession, err := mgo.Dial(mgoServer)
	if err != nil {
		panic(err)
	}
	defer mgoSession.Close()

	pgDB, err = sql.Open("postgres", pgConnectString)

	if err != nil {
		log.Fatal(err)
	}

	defer pgDB.Close()

	bulkfhirloader.ClearFactTables(pgDB)
	bulkfhirloader.CalculatePopulation(mgoSession, mgoDB, pgDB)
	bulkfhirloader.CalculateDiseaseFact(mgoSession, mgoDB, pgDB)
	bulkfhirloader.CalculateConditionFact(mgoSession, mgoDB, pgDB)

}
