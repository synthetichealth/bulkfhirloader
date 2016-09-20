package main

import (
	"database/sql"
	"encoding/json"
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
	root            = os.Args[1]
	mgoServer       = os.Args[2]
	mgoDB           = os.Args[3]
	pgConnectString = os.Args[4]
	pgFipsMap       map[string]bulkfhirloader.PgFips
	pgDiseases      map[bulkfhirloader.DiseaseKey]int32
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

			bulkfhirloader.UploadResources(rsc, mgoSession, mgoDB, pgFipsMap, pgDiseases)
		} // close the select
	} // close the for
}

func pgMaps() {
	var (
		csName      string
		ctFips      string
		csFips      string
		fipsRecord  bulkfhirloader.PgFips
		dFp         int32
		dCodeSystem string
		dCode       string
	)
	pgFipsMap = make(map[string]bulkfhirloader.PgFips)

	// configure the GORM Postgres driver and database connection
	db, err := sql.Open("postgres", pgConnectString)

	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	// ping the db to ensure we connected successfully
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query(`SELECT cousub_stats.cs_name, cousub_stats.ct_fips, cousub_stats.cs_fips FROM synth_ma.cousub_stats`)
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&csName, &ctFips, &csFips)
		if err != nil {
			log.Fatal(err)
		}
		fipsRecord.CountyIDFips = ctFips
		fipsRecord.SubCountyIDFips = csFips
		pgFipsMap[csName] = fipsRecord
	}

	pgDiseases = make(map[bulkfhirloader.DiseaseKey]int32)

	rows2, err := db.Query(`SELECT synth_disease2.diseasefp, synth_disease2.code_system, synth_disease2.code FROM synth_ma.synth_disease2`)
	defer rows2.Close()
	for rows2.Next() {
		err := rows2.Scan(&dFp, &dCodeSystem, &dCode)
		if err != nil {
			log.Fatal(err)
		}
		pgDiseases[bulkfhirloader.DiseaseKey{dCodeSystem, dCode}] = dFp
	}

	return
}

func main() {
	pgMaps()
	then := time.Now()

	carrotTop := new(WeirdAl)
	carrotTop.bundlechannel = make(chan string, 256)

	var wg sync.WaitGroup

	// spawn workers
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go worker(carrotTop.bundlechannel, &wg)
	}

	err := filepath.Walk(root, carrotTop.visit)
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
	bulkfhirloader.CalculateStatistics(mgoSession, mgoDB, pgConnectString)
	bulkfhirloader.CalculateFacts(mgoSession, mgoDB, pgConnectString)

}
