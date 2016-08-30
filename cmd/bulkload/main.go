package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/intervention-engine/fhir/models"
	"github.com/synthetichealth/bulkfhirloader"
)

var (
	root      = os.Args[1]
	mgoServer = os.Args[2]
	mgoDB     = os.Args[3]
)

func visit(path string, f os.FileInfo, err error) error {
	fmt.Printf("Visited: %s\n", path)

	if !f.IsDir() {
		jsonFile, err := os.Open(path)
		if err != nil {
			fmt.Println("Error opening JSON file:", err)
			return err
		}
		defer jsonFile.Close()
		jsonData, err := ioutil.ReadAll(jsonFile)
		if err != nil {
			fmt.Println("Error reading JSON data:", err)
			return err
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

		var rsc []interface{}
		for _, entry := range entries {
			rsc = append(rsc, entry.Resource)
		}

		bulkfhirloader.UploadResources(rsc, mgoServer, mgoDB)

		return nil
	} else {
		fmt.Println("just processing the directory path....")
		return nil
	}
}

func main() {

	then := time.Now()

	err := filepath.Walk(root, visit)
	fmt.Printf("filepath.Walk() returned %v\n", err)

	now := time.Now()
	diff := now.Sub(then)
	fmt.Println("the final tally is: ", diff.Seconds(), "seconds.")

}
