package bulkloader

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/intervention-engine/fhir/models"
	"gopkg.in/mgo.v2"
)

// CousubMap maps a county subdivision ID (called "csfips" in the synth_ma.synth_cousub_dim
// table) to a bulkloader.Cousub struct.
type CousubMap map[string]Cousub

// DiseaseMap maps a bulkloader.DiseaseKey (a system/coding pair) to a disease
// in the synth_ma.synth_disease_dim table.
type DiseaseMap map[DiseaseKey]Disease

// removeDuplicats removes any duplicate Condition and Disease IDs and
// returns two slices containing unique conditionIDs and diseaseIDs, respectively.
func removeDuplicates(conditions []ConditionCode) ([]int, []int) {
	// Use maps to record duplicates as we find them
	c := map[int]bool{}
	d := map[int]bool{}

	for _, el := range conditions {
		c[el.ConditionID] = true
		d[el.DiseaseID] = true
	}

	// iterate through the keys
	uniqueC := make([]int, len(c))
	uniqueD := make([]int, len(d))

	i := 0
	for k := range c {
		uniqueC[i] = k
		i++
	}

	i = 0
	for k := range d {
		uniqueD[i] = k
		i++
	}

	return uniqueC, uniqueD
}

// getAge returns the person's current age given his/her Birthdate bd
func getAge(bd time.Time) int {
	i := 1
	for time.Now().AddDate(i*-1, 0, 0).After(bd) {
		i++
	}
	return i - 1
}

// UploadResources uploads all resources in FHIR bundle to the Mongo database, collecting the
// relevant statistics before uploading. NOTE: This is a destructive operation.  Resources will
// be updated with new server-assigned ID and all references to this ID will point to other
// resources on the server.
func UploadResources(resources []interface{}, mgoSession *mgo.Session, dbName string, cousubs CousubMap, diseases DiseaseMap, ageRanges map[int]int) {

	var basestat RawStats
	var condcode ConditionCode

	// create a copy of the master Mongo session for this upload
	session := mgoSession.Copy()
	defer session.Close()

	forMango := make(map[string][]interface{})

	for _, t := range resources {

		resourceType := reflect.TypeOf(t).Elem().Name()
		collection := models.PluralizeLowerResourceName(resourceType)

		forMango[collection] = append(forMango[collection], t)

		if resourceType == "Patient" {
			p, ok := t.(*models.Patient)
			if ok {
				basestat.ID = p.Id
				basestat.Gender = p.Gender
				basestat.Age = getAge(p.BirthDate.Time)
				basestat.AgeRange = ageRanges[basestat.Age]
				basestat.DeceasedBoolean = p.DeceasedDateTime != nil || (p.DeceasedBoolean != nil && *p.DeceasedBoolean)
				basestat.Location.City = p.Address[0].City
				basestat.Location.ZipCode = p.Address[0].PostalCode
				basestat.Location.CountyIDFips = cousubs[p.Address[0].City].CountyIDFips
				basestat.Location.SubCountyIDFips = cousubs[p.Address[0].City].SubCountyIDFips
			}
		}

		if resourceType == "Condition" {
			p, ok := t.(*models.Condition)
			if ok {
				condcode.Code = p.Code.Coding[0].Code
				condcode.System = p.Code.Coding[0].System
				condcode.DiseaseID = diseases[DiseaseKey{condcode.System, condcode.Code}].DiseaseID
				condcode.ConditionID = diseases[DiseaseKey{condcode.System, condcode.Code}].ConditionID
				basestat.Conditions = append(basestat.Conditions, condcode)
			}
		}
	}

	for key, value := range forMango {
		c := session.DB(dbName).C(key)
		x := c.Bulk()
		x.Unordered()
		x.Insert(value...)
		_, err := x.Run()
		if err != nil {
			panic(err)
		}
	}

	c := session.DB(dbName).C("rawstat")
	basestat.UniqueConditions, basestat.UniqueDiseases = removeDuplicates(basestat.Conditions)

	c.Insert(basestat)
}

func updateReferences(resource interface{}, refMap map[string]string) error {
	refs := getAllReferences(resource)
	for _, ref := range refs {
		if err := updateReference(ref, refMap); err != nil {
			return err
		}
	}
	return nil
}

func updateReference(ref *models.Reference, refMap map[string]string) error {
	if ref != nil && strings.HasPrefix(ref.Reference, "cid:") {
		newRef, ok := refMap[strings.TrimPrefix(ref.Reference, "cid:")]
		if ok {
			ref.Reference = newRef
		} else {
			return errors.New(fmt.Sprint("Failed to find updated reference for ", ref))
		}
	}
	return nil
}

func getAllReferences(model interface{}) []*models.Reference {
	var refs []*models.Reference
	s := reflect.ValueOf(model).Elem()
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		if f.Type() == reflect.TypeOf(&models.Reference{}) && !f.IsNil() {
			refs = append(refs, f.Interface().(*models.Reference))
		} else if f.Type() == reflect.TypeOf([]models.Reference{}) {
			for j := 0; j < f.Len(); j++ {
				refs = append(refs, f.Index(j).Addr().Interface().(*models.Reference))
			}
		}
	}
	return refs
}

// SetID sets the BSON ID for the provided resource
func SetID(model interface{}, id string) {
	v := reflect.ValueOf(model).Elem().FieldByName("Id")
	if v.CanSet() {
		v.SetString(id)
	}
}

func UpdateAllReferences(entries []*models.BundleEntryComponent, refMap map[string]models.Reference) {
	// First, get all the references by reflecting through the fields of each model
	var refs []*models.Reference
	for _, entry := range entries {
		model := entry.Resource
		if model != nil {
			entryRefs := findRefsInValue(reflect.ValueOf(model))
			refs = append(refs, entryRefs...)
		}
	}
	// Then iterate through and update as necessary
	for _, ref := range refs {
		newRef, found := refMap[ref.Reference]
		if found {
			*ref = newRef
		}
	}
}

func findRefsInValue(val reflect.Value) []*models.Reference {
	var refs []*models.Reference

	// Dereference pointers in order to simplify things
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	// Make sure it's a valid thing, else return right away
	if !val.IsValid() {
		return refs
	}

	// Handle it if it's a ref, otherwise iterate its members for refs
	if val.Type() == reflect.TypeOf(models.Reference{}) {
		refs = append(refs, val.Addr().Interface().(*models.Reference))
	} else if val.Kind() == reflect.Struct {
		for i := 0; i < val.NumField(); i++ {
			subRefs := findRefsInValue(val.Field(i))
			refs = append(refs, subRefs...)
		}
	} else if val.Kind() == reflect.Slice {
		for i := 0; i < val.Len(); i++ {
			subRefs := findRefsInValue(val.Index(i))
			refs = append(refs, subRefs...)
		}
	}

	return refs
}
