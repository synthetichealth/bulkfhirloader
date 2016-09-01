package bulkfhirloader

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	mgo "gopkg.in/mgo.v2"

	"github.com/intervention-engine/fhir/models"
)

func removeDuplicateDiseases(elements []ConditionCode) []int {
	// Use map to record duplicates as we find them.
	encountered := map[int]bool{}
	result := []int{}

	for v := range elements {
		if encountered[elements[v].DiseaseID] == true {
			// Do not add duplicate.
		} else {
			// Record this element as an encountered element.
			if elements[v].DiseaseID >= 0 {
				encountered[elements[v].DiseaseID] = true
				// Append to result slice.
				result = append(result, elements[v].DiseaseID)
			}
		}
	}
	// Return the new slice.
	return result
}

func removeDuplicateConditions(elements []ConditionCode) []int {
	// Use map to record duplicates as we find them.
	encountered := map[int]bool{}
	result := []int{}

	for v := range elements {
		if encountered[elements[v].ConditionID] == true {
			// Do not add duplicate.
		} else {
			// Record this element as an encountered element.
			if elements[v].ConditionID >= 0 {
				encountered[elements[v].ConditionID] = true
				// Append to result slice.
				result = append(result, elements[v].ConditionID)
			}
		}
	}
	// Return the new slice.
	return result
}

func diff(a, b time.Time) (year, month, day, hour, min, sec int) {
	if a.Location() != b.Location() {
		b = b.In(a.Location())
	}
	if a.After(b) {
		a, b = b, a
	}
	y1, M1, d1 := a.Date()
	y2, M2, d2 := b.Date()

	h1, m1, s1 := a.Clock()
	h2, m2, s2 := b.Clock()

	year = int(y2 - y1)
	month = int(M2 - M1)
	day = int(d2 - d1)
	hour = int(h2 - h1)
	min = int(m2 - m1)
	sec = int(s2 - s1)

	// Normalize negative values
	if sec < 0 {
		sec += 60
		min--
	}
	if min < 0 {
		min += 60
		hour--
	}
	if hour < 0 {
		hour += 24
		day--
	}
	if day < 0 {
		// days in month:
		t := time.Date(y1, M1, 32, 0, 0, 0, 0, time.UTC)
		day += 32 - t.Day()
		month--
	}
	if month < 0 {
		month += 12
		year--
	}

	return
}

/*
 * NOTE: This is a destructive operation.  Resources will be updated with new server-assigned ID and
 * its references will point to server locations of other resources.
 */
func UploadResources(resources []interface{}, mgoSession *mgo.Session, mDB string, pgMapFips map[string]PgFips, pgDiseases map[DiseaseKey]DiseaseGroup) {

	var basestat RawStats
	var condcode ConditionCode

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
				year, _, _, _, _, _ := diff(p.BirthDate.Time, time.Now())
				basestat.Age = year
				basestat.AgeRange = 1
				basestat.DeceasedBoolean = p.DeceasedDateTime != nil || (p.DeceasedBoolean != nil && *p.DeceasedBoolean)
				basestat.Location.City = p.Address[0].City
				basestat.Location.ZipCode = p.Address[0].PostalCode
				basestat.Location.CountyIDFips = pgMapFips[p.Address[0].City].CountyIDFips
				basestat.Location.SubCountyIDFips = pgMapFips[p.Address[0].City].SubCountyIDFips
			}
		}

		if resourceType == "Condition" {
			p, ok := t.(*models.Condition)
			if ok {
				condcode.Code = p.Code.Coding[0].Code
				condcode.System = p.Code.Coding[0].System
				condcode.DiseaseID = pgDiseases[DiseaseKey{condcode.System, condcode.Code}].DiseaseID
				condcode.ConditionID = pgDiseases[DiseaseKey{condcode.System, condcode.Code}].ConditionID
				basestat.Conditions = append(basestat.Conditions, condcode)
			}
		}
	}

	for key, value := range forMango {
		c := mgoSession.DB(mDB).C(key)
		x := c.Bulk()
		x.Unordered()
		x.Insert(value...)
		_, err := x.Run()
		if err != nil {
			panic(err)
		}
	}

	c := mgoSession.DB(mDB).C("rawstat")
	basestat.UniqueConditions = removeDuplicateConditions(basestat.Conditions)
	basestat.UniqueDiseases = removeDuplicateDiseases(basestat.Conditions)
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
	refs := make([]*models.Reference, 0)
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

func SetId(model interface{}, id string) {
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
