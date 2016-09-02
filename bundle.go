package bulkfhirloader

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	mgo "gopkg.in/mgo.v2"

	"github.com/intervention-engine/fhir/models"
)

/*
 * NOTE: This is a destructive operation.  Resources will be updated with new server-assigned ID and
 * its references will point to server locations of other resources.
 */
func UploadResources(resources []interface{}, mgoSession *mgo.Session, mDB string, pgMapFips map[string]PgFips) {

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
				// basestat.City = p.Address[0].City
				// basestat.ZipCode = p.Address[0].PostalCode
				basestat.DeceasedBoolean = p.DeceasedDateTime != nil || (p.DeceasedBoolean != nil && *p.DeceasedBoolean)
				// basestat.Fips = pgMapFips[p.Address[0].City]
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

func getId(model interface{}) string {
	return reflect.ValueOf(model).Elem().FieldByName("Id").String()
}

func SetId(model interface{}, id string) {
	v := reflect.ValueOf(model).Elem().FieldByName("Id")
	if v.CanSet() {
		v.SetString(id)
	}
}

// In order for upload to work correctly, resources must go after the resources they depend on.
func sortResourcesByDependency(resources []interface{}) []interface{} {
	var result []interface{}
	for _, resource := range resources {
		var i int
		for i = 0; i < len(result); i++ {
			if references(result[i], resource) {
				break
			}
		}
		result = append(result[:i], append([]interface{}{resource}, result[i:]...)...)
	}
	return result
}

func references(from interface{}, to interface{}) bool {
	toID := getId(to)
	for _, ref := range getAllReferences(from) {
		if strings.TrimPrefix(ref.Reference, "cid:") == toID {
			return true
		}
	}
	return false
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
