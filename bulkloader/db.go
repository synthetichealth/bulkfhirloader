package bulkloader

import (
	"database/sql"
	"log"

	"github.com/lib/pq"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var collectionNames = []string{
	"allergyintolerances",
	"careplans",
	"conditions",
	"diagnosticreports",
	"encounters",
	"immunizations",
	"medicationorders",
	"observations",
	"patients",
	"procedures",
	"rawstat",
}

type commonResID struct {
	CsFips      string `bson:"CsFips"`
	AgeRange    int    `bson:"AgeRange"`
	DiseaseID   int    `bson:"DiseaseID,omitempty"`
	ConditionID int    `bson:"ConditionID,omitempty"`
}

type commonResults struct {
	ID        commonResID `bson:"_id"`
	Pop       int32       `bson:"pop"`
	PopMale   int32       `bson:"pop_male"`
	PopFemale int32       `bson:"pop_female"`
}

// ClearFactTables clears the facts tables in Postgres so they can be reloaded
// with new statistics from the uploaded FHIR bundles. This action is disabled
// by default and can be enabled with the -reset flag.
func ClearFactTables(db *sql.DB) {
	log.Println("[WARNING] Clearing statistics in Postgres")
	_, err := db.Query(`truncate table synth_ma.synth_condition_facts;`)
	if err != nil {
		log.Println("Couldn't truncate synth_ma.synth_condition_facts")
	}

	_, err = db.Query(`truncate table synth_ma.synth_disease_facts;`)
	if err != nil {
		log.Println("Couldn't truncate synth_ma.synth_disease_facts")
	}

	_, err = db.Query(`truncate table synth_ma.synth_pop_facts;`)
	if err != nil {
		log.Println("Couldn't truncate synth_ma.synth_pop_facts")
	}
}

// ClearMongoCollections clears the relevant collections in Mongo. This action is
// disabled by default and can be enabled with the -reset flag.
func ClearMongoCollections(mongoSession *mgo.Session, dbName string) {
	log.Println("[WARNING] Clearing collections in Mongo")

	session := mongoSession.Copy()
	defer session.Close()

	for _, name := range collectionNames {
		err := session.DB(dbName).C(name).DropCollection()
		if err != nil {
			log.Printf("Failed to drop collection '%s'\n", name)
		}
	}
}

// CalculatePopulationFacts calculates the basic population facts for each subdivision.
// This only counts living patients.
func CalculatePopulationFacts(mongoSession *mgo.Session, dbName string, db *sql.DB) {

	log.Println("Calculating population statistics...")

	// copy the mongo session
	session := mongoSession.Copy()
	defer session.Close()

	// check that we're still connected to Postgres
	if err := db.Ping(); err != nil {
		log.Fatal("Lost connection to Postgres")
	}

	c := session.DB(dbName).C("rawstat")
	pipeline := []bson.M{
		bson.M{"$match": bson.M{"$or": []interface{}{
			bson.M{"deceasedboolean": bson.M{"$exists": false}},
			bson.M{"deceasedboolean": false},
		}},
		},
		bson.M{
			"$project": bson.M{
				"_id":                       0,
				"gender":                    1,
				"location.subcountyid_fips": 1,
				"agerange":                  1,
				"male": bson.M{"$cond": []interface{}{
					bson.M{"$eq": []interface{}{"$gender", "male"}},
					1,
					0,
				}},
				"female": bson.M{"$cond": []interface{}{
					bson.M{"$eq": []interface{}{"$gender", "female"}},
					1,
					0,
				}},
			},
		},
		bson.M{
			"$group": bson.M{
				"_id": bson.M{
					"CsFips":   "$location.subcountyid_fips",
					"AgeRange": "$agerange"},
				"pop":        bson.M{"$sum": 1},
				"pop_male":   bson.M{"$sum": "$male"},
				"pop_female": bson.M{"$sum": "$female"},
			},
		},
	}

	pipe := c.Pipe(pipeline)
	iter := pipe.Iter()

	log.Println("Adding stats to Postgres...")

	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyInSchema("synth_ma", "synth_pop_facts", "cs_fips", "age_id", "pop", "pop_male", "pop_female"))
	if err != nil {
		log.Fatal(err)
	}

	result := commonResults{}

	for iter.Next(&result) {
		_, err = stmt.Exec(result.ID.CsFips, result.ID.AgeRange, result.Pop, result.PopMale, result.PopFemale)
		if err != nil {
			log.Fatal(err)
		}
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
}

// CalculateDiseaseFacts calculates the populations for each disease we track statistics for.
// This only counts living patients. A patient is counted only once per disease.
func CalculateDiseaseFacts(mongoSession *mgo.Session, dbName string, db *sql.DB) {

	log.Println("Calculating disease statistics...")

	// copy the mongo session
	session := mongoSession.Copy()
	defer session.Close()

	// check that we're still connected to Postgres
	if err := db.Ping(); err != nil {
		log.Fatal("Lost connection to Postgres")
	}

	c := session.DB(dbName).C("rawstat")
	pipeline := []bson.M{
		bson.M{"$match": bson.M{"$or": []interface{}{
			bson.M{"deceasedboolean": bson.M{"$exists": false}},
			bson.M{"deceasedboolean": false},
		}},
		},
		bson.M{"$unwind": "$uniquediseases"},
		bson.M{"$match": bson.M{"uniquediseases": bson.M{"$gt": 0}}},
		bson.M{
			"$project": bson.M{
				"_id":                       0,
				"gender":                    1,
				"agerange":                  1,
				"location.subcountyid_fips": 1,
				"uniquediseases":            1,
				"male": bson.M{"$cond": []interface{}{
					bson.M{"$eq": []interface{}{"$gender", "male"}},
					1,
					0,
				}},
				"female": bson.M{"$cond": []interface{}{
					bson.M{"$eq": []interface{}{"$gender", "female"}},
					1,
					0,
				}},
			},
		},
		bson.M{
			"$group": bson.M{
				"_id": bson.M{
					"CsFips":    "$location.subcountyid_fips",
					"DiseaseID": "$uniquediseases",
					"AgeRange":  "$agerange"},
				"pop":        bson.M{"$sum": 1},
				"pop_male":   bson.M{"$sum": "$male"},
				"pop_female": bson.M{"$sum": "$female"},
			},
		},
	}

	pipe := c.Pipe(pipeline)
	iter := pipe.Iter()

	log.Println("Adding stats to Postgres...")

	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyInSchema("synth_ma", "synth_disease_facts", "cs_fips", "disease_id", "age_id", "pop", "pop_male", "pop_female"))
	if err != nil {
		log.Fatal(err)
	}

	result := commonResults{}

	for iter.Next(&result) {
		_, err = stmt.Exec(result.ID.CsFips, result.ID.DiseaseID, result.ID.AgeRange, result.Pop, result.PopMale, result.PopFemale)
		if err != nil {
			log.Fatal(err)
		}
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
}

// CalculateConditionFacts calculates the populations broken down by condition.
// This only counts living patients. A patient is counted only once per condition.
func CalculateConditionFacts(mongoSession *mgo.Session, dbName string, db *sql.DB) {

	log.Println("Calculating condition statistics...")

	// copy the mongo session
	session := mongoSession.Copy()
	defer session.Close()

	// check that we're still connected to Postgres
	if err := db.Ping(); err != nil {
		log.Fatal("Lost connection to Postgres")
	}

	c := session.DB(dbName).C("rawstat")
	pipeline := []bson.M{
		bson.M{"$match": bson.M{"$or": []interface{}{
			bson.M{"deceasedboolean": bson.M{"$exists": false}},
			bson.M{"deceasedboolean": false},
		}},
		},
		bson.M{"$unwind": "$uniqueconditions"},
		bson.M{"$match": bson.M{"uniqueconditions": bson.M{"$gt": 0}}},
		bson.M{
			"$project": bson.M{
				"_id":                       0,
				"gender":                    1,
				"agerange":                  1,
				"location.subcountyid_fips": 1,
				"uniqueconditions":          1,
				"male": bson.M{"$cond": []interface{}{
					bson.M{"$eq": []interface{}{"$gender", "male"}},
					1,
					0,
				}},
				"female": bson.M{"$cond": []interface{}{
					bson.M{"$eq": []interface{}{"$gender", "female"}},
					1,
					0,
				}},
			},
		},
		bson.M{
			"$group": bson.M{
				"_id": bson.M{
					"CsFips":      "$location.subcountyid_fips",
					"ConditionID": "$uniqueconditions",
					"AgeRange":    "$agerange"},
				"pop":        bson.M{"$sum": 1},
				"pop_male":   bson.M{"$sum": "$male"},
				"pop_female": bson.M{"$sum": "$female"},
			},
		},
	}

	pipe := c.Pipe(pipeline)
	iter := pipe.Iter()

	log.Println("Adding stats to Postgres...")

	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyInSchema("synth_ma", "synth_condition_facts", "cs_fips", "condition_id", "age_id", "pop", "pop_male", "pop_female"))
	if err != nil {
		log.Fatal(err)
	}

	result := commonResults{}

	for iter.Next(&result) {
		_, err = stmt.Exec(result.ID.CsFips, result.ID.ConditionID, result.ID.AgeRange, result.Pop, result.PopMale, result.PopFemale)
		if err != nil {
			log.Fatal(err)
		}
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
}
