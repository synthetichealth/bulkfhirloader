package bulkfhirloader

import (
	"database/sql"
	"log"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/lib/pq"
)

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

// ClearFactTables clears the facts tables in postgres so they can be reloaded
func ClearFactTables(db *sql.DB) {
	_, err := db.Query(`truncate table synth_ma.synth_condition_facts;`)
	if err != nil {
		fmt.Println("Couldn't truncate synth_ma.synth_condition_facts")
		fmt.Println(err)
		log.Fatal(err)
	}

	_, err = db.Query(`truncate table synth_ma.synth_disease_facts;`)
	if err != nil {
		fmt.Println("Couldn't truncate synth_ma.synth_disease_facts")
		fmt.Println(err)
		log.Fatal(err)
	}

	_, err = db.Query(`truncate table synth_ma.synth_pop_facts;`)
	if err != nil {
		fmt.Println("Couldn't truncate synth_ma.synth_pop_facts")
		fmt.Println(err)
		log.Fatal(err)
	}
}

// CalculatePopulation: Calculate the basic population facts.  Only counting living patients.
func CalculatePopulation(mgoSession *mgo.Session, mDB string, db *sql.DB) {
	if err := db.Ping(); err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}

	c := mgoSession.DB(mDB).C("rawstat")
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

	txn, err := db.Begin()
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyInSchema("synth_ma", "synth_pop_facts", "cs_fips", "age_id", "pop", "pop_male", "pop_female"))
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}

	result := commonResults{}

	for iter.Next(&result) {
		_, err = stmt.Exec(result.ID.CsFips, result.ID.AgeRange, result.Pop, result.PopMale, result.PopFemale)
		if err != nil {
			fmt.Println(err)
			log.Fatal(err)
		}
	}

	err = stmt.Close()
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}

}

// CalculateDiseaseFact: Calculate the populations broken down by disease.  Only counting living patients.  A patient is counted only once per disease.
func CalculateDiseaseFact(mgoSession *mgo.Session, mDB string, db *sql.DB) {
	if err := db.Ping(); err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}

	c := mgoSession.DB(mDB).C("rawstat")
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

	txn, err := db.Begin()
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyInSchema("synth_ma", "synth_disease_facts", "cs_fips", "disease_id", "age_id", "pop", "pop_male", "pop_female"))
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}

	result := commonResults{}

	for iter.Next(&result) {
		_, err = stmt.Exec(result.ID.CsFips, result.ID.DiseaseID, result.ID.AgeRange, result.Pop, result.PopMale, result.PopFemale)
		if err != nil {
			fmt.Println(err)
			log.Fatal(err)
		}
	}

	err = stmt.Close()
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}

}

// CalculateDiseaseFact: Calculate the populations broken down by disease.  Only counting living patients.  A patient is counted only once per disease.
func CalculateConditionFact(mgoSession *mgo.Session, mDB string, db *sql.DB) {
	if err := db.Ping(); err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}

}

	c := mgoSession.DB(mDB).C("rawstat")
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

	txn, err := db.Begin()
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyInSchema("synth_ma", "synth_condition_facts", "cs_fips", "condition_id", "age_id", "pop", "pop_male", "pop_female"))
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}

	result := commonResults{}

	for iter.Next(&result) {
		_, err = stmt.Exec(result.ID.CsFips, result.ID.ConditionID, result.ID.AgeRange, result.Pop, result.PopMale, result.PopFemale)
		if err != nil {
			fmt.Println(err)
			log.Fatal(err)
		}
	}

	err = stmt.Close()
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}

}
