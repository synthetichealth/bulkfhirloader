package bulkfhirloader

import (
	"database/sql"
	"flag"
	"fmt"
	"log"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

func CalcSubCountyStats(mgoSession *mgo.Session, mDB string, pgConn string) {
	fmt.Println("Hello from CalcSubCountyStats")
	c := mgoSession.DB(mDB).C("rawstat")
	pipeline := []bson.M{
		bson.M{"$match": bson.M{"$and": []interface{}{
			bson.M{"conditions.diseasefp": bson.M{"$gt": 0}},
			bson.M{"$or": []interface{}{
				bson.M{"deceasedboolean": bson.M{"$exists": false}},
				bson.M{"deceasedboolean": false},
			}},
		}},
		},
		bson.M{"$unwind": "$conditions"},
		bson.M{"$match": bson.M{"conditions.diseasefp": bson.M{"$gt": 0}}},
		bson.M{
			"$project": bson.M{
				"_id":                       0,
				"gender":                    1,
				"location.subcountyid_fips": 1,
				"conditions.diseasefp":      1,
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
					"SubCountyFIPS": "$location.subcountyid_fips",
					"DiseaseFp":     "$conditions.diseasefp"},
				"pop":        bson.M{"$sum": 1},
				"pop_male":   bson.M{"$sum": "$male"},
				"pop_female": bson.M{"$sum": "$female"},
			},
		},
	}

	pipe := c.Pipe(pipeline)

	type qID struct {
		SubCountyFIPS string `bson:"SubCountyFIPS"`
		DiseaseFp     int32  `bson:"DiseaseFp"`
	}
	type qResult struct {
		ID        qID   `bson:"_id"`
		Pop       int32 `bson:"pop"`
		PopMale   int32 `bson:"pop_male"`
		PopFemale int32 `bson:"pop_female"`
	}

	iter := pipe.Iter()

	pgURL2 := flag.String("pgurl2", "postgres://fhir_test:fhir_test@localhost/fhir_test?sslmode=disable", "The PG connection URL (e.g., postgres://pqgotest:password@localhost/pqgotest?sslmode=verify-full)")

	// configure the GORM Postgres driver and database connection
	db, err := sql.Open("postgres", *pgURL2)

	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	// ping the db to ensure we connected successfully
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyInSchema("synth_ma", "synth_cousub_facts", "cousubfp", "diseasefp", "pop", "pop_male", "pop_female"))
	if err != nil {
		log.Fatal(err)
	}

	result := qResult{}

	for iter.Next(&result) {
		_, err = stmt.Exec(result.ID.SubCountyFIPS, result.ID.DiseaseFp, result.Pop, result.PopMale, result.PopFemale)
		if err != nil {
			log.Fatal(err)
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}

	res, err := db.Exec(`update synth_ma.synth_cousub_facts
set rate =  f.pop / (s.pop * 1.0)
from synth_ma.synth_cousub_facts f
join synth_ma.synth_cousub_stats s
	on f.cousubfp = s.cs_fips`)
	if err != nil {
		log.Fatal(err)
	}
	rowCnt, err := res.RowsAffected()
	fmt.Println(rowCnt, err)

	res, err = db.Exec(`insert into synth_ma.synth_county_facts (countyfp, diseasefp, pop, pop_male, pop_female, rate)
select s.ct_fips, f.diseasefp, sum(f.pop) as pop
	, sum(f.pop_male) as pop_male
	, sum(f.pop_female) as pop_female
	, sum(f.pop) / (c.pop * 1.0)
from synth_ma.synth_cousub_facts f
join synth_ma.synth_cousub_stats s
	on f.cousubfp = s.cs_fips
join synth_ma.synth_county_stats c
	on s.ct_fips = c.ct_fips
group by s.ct_fips, f.diseasefp, c.pop`)
	if err != nil {
		log.Fatal(err)
	}
	rowCnt, err = res.RowsAffected()
	fmt.Println(rowCnt, err)

}
