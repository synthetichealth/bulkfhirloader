package bulkfhirloader

import (
	"database/sql"
	"fmt"
	"log"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

/* Calculate the populations including a breakdown by disease.
Only counting living patients */
func CalculateFacts(mgoSession *mgo.Session, mDB string, pgConn string) {
	fmt.Println("Hello from CalculateFacts")
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

	// configure the GORM Postgres driver and database connection
	db, err := sql.Open("postgres", pgConn)

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

	db.Query(`update synth_ma.synth_cousub_facts
		set rate =  f.pop / (s.pop * 1.0)
		from synth_ma.synth_cousub_facts f
		join synth_ma.cousub_stats s
			on f.cousubfp = s.cs_fips`)
	if err != nil {
		log.Fatal(err)
	}

	db.Query(`insert into synth_ma.synth_county_facts (countyfp, diseasefp, pop, pop_male, pop_female, rate)
		select s.ct_fips, f.diseasefp, sum(f.pop) as pop
			, sum(f.pop_male) as pop_male
			, sum(f.pop_female) as pop_female
			, sum(f.pop) / (sum(s.pop) * 1.0)
		from synth_ma.synth_cousub_facts f
		join synth_ma.synth_cousub_stats s
			on f.cousubfp = s.cs_fips
		group by s.ct_fips, f.diseasefp`)
	if err != nil {
		log.Fatal(err)
	}

}

/*Calculate the basic population statistics.
Only counting living patients.*/
func CalculateStatistics(mgoSession *mgo.Session, mDB string, pgConn string) {
	fmt.Println("Hello from CalculateStatistics")
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
				"location.countyid_fips":    1,
				"location.subcountyid_fips": 1,
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
					"CountyFIPS":    "$location.countyid_fips",
					"SubCountyFIPS": "$location.subcountyid_fips"},
				"pop":        bson.M{"$sum": 1},
				"pop_male":   bson.M{"$sum": "$male"},
				"pop_female": bson.M{"$sum": "$female"},
			},
		},
	}

	pipe := c.Pipe(pipeline)

	type qID struct {
		CountyFIPS    string `bson:"CountyFIPS"`
		SubCountyFIPS string `bson:"SubCountyFIPS"`
	}
	type qResult struct {
		ID        qID   `bson:"_id"`
		Pop       int32 `bson:"pop"`
		PopMale   int32 `bson:"pop_male"`
		PopFemale int32 `bson:"pop_female"`
	}

	iter := pipe.Iter()

	// configure the GORM Postgres driver and database connection
	db, err := sql.Open("postgres", pgConn)
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

	stmt, err := txn.Prepare(pq.CopyInSchema("synth_ma", "synth_cousub_stats", "ct_fips", "cs_fips", "pop", "pop_male", "pop_female"))
	if err != nil {
		log.Fatal(err)
	}

	result := qResult{}

	for iter.Next(&result) {
		_, err = stmt.Exec(result.ID.CountyFIPS, result.ID.SubCountyFIPS, result.Pop, result.PopMale, result.PopFemale)
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

	db.Query(`update
		synth_ma.synth_cousub_stats f_alias
	set
		ct_name = s_alias.ct_name
		, cs_name = s_alias.cs_name
		, pop_sm = f_alias.pop / (s_alias.sq_mi * 1.0)
		, sq_mi = s_alias.sq_mi
	from
		synth_ma.cousub_stats as s_alias
	where
		f_alias.cs_fips = s_alias.cs_fips;`)
	if err != nil {
		log.Fatal(err)
	}

	db.Query(`insert into synth_ma.synth_county_stats (ct_name, ct_fips, sq_mi, pop, pop_male, pop_female, pop_sm)
			select s.ct_name
				, s.ct_fips
				, s.sq_mi
				, sum(f.pop) as pop
				, sum(f.pop_male) as pop_male
				, sum(f.pop_female) as pop_female
				, sum(f.pop) / (s.sq_mi * 1.0)
			from synth_ma.synth_cousub_stats f
			join synth_ma.county_stats s
				on f.ct_fips = s.ct_fips
			group by s.ct_name, s.ct_fips, s.sq_mi`)
	if err != nil {
		log.Fatal(err)
	}

}
