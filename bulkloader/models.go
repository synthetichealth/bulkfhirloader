package bulkloader

// RawStats is a document in the fhir.rawstats collection representing the
// raw, aggregated statistics for a subdivision generated after a bulk upload.
type RawStats struct {
	ID               string          `bson:"_id" json:"_id"`
	Location         Cousub          `bson:"location,omitempty" json:"location,omitempty"`
	Gender           string          `bson:"gender,omitempty" json:"gender"`
	AgeRange         int             `bson:"agerange" json:"agerange"`
	Age              int             `bson:"age" json:"age"`
	DeceasedBoolean  bool            `bson:"deceasedboolean,omitempty" json:"deceasedboolean,omitempty"`
	Conditions       []ConditionCode `bson:"conditions,omitempty" json:"conditions,omitempty"`
	UniqueConditions []int           `bson:"uniqueconditions,omitempty" json:"uniqueconditions,omitempty"`
	UniqueDiseases   []int           `bson:"uniquediseases,omitempty" json:"uniquediseases,omitempty"`
}

// ConditionCode is a unique condition represented by a code system (e.g. SNOMED_CT) and code,
// and mapped to a disease (e.g. Diabetes) that we track statistics for.
type ConditionCode struct {
	System      string `bson:"system,omitempty" json:"system,omitempty"`
	Code        string `bson:"code,omitempty" json:"code,omitempty"`
	ConditionID int    `bson:"conditionid" json:"conditionid"`
	DiseaseID   int    `bson:"diseaseid" json:"diseaseid"`
}

// Cousub represents a county subdivision.
type Cousub struct {
	CountyIDFips    string `bson:"countyid_fips,omitempty" json:"countyid_fips, omitempty"`
	SubCountyIDFips string `bson:"subcountyid_fips,omitempty" json:"subcountyid_fips, omitempty"`
	City            string `bson:"city,omitempty" json:"city,omitempty"`
	ZipCode         string `bson:"zipcode,omitempty" json:"zipcode,omitempty"`
}

// DiseaseKey uniquely represents a Disease in the diseaseMap.
type DiseaseKey struct {
	CodeSystem  string `bson:"codesystem,omitempty"`
	CodeSysCode string `bson:"codesyscode,omitempty"`
}

// Disease represents a disease (e.g. heart disease) that we track statistics for in Postgres.
// A disease can be made up of multiple conditions. For example, "heart disease" can include
// "myocardial infarction" and "cardiac arrest".
type Disease struct {
	ConditionID int `bson:"conditionid" json:"conditionid"`
	DiseaseID   int `bson:"diseaseid" json:"diseaseid"`
}
