package bulkfhirloader

type RawStats struct {
	ID               string          `bson:"_id" json:"_id"`
	Location         PgFips          `bson:"location,omitempty" json:"location,omitempty"`
	Gender           string          `bson:"gender,omitempty" json:"gender"`
	AgeRange         int             `bson:"agerange" json:"agerange"`
	Age              int             `bson:"age" json:"age"`
	DeceasedBoolean  bool            `bson:"deceasedboolean,omitempty" json:"deceasedboolean,omitempty"`
	Conditions       []ConditionCode `bson:"conditions,omitempty" json:"conditions,omitempty"`
	UniqueConditions []int           `bson:"uniqueconditions,omitempty" json:"uniqueconditions,omitempty"`
	UniqueDiseases   []int           `bson:"uniquediseases,omitempty" json:"uniquediseases,omitempty"`
}

type ConditionCode struct {
	System      string `bson:"system,omitempty" json:"system,omitempty"`
	Code        string `bson:"code,omitempty" json:"code,omitempty"`
	ConditionID int    `bson:"conditionid" json:"conditionid"`
	DiseaseID   int    `bson:"diseaseid" json:"diseaseid"`
}

type PgFips struct {
	CountyIDFips    string `bson:"countyid_fips,omitempty" json:"countyid_fips, omitempty"`
	SubCountyIDFips string `bson:"subcountyid_fips,omitempty" json:"subcountyid_fips, omitempty"`
	City            string `bson:"city,omitempty" json:"city,omitempty"`
	ZipCode         string `bson:"zipcode,omitempty" json:"zipcode,omitempty"`
}

type DiseaseKey struct {
	CodeSystem  string `bson:"codesystem,omitempty"`
	CodeSysCode string `bson:"codesyscode,omitempty"`
}

type DiseaseGroup struct {
	ConditionID int `bson:"conditionid" json:"conditionid"`
	DiseaseID   int `bson:"diseaseid" json:"diseaseid"`
}

type PgFips struct {
	CountyIDFips    string `bson:"countyid_fips,omitempty" json:"countyid_fips, omitempty"`
	SubCountyIDFips string `bson:"subcountyid_fips,omitempty" json:"subcountyid_fips, omitempty"`
	City            string `bson:"city,omitempty" json:"city,omitempty"`
	ZipCode         string `bson:"zipcode,omitempty" json:"zipcode,omitempty"`
}

type DiseaseKey struct {
	CodeSystem  string `bson:"codesystem,omitempty"`
	CodeSysCode string `bson:"codesyscode,omitempty"`
}
