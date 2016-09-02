package bulkfhirloader

type RawStats struct {
	ID              string          `bson:"_id" json:"_id"`
	City            string          `bson:"city,omitempty" json:"city,omitempty"`
	ZipCode         string          `bson:"zipcode,omitempty" json:"zipcode,omitempty"`
	Fips            PgFips          `bson:"fips,omitempty" json:"fips,omitempty"`
	Gender          string          `bson:"gender,omitempty" json:"gender"`
	DeceasedBoolean bool            `bson:"deceasedBoolean,omitempty" json:"deceasedBoolean,omitempty"`
	Conditions      []ConditionCode `bson:"conditions,omitempty" json:"conditions"`
}

type ConditionCode struct {
	System string `bson:"system,omitempty" json:"system,omitempty"`
	Code   string `bson:"code,omitempty" json:"code,omitempty"`
}

type PgFips struct {
	CountyID    string
	SubCountyID string
}
