package bulkfhirloader

type RawStats struct {
	ID              string          `bson:"_id" json:"_id"`
	Location        PgFips          `bson:"location,omitempty" json:"location,omitempty"`
	Gender          string          `bson:"gender,omitempty" json:"gender"`
	DeceasedBoolean bool            `bson:"deceasedBoolean,omitempty" json:"deceasedBoolean,omitempty"`
	Conditions      []ConditionCode `bson:"conditions,omitempty" json:"conditions,omitempty"`
}

type ConditionCode struct {
	System string `bson:"system,omitempty" json:"system,omitempty"`
	Code   string `bson:"code,omitempty" json:"code,omitempty"`
}

type PgFips struct {
	CountyIDFips    string `bson:"countyid_fips,omitempty" json:"countyid_fips, omitempty"`
	SubCountyIDFips string `bson:"subcountyid_fips,omitempty" json:"subcountyid_fips, omitempty"`
	City            string `bson:"city,omitempty" json:"city,omitempty"`
	ZipCode         string `bson:"zipcode,omitempty" json:"zipcode,omitempty"`
}
