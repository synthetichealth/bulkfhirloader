package bulkfhirloader

type RawStats struct {
	ID              string          `bson:"_id" json:"_id"`
	City            string          `bson:"city" json:"city"`
	ZipCode         string          `bson:"zipcode" json:"zipcode"`
	Gender          string          `bson:"gender" json:"gender"`
	DeceasedBoolean bool            `bson:"deceasedBoolean,omitempty" json:"deceasedBoolean,omitempty"`
	Conditions      []ConditionCode `bson:"conditions" json:"conditions"`
}

type ConditionCode struct {
	System string `bson:"system,omitempty" json:"system,omitempty"`
	Code   string `bson:"code,omitempty" json:"code,omitempty"`
}
