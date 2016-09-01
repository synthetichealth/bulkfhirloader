package bulkfhirloader

type RawStats struct {
	City            string   `bson:"city" json:"city"`
	ZipCode         string   `bson:"zipcode" json:"zipcode"`
	Gender          string   `bson:"gender" json:"gender"`
	ConditionSNOMED []string `bson:"conditionsnomed" json:"conditionsnomed"`
}
