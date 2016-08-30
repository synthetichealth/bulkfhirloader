package bulkfhirloader

type RawStats struct {
	City            string   `bson:"city" json:"city"`
	Gender          string   `bson:"gender" json:"gender"`
	ConditionSNOMED []string `bson:"conditionsnomed" json:"conditionsnomed"`
}
