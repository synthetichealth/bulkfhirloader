package bulkfhirloader

type RawStats struct {
	Id              string   `bson:"_id" json:"_id"`
	City            string   `bson:"city" json:"city"`
	ZipCode         string   `bson:"zipcode" json:"zipcode"`
	Gender          string   `bson:"gender" json:"gender"`
	ConditionSNOMED []string `bson:"conditionsnomed" json:"conditionsnomed"`
}
