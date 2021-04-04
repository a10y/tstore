package main

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/arrow/flight"

	"github.com/apache/arrow/go/arrow/array"

	"github.com/apache/arrow/go/arrow/memory"

	"github.com/apache/arrow/go/arrow"
)

// RealEstateSchema is the Apache Arrow schema for our NYC home sale
// data, which we want to store as an Arrow table.
var RealEstateSchema *arrow.Schema = arrow.NewSchema([]arrow.Field{
	{
		Name:     "zipcode",
		Type:     arrow.BinaryTypes.String,
		Nullable: false,
	},
	{
		Name:     "date",
		Type:     arrow.FixedWidthTypes.Date64,
		Nullable: false,
	},
	{
		Name:     "price",
		Type:     arrow.PrimitiveTypes.Uint32,
		Nullable: false,
	},
}, nil)

func main() {
	fmt.Println("hello tstore")

	pool := memory.DefaultAllocator

	// Generate a new Arrow record for reading here...
	records := array.NewRecordBuilder(pool, RealEstateSchema)
	defer records.Release()

	test(records, 1)
	test(records, 1)
	test(records, 1_000)
	test(records, 1_000_000)

	// What to do with records?
	// fmt.Printf("%v\n", record1)
}

func test(records *array.RecordBuilder, n int) {
	fmt.Printf("Records: %v\n", n)
	start := time.Now()
	generate(records, 1_000_000)
	elapsed := time.Since(start).Nanoseconds()

	fmt.Printf("ns emitting record: %9d\n", elapsed)

	// Flush this as a record
	start = time.Now()
	record1 := records.NewRecord()
	elapsed = time.Since(start).Nanoseconds()
	fmt.Printf("ns spent packing:   %9d\n", elapsed)

	record1.Release()
}

// generate the desired number of random records into the builder.
func generate(records *array.RecordBuilder, n int) {
	field1 := records.Field(0).(*array.StringBuilder)
	field2 := records.Field(1).(*array.Date64Builder)
	field3 := records.Field(2).(*array.Uint32Builder)

	for i := 0; i < n; i++ {
		field1.Append("10014")
		field2.Append(arrow.Date64(time.Now().UnixNano()))
		field3.Append(1_000_000)
	}
}

func setupFlight() {
	srv := flight.NewFlightServer(nil)
	srv.Serve()
}
