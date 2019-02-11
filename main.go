package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
)

func main() {
	log.Println("BigQuery InsertAll Testing")

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, os.Getenv("GOOGLE_PROJECT_ID"))
	if err != nil {
		log.Println("unable to create client:", err)
		return
	}
	ds := client.Dataset("abqtest")

	err = ds.Create(ctx, &bigquery.DatasetMetadata{Location: "EU"})
	if err != nil {
		gerr, ok := err.(*googleapi.Error)
		if !ok {
			log.Println(err)
			return
		} else if gerr != nil && gerr.Code != 409 {
			log.Println("dataset creation issue:", gerr.Error())
			return
		}

		log.Println("dataset exists, carry on")
	}

	schema, err := bigquery.InferSchema(student{})
	if err != nil {
		log.Println("unable to infer schema:", err)
		return
	}

	md := &bigquery.TableMetadata{
		Name:   "testing",
		Schema: schema,
	}

	t := ds.Table("testing")
	err = t.Create(ctx, md)
	if err != nil {
		gerr, ok := err.(*googleapi.Error)
		if !ok {
			log.Println(err)
			return
		} else if gerr != nil && gerr.Code != 409 {
			log.Println("table creation issue:", gerr.Error())
			return
		}

		log.Println("table exists, carry on")
	}

	log.Println("Building 10,000 saver structs")

	savers := make([]*bigquery.StructSaver, 10000)
	for i := range savers {
		savers[i] = &bigquery.StructSaver{Struct: student{Name: "Pass", Grades: []int{10, 20, 30, 40}, Required:[]byte{0x00}}, Schema: schema, InsertID: fmt.Sprintf("id%v", i)}
	}
	savers[9999].Struct = &bigquery.StructSaver{Struct: student{Name: "Fail", Grades: []int{10, 20, 30, 40}}, Schema: schema, InsertID: "id9999"}

	log.Println("Putting 10,000 saver structs")
	// should fail on empty field "Required"
	err = t.Inserter().Put(ctx, savers)
	if err != nil {
		log.Println("put error:", err)
		me, ok := err.(bigquery.PutMultiError)
		if ok {
			// entire insertion fails but the cause appears to be bubbled to the top.
			log.Println("put error len:", len(me))
			log.Println(me[0])
			log.Println(me[1])

			/*

resulting log output
~~~~~~~~~~~~~~~~~~~~
2019/02/11 14:54:00 put error len: 10000
2019/02/11 14:54:00 {id9999 9999 {Location: ""; Message: ""; Reason: "invalid"}}
2019/02/11 14:54:00 {id0 0 {Location: ""; Message: ""; Reason: "stopped"}}

			 */
		}
		return
	}

	log.Println("jobs done...")
}

/*
savers := []*bigquery.StructSaver{
	{Struct: student{Name: "Pass", Grades: []int{10, 20, 30, 40}, Required:[]byte{0x00}}, Schema: schema, InsertID: "id0"},
	{Struct: student{Name: "Fail", Grades: []int{10, 20, 30, 40}}, Schema: schema, InsertID: "id1"},
	{Struct: student{Name: "Pass", Grades: []int{10, 20, 30, 40}, Required:[]byte{0x00}}, Schema: schema, InsertID: "id2"},
}

for _, e := range me {
	log.Println(e.InsertID, e.Errors)
}
*/

type student struct {
	Name     string `bigquery:"full_name"`
	Grades   []int  `bigquery:"grades"`
	Secret   string `bigquery:"-"`
	Required []byte `bigquery:"required"`
}
