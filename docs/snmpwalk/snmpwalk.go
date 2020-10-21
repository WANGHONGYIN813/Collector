package main

import (
	"fmt"
	g "github.com/soniah/gosnmp"
	"log"
)

func main() {

	g.Default.Target = "10.0.24.42"
	err := g.Default.Connect()
	if err != nil {
		log.Fatalf("Connect() err: %v", err)
	}
	defer g.Default.Conn.Close()

	//oids := []string{"1.3.6.1.2.1.1.4.0", "1.3.6.1.2.1.1.3.0", "1.3.6.1.2.1.2.2.1.2"}
	oids := []string{"1.3.6.1.2.1.1.4.0"}
	result, err2 := g.Default.Get(oids)
	if err2 != nil {
		log.Fatalf("Get() err: %v", err2)
	}

	for i, variable := range result.Variables {

		fmt.Println("variable.is", variable)

		fmt.Printf("variable.Name is %s\n", variable.Name)

		fmt.Printf("%d: oid: %s \n", i, variable.Name)

		fmt.Println("variable.Type is", variable.Type)

		switch variable.Type {
		case g.OctetString:
			fmt.Printf("is string!!")
			fmt.Printf("string is : %s\n", string(variable.Value.([]byte)))
		case g.Integer:
			fmt.Printf("is number !!")
			fmt.Printf("number is : %d\n", g.ToBigInt(variable.Value))
		default:
			fmt.Printf("is other!")
			fmt.Printf("number is : %d\n", g.ToBigInt(variable.Value))
		}
	}
}
