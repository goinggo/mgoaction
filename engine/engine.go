// Copyright 2014 Ardan Studios. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package engine

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

// MongoDB connection information.
const (
	MongoDBHosts = "ds035428.mongolab.com:35428"
	AuthDatabase = "goinggo"
	AuthUserName = "guest"
	AuthPassword = "welcome"
	TestDatabase = "goinggo"
)

type (
	// operation contains a set of expressions for a collection.
	operation struct {
		Collection  string   `json:"collection"`
		Expressions []string `json:"expressions"`
	}

	// rule contains a single rule with an action.
	rule struct {
		Test    operation `json:"test"`
		Success operation `json:"success"`
		Failed  operation `json:"failed"`
	}
)

// mongoCall provides mongodb execution support.
type mongoCall func(*mgo.Collection) error

// RunRule sets up and executes the specified rule.
func RunRule(ruleName string, user string) error {
	// We need this object to establish a session to our MongoDB.
	mongoDBDialInfo := mgo.DialInfo{
		Addrs:    []string{MongoDBHosts},
		Timeout:  60 * time.Second,
		Database: AuthDatabase,
		Username: AuthUserName,
		Password: AuthPassword,
	}

	// Create a session which maintains a pool of socket connections
	// to our MongoDB.
	session, err := mgo.DialWithInfo(&mongoDBDialInfo)
	if err != nil {
		log.Println("CreateSession:", err)
		return err
	}

	// Schedule the close to occur once the function returns.
	defer session.Close()

	// Reads may not be entirely up-to-date, but they will always see the
	// history of changes moving forward, the data read will be consistent
	// across sequential queries in the same session, and modifications made
	// within the session will be observed in following queries (read-your-writes).
	// http://godoc.org/labix.org/v2/mgo#Session.SetMode
	session.SetMode(mgo.Monotonic, true)

	// Run the specified rule
	return runRule(session, user, ruleName)
}

// runRule runs a set of expressions through the MongoDB aggregation pipeline and
// displays to the terminal the result for the user.
func runRule(session *mgo.Session, user string, ruleName string) error {
	// Retrieve the actions to run.
	r, err := retrieveRule(ruleName)
	if err != nil {
		log.Println(err)
		return err
	}

	// Process the rule.
	return processRule(session, r, user)
}

// retrieveRule reads and unmarshals the specified rule data file.
func retrieveRule(ruleName string) (*rule, error) {
	// Open the file.
	file, err := os.Open("rules/" + ruleName + ".json")
	if err != nil {
		return nil, err
	}

	// Schedule the file to be closed once the function returns.
	defer file.Close()

	// Decode the file into a value of the rule type.
	var r rule
	err = json.NewDecoder(file).Decode(&r)

	// We don't need to check for errors, the caller can do this.
	return &r, err
}

// processRule execute the expressions against the aggregation pipeline.
func processRule(session *mgo.Session, r *rule, user string) error {
	// Process the rule and check for results
	results, err := executeOperation(session, r.Test, user)
	if err != nil {
		log.Println("Unable To Process Action", err)
		return err
	}

	if len(results) == 0 {
		// If no result is returned, provide the failed result
		results, err = executeOperation(session, r.Failed, user)
	} else {
		// Provide the success result
		results, err = executeOperation(session, r.Success, user)
	}

	if err != nil {
		log.Println("Unable To Process Action", err)
		return err
	}

	log.Println(results)
	return nil
}

// executeOperation builds an aggregation pipeline query based on the
// specififed expressions.
func executeOperation(session *mgo.Session, op operation, user string) ([]bson.M, error) {
	var err error
	operations := make([]bson.M, len(op.Expressions))

	// Iterate through the set of expressions and build the slice
	// of operations.
	for index, exp := range op.Expressions {
		if index := strings.Index(exp, "#userId#"); index >= 0 {
			exp = strings.Replace(exp, "#userId#", user, -1)
		}

		log.Println(exp)
		operations[index] = unmarshalOperation(exp)
	}

	collection := session.DB(TestDatabase).C(op.Collection)
	if collection == nil {
		return nil, fmt.Errorf("Collection %s does not exist", op.Collection)
	}

	// Execute the aggregation pipeline expressions.
	var results []bson.M
	err = collection.Pipe(operations).All(&results)

	return results, err
}

// unmarshalOperation converts a JSON string into a BSON map.
func unmarshalOperation(expression string) bson.M {
	// Convert the expression to a byte slice.
	op := []byte(expression)

	// Unmarshal the expression to a bson map.
	operation := make(bson.M)
	json.Unmarshal(op, &operation)
	return operation
}
