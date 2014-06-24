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

	// actionScript contains a single rule with an action.
	actionScript struct {
		Rule    operation `json:"rule"`
		Success operation `json:"success"`
		Failed  operation `json:"failed"`
	}
)

// mongoCall provides mongodb execution support.
type mongoCall func(*mgo.Collection) error

// RunAction sets up and executes the specified action.
func RunAction(actionName string, user string) error {
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

	// Run the specified action
	return runAction(session, user, actionName)
}

// FindAdvice runs a set of data aggregations through the MongoDB pipeline and displays to the terminal
// the advice for the user.
func runAction(session *mgo.Session, user string, actionName string) error {
	// Retrieve the actions to run.
	action, err := retrieveAction(actionName)
	if err != nil {
		log.Println(err)
		return err
	}

	// Process the action.
	return processAction(session, action, user)
}

// retrieveAction reads and unmarshals the specified action data file.
func retrieveAction(actionName string) (*actionScript, error) {
	// Open the file.
	file, err := os.Open("actions/" + actionName + ".json")
	if err != nil {
		return nil, err
	}

	// Schedule the file to be closed once the function returns.
	defer file.Close()

	// Decode the file into a value of the actionScript type.
	var action actionScript
	err = json.NewDecoder(file).Decode(&action)

	// We don't need to check for errors, the caller can do this.
	return &action, err
}

// processAction execute the queries against the aggregation pipeline.
func processAction(session *mgo.Session, action *actionScript, user string) error {
	// Process the rule and check for results
	results, err := executeOperations(session, &action.Rule, user)
	if err != nil {
		return err
	}

	// If no result is returned, provide the failed result
	if len(results) == 0 {
		results, err = executeOperations(session, &action.Failed, user)
	} else {
		results, err = executeOperations(session, &action.Success, user)
	}

	if err != nil {
		log.Println("Unable To Process Action", err)
		return err
	}

	return nil
}

// executeOperations builds an aggregation pipeline query based on the
// specififed expressions.
func executeOperations(session *mgo.Session, op *operation, user string) ([]bson.M, error) {
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

	// Execute the aggregation pipeline expressions.
	var result []bson.M
	err = mongoDB(session, op.Collection,
		func(collection *mgo.Collection) error {
			return collection.Pipe(operations).All(&result)
		})

	log.Println(result)
	return result, err
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

// mongoDB the MongoDB literal function.
func mongoDB(mongoSession *mgo.Session, collectionName string, mc mongoCall) error {
	// Capture the specified collection.
	collection := mongoSession.DB(TestDatabase).C(collectionName)
	if collection == nil {
		return fmt.Errorf("Collection %s does not exist", collectionName)
	}

	// Execute the mongo call.
	err := mc(collection)
	if err != nil {
		return err
	}

	return nil
}
