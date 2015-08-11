package main

import (
	"github.com/fatih/structs"
	"labix.org/v2/mgo"
	"log"
	"sort"
	"strings"
	"time"
)

var (
	mongoDBHosts   []string
	authDatabase   string
	authUserName   string
	authPassword   string
	databaseName   string = "slogger"
	collectionName string = "logitems"
)

type Database struct {
	mongoSession    *mgo.Session
	mongoDBDialInfo *mgo.DialInfo
}

var jsonMap map[string]string

func newDatabase() *Database {

	database := new(Database)
	// establish a connection
	database.mongoDBDialInfo = &mgo.DialInfo{
		Addrs:   mongoDBHosts,
		Timeout: 60 * time.Second,
	}

	if authUserName != "" && authPassword != "" {
		database.mongoDBDialInfo.Username = authUserName
		database.mongoDBDialInfo.Password = authPassword
		if authDatabase != "" {
			database.mongoDBDialInfo.Database = authDatabase
		}
	}

	log.Printf("Connecting to mongo on %s", strings.Join(mongoDBHosts, ","))

	// Create a session which maintains a pool of socket connections
	// to our MongoDB.
	var err error
	database.mongoSession, err = mgo.DialWithInfo(database.mongoDBDialInfo)
	if err != nil {
		log.Fatalf("Can create a mongo session: %s", err)
	}

	// Reads may not be entirely up-to-date, but they will always see the
	// history of changes moving forward, the data read will be consistent
	// across sequential queries in the same session, and modifications made
	// within the session will be observed in following queries (read-your-writes).
	// http://godoc.org/labix.org/v2/mgo#Session.SetMode
	database.mongoSession.SetMode(mgo.Monotonic, true)
	database.mongoSession.SetSafe(&mgo.Safe{WMode: "majority"})

	database.ensureIndices()

	return database
}

func (db *Database) getLogItemCollection(s *mgo.Session) *mgo.Collection {
	return s.DB(databaseName).C(collectionName)
}

func (db *Database) ensureIndices() {
	// We want to ensure that every field in mongo is indexed.
	keys := structs.Names(&LogItem{})
	for i := range keys {
		keys[i] = strings.ToLower(keys[i])
	}
	sort.Strings(keys)

	sessionCopy := db.mongoSession.Copy()
	defer sessionCopy.Close()

	c := db.getLogItemCollection(sessionCopy)

	for _, k := range keys {
		index := mgo.Index{
			Key: []string{k},
		}
		switch k {
		case "sequenceid":
			index.Key = append(index.Key, "shardgroup")
			index.Unique = true
		}
		if hasFieldProperty(k, fpNoIndex) {
			continue
		}
		if err := c.EnsureIndex(index); err != nil {
			panic("Could not add index")
		}
	}
}

func buildJsonMap() {
	jsonMap = make(map[string]string)
	fields := structs.Fields(&LogItem{})
	for _, f := range fields {
		if f.IsExported() {
			fname := f.Name()
			mname := strings.ToLower(fname)
			jname := fname
			if tag := f.Tag("json"); tag != "" {
				jname = strings.Split(tag, ",")[0]
			}
			jsonMap[jname] = mname
		}
	}
}
