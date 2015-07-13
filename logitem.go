package main

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"github.com/fatih/structs"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"math/rand"
	"sort"
	"strings"
	"time"
)

/* {
 *   // Existing fields
 *   "message":"my message",
 *   "instance_id": "55914e901650d971d60000ab",
 *   "account_group_id":"55266f8611305a957d000016",
 *   "level":"debug",
 *   "exception":null,
 *   "timestamp":"2015-06-29T14:02:39+00:00",
 *   "pid":7,
 *
 *   // New fields
 *   originator: "originatorname", // ie the VM or container that generated the log message
 *   facility: "facility/procname",// the name of the process generating the log message or syslog facility
 *   user: "userid",               // the user who instantiated the action that led to the log message
 *
 *   // Parsed by us
 *   levelno: 3
 *  }
 */
type LogItem struct {
	// Things in Concerto
	Message        string    `json:"message"`
	InstanceId     string    `json:"instance_id"`
	AccountGroupId string    `json:"account_group_id"`
	Level          string    `json:"level"`
	Exception      string    `json:"exception"`
	OriginatorTime time.Time `json:"timestamp"`
	Pid            int       `json:"pid"`

	// Things added to Concerto
	OriginatorIp   string    `json:"originator_ip"`
	OriginatorPort int       `json:"originator_port"`
	Facility       string    `json:"facility"`
	Hostname       string    `json:"hostname"`
	User           string    `json:"user"`
	Time           time.Time `json:"time"`

	// Things we (re)calculate ourselves
	LevelNo       int    `json:"level_no"`
	Hash          string `json:"hash" slogger:"nohash"`
	PreviousHash  string `json:"previous_hash"`
	SequenceId    int64  `json:"sequence_id"`
	ShardGroup    int    `json:"shard_group"`
	FormatVersion int    `json:"format_version"`
	Verified      bool   `json:"verified" bson:",omitempty" slogger:"nohash,noquery,noindex"`
}

type LogItems []LogItem

var levelMap = map[string]int{
	"alert":   1,
	"crit":    2,
	"debug":   7,
	"emerg":   0,
	"err":     3,
	"error":   3,
	"info":    6,
	"none":    -1,
	"notice":  5,
	"panic":   0,
	"warn":    4,
	"warning": 4,
}

var levelMapInvert = map[int]string{
	0:  "emerg",
	1:  "alert",
	2:  "crit",
	3:  "err",
	4:  "warn",
	5:  "notice",
	6:  "info",
	7:  "debug",
	-1: "none",
}

var facilityMapInvert = map[int]string{
	0:  "kern",
	1:  "user",
	2:  "mail",
	3:  "daemon",
	4:  "auth",
	5:  "syslog",
	6:  "lpr",
	7:  "news",
	8:  "uucp",
	9:  "cron",
	10: "authpriv",
	11: "ftp",
	12: "netinfo",
	13: "remoteauth",
	14: "install",
	15: "ras",
	16: "local0",
	17: "local1",
	18: "local2",
	19: "local3",
	20: "local4",
	21: "local5",
	22: "local6",
	23: "local7",
}

// TODO: all these should be read from a configuration file
const (
	initialBackoff          = 1          // Initial backoff period in microseconds
	maximumBackoff          = 100 * 1000 // Maximum backoff period in microseconds
	iterationsBeforeBackoff = 5
	secret                  = "sekritsquirrel"
	shardGroup              = 1234
)

const (
	fpPresent = iota
	fpNoHash = iota
	fpNoQuery = iota
	fpNoIndex = iota
)

type fieldType struct {
	name string
	properties map[int] interface{}
}

var logItemFields map[string]fieldType
var logItemFieldList []string

func getFieldProperty(field string, p int) (*interface{}, bool) {
	prop, ok := logItemFields[strings.ToLower(field)].properties[p]
	if (ok) {
		return &prop, true
	} else {
		return nil, false
	}
}

func hasFieldProperty(field string, p int) bool {
	prop, ok := getFieldProperty(field, p)
	return ok && (prop!=nil) && ((*prop).(bool))
}

func setFieldProperty(field string, p int, prop interface{}) {
	logItemFields[strings.ToLower(field)].properties[p] = prop
}

func initFieldProperties() {
	logItemFields = make(map[string]fieldType)
	for _, f := range structs.Fields(&LogItem{}) {
		if f.IsExported() {
			name := f.Name()
			logItemFieldList = append(logItemFieldList, name)
			logItemFields[strings.ToLower(name)] = fieldType{name: name, properties: make(map[int]interface{})}
			setFieldProperty(name, fpPresent, true)
			if tag := f.Tag("slogger"); tag!="" {
				comps := strings.Split(tag, ",")
				for _, comp := range comps {
					switch comp {
					case "nohash":
						setFieldProperty(name, fpNoHash, true)
					case "noquery":
						setFieldProperty(name, fpNoQuery, true)
					case "noindex":
						setFieldProperty(name, fpNoIndex, true)
					}
				}
			}
		}
	}
	sort.Strings(logItemFieldList)
}

func (l *LogItem) normalise() {
	var ok bool
	l.LevelNo, ok = levelMap[strings.ToLower(l.Level)]
	if !ok {
		l.LevelNo = levelMap["none"]
	}
	// We should also check if it's too far from Now
	if l.Time.IsZero() {
		l.Time = time.Now()
	}
	if l.OriginatorTime.IsZero() {
		l.OriginatorTime = l.Time
	}
	l.FormatVersion = 1
	l.Verified = false
}

func levelToString(l int) string {
	if s, ok := levelMapInvert[l]; ok {
		return s
	}
	return levelMapInvert[-1]
}

func facilityToString(l int) string {
	if s, ok := facilityMapInvert[l]; ok {
		return s
	}
	return fmt.Sprintf("unknown [%d]", l)
}

func (l *LogItem) makeHash() {
	var b bytes.Buffer
	str := structs.New(l)
	for _, k := range logItemFieldList {
		v, ok := str.FieldOk(k)
		if (ok) {
			if !hasFieldProperty(k, fpNoHash) {
				switch t := v.Value().(type) {
				case time.Time:
					if !t.IsZero() {
						fmt.Fprintf(&b, "%x", t.UnixNano())
					}
				case string:
					fmt.Fprintf(&b, "%s", t)
				case int64:
					fmt.Fprintf(&b, "%x", t)
				case int:
					fmt.Fprintf(&b, "%x", t)
				case fmt.Stringer:
					fmt.Fprintf(&b, "%s", t.String())
				default:
					log.Panicf("Cannot stringify %s", k)
				}
			}
		}
		b.WriteByte(0)
	}
	fmt.Fprintf(&b, "%s", secret)
	sha := sha256.Sum256(b.Bytes())
	l.Hash = fmt.Sprintf("%064x", sha)
}

func (l *LogItem) checkHash() bool {
	tl := *l
	tl.makeHash()
	// Constant time compare probably unnecessary but let's err on the
	// side of caution
	//log.Printf("Compare: %s = %s", tl.Hash, l.Hash)
	return subtle.ConstantTimeCompare([]byte(tl.Hash), []byte(l.Hash)) == 1
}

func (l *LogItem) makeHashAndInsert(db *Database) {
	start := time.Now()
	sessionCopy := db.mongoSession.Copy()
	defer func() {
		sessionCopy.Close()
		log.Printf("Time to insert = %s\n", time.Since(start))
	}()

	// Convert to BSON and back to round times properly
	bytes, err := bson.Marshal(l)
	if (err != nil) {
		log.Panic("Cannot BSON marshal logitem")
	}
	if err := bson.Unmarshal(bytes, &l); err != nil {
		log.Panic("Cannot BSON unmarshal logitem")
	}
	
	l.ShardGroup = shardGroup
	backoff := initialBackoff

	c := db.getLogItemCollection(sessionCopy)

	for iteration := 0; ; iteration++ {
		var previous LogItem = LogItem{}
		if err := c.Find(bson.M{"shardgroup": l.ShardGroup}).Select(bson.M{"sequenceid": 1, "hash": 1}).Sort("-sequenceid").Limit(1).One(&previous); err != nil {
			if err != mgo.ErrNotFound {
				log.Panicf("Query returned error %v\n", err)
			}
			l.PreviousHash = ""
			l.SequenceId = 0
		} else {
			l.PreviousHash = previous.Hash
			l.SequenceId = previous.SequenceId + 1
		}
		l.makeHash()
		err := c.Insert(l)
		if err == nil {
			if backoff >= maximumBackoff {
				log.Printf("Succeeded only after %d iterations\n", iteration)
			}
			l.Verified = true
			break
		}
		if !mgo.IsDup(err) {
			log.Panicf("Could not insert record %v\n", err)
		}
		if iteration >= iterationsBeforeBackoff {
			time.Sleep(time.Duration(1+rand.Int()%backoff) * time.Microsecond)
			backoff *= 2
			if backoff > maximumBackoff {
				backoff = maximumBackoff
			}
		}
	}
}

// Returns whether the set of result values has been validated as complete
func queryLogItems(db *Database, query interface{}, sortOrder []string, limit int, ch chan LogItem) (int, bool) {
	start := time.Now()
	sessionCopy := db.mongoSession.Copy()
	defer func() {
		sessionCopy.Close()
		log.Printf("Time to reply = %s\n", time.Since(start))
	}()

	items := 0
	c := db.getLogItemCollection(sessionCopy)

	q := c.Find(query).Sort(append(sortOrder, "_id")...)
	if (limit > 0) {
		q = q.Limit(limit)
	}
	iter := q.Iter()
	defer iter.Close()

	var result LogItem
	for iter.Next(&result) {
		result.Verified = result.checkHash()
		ch <- result
		items++
	}
	if err := iter.Err(); err != nil {
		log.Panicf("Error while iterating: %v\n", err)
	}
	return items, true
}
