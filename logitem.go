package main

import (
	"fmt"
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
	LevelNo int `json:"level_no"`
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
