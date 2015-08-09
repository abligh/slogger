package main

import (
	"encoding/json"
	"fmt"
	"github.com/abligh/go-syslog"
	"github.com/jeromer/syslogparser"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

func getPartString(logParts *syslogparser.LogParts, key string) (string, bool) {
	if _, ok := (*logParts)[key]; !ok {
		return "", false
	}
	switch t := (*logParts)[key].(type) {
	case string:
		return t, true
	case fmt.Stringer:
		return t.String(), true
	case int:
		return fmt.Sprintf("%d", t), true
	}
	return "", false
}

func getPartInt(logParts *syslogparser.LogParts, key string) (int, bool) {
	if _, ok := (*logParts)[key]; !ok {
		return 0, false
	}
	switch t := (*logParts)[key].(type) {
	case int:
		return t, true
	case string:
		if r, err := strconv.Atoi(t); err == nil {
			return r, true
		}
	case fmt.Stringer:
		if r, err := strconv.Atoi(t.String()); err == nil {
			return r, true
		}
	}
	return 0, false
}

func getPartTime(logParts *syslogparser.LogParts, key string) (time.Time, bool) {
	format := "2015-07-03 16:40:54 +0000 UTC"
	switch t := (*logParts)[key].(type) {
	case time.Time:
		return t, true
	case string:
		if tim, err := time.Parse(t, format); err == nil {
			return tim, true
		}
	case fmt.Stringer:
		if tim, err := time.Parse(t.String(), format); err == nil {
			return tim, true
		}
	case int64:
		return time.Unix(t, 0), true
	case int:
		return time.Unix(int64(t), 0), true
	}
	return time.Time{}, false
}

func processLogParts(db *Database, logParts syslogparser.LogParts) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("panic caught: %+v", err)
		}
	}()
	var logItem LogItem
	if client, ok := getPartString(&logParts, "client"); ok {
		if host, port, err := net.SplitHostPort(client); err == nil {
			logItem.OriginatorIp = host
			if iPort, err := strconv.Atoi(port); err != nil {
				logItem.OriginatorPort = iPort
			}
		}
	}
	if time, ok := getPartTime(&logParts, "timestamp"); ok {
		logItem.OriginatorTime = time
	}
	if severity, ok := getPartInt(&logParts, "severity"); ok {
		logItem.Level = levelToString(severity)
	}
	if facility, ok := getPartInt(&logParts, "facility"); ok {
		logItem.Facility = facilityToString(facility)
	}
	if hostname, ok := getPartString(&logParts, "hostname"); ok {
		logItem.Hostname = hostname
	}
	if msg, ok := getPartString(&logParts, "content"); ok {
		if tag, ok := getPartString(&logParts, "tag"); ok {
			// msg AND tag
			combined := fmt.Sprintf("%s:%s", tag, msg)
			if strings.Contains(tag, "{") {
				// tag has { in it, which means it was one single piece of JSON
				if err := json.Unmarshal([]byte(combined), &logItem); err != nil {
					logItem.Message = combined
				}
			} else if strings.Contains(msg, "{") {
				// tag does not have { in it, but msg does, so try interpreting msg as JSON
				if err := json.Unmarshal([]byte(msg), &logItem); err != nil {
					logItem.Message = combined
				}
			} else {
				// neither has a { in it, so it's not JSON
				logItem.Message = combined
			}
		} else {
			// msg only, no tag
			if strings.Contains(msg, "{") {
				// tbut msg has a {, so try interpreting msg as JSON
				if err := json.Unmarshal([]byte(msg), &logItem); err != nil {
					logItem.Message = msg
				}
			} else {
				logItem.Message = msg
			}
		}
	} else {
		if tag, ok := getPartString(&logParts, "tag"); ok {
			logItem.Message = tag
		}
	}
	// override any supplied rx time - we keep the originator time
	logItem.Time = time.Now()
	logItem.normalise()
	logItem.makeHashAndInsert(db)
}

func syslogServerStart(db *Database) {
	// something here
	channel := make(syslog.LogPartsChannel)
	handler := syslog.NewChannelHandler(channel)

	server := syslog.NewServer()
	server.SetFormat(syslog.Automatic)
	server.SetHandler(handler)
	server.ListenTCP("0.0.0.0:10514")
	server.ListenUDP("0.0.0.0:10514")
	server.Boot()

	go func(channel syslog.LogPartsChannel) {
		for logParts := range channel {
			processLogParts(db, logParts)
		}
	}(channel)

	server.Wait()
}
