package main

import (
	"fmt"
	"github.com/abligh/go-syslog"
	"github.com/jeromer/syslogparser"
	"net"
	"strconv"
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

func syslogServerStart(db *Database) {
	// something here
	channel := make(syslog.LogPartsChannel)
	handler := syslog.NewChannelHandler(channel)

	server := syslog.NewServer()
	//server.SetFormat(syslog.RFC5424)
	server.SetFormat(syslog.RFC3164)
	server.SetHandler(handler)
	server.ListenUDP("0.0.0.0:10514")
	server.Boot()

	go func(channel syslog.LogPartsChannel) {
		for logParts := range channel {
			fmt.Println(logParts)
			var logItem LogItem
			if tag, ok := getPartString(&logParts, "tag"); ok {
				if msg, ok := getPartString(&logParts, "content"); ok {
					logItem.Message = fmt.Sprintf("%s %s", tag, msg)
				} else {
					logItem.Message = tag
				}
			} else {
				if msg, ok := getPartString(&logParts, "content"); ok {
					logItem.Message = msg
				}
			}
			if stime, ok := getPartString(&logParts, "timestamp"); ok {
				if time, err := time.Parse(stime, "2015-07-03 16:40:54 +0000 UTC"); err == nil {
					logItem.Time = time
				}
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
			if client, ok := getPartString(&logParts, "client"); ok {
				if host, port, err := net.SplitHostPort(client); err == nil {
					logItem.OriginatorIp = host
					if iPort, err := strconv.Atoi(port); err != nil {
						logItem.OriginatorPort = iPort
					}
				}
			}
			logItem.normalise()
			db.insertLogItem(logItem)
		}
	}(channel)

	server.Wait()
}
