package main

import (
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"github.com/abligh/cdl"
	"github.com/abligh/go-syslog"
	"io/ioutil"
	"log"
)

var serviceTypeEnum = cdl.NewEnumType("syslog", "rest")
var protocolEnum = cdl.NewEnumType("tcp", "udp")

var defaultConfig string = `
{
	"services" : [
		{
			"type": "syslog",
			"listen": "127.0.0.1:10514",
			"protocol": "udp"
		},
		{
			"type": "rest",
			"listen": "127.0.0.1:10080",
			"protocol": "tcp"
		}
	],
	"db" : {
		"mongoservers": [ "127.0.0.1:27017" ],
		"database": "slogger",
		"collection": "logitems"
	}
}
`

type service struct {
	serviceType cdl.Enum
	protocol    cdl.Enum
	listen      string
	certpath    string
	keypath     string
	cacertpath  string
}

func newService() service {
	return service{serviceType: serviceTypeEnum.New("syslog"), protocol: protocolEnum.New("udp")}
}

var services []service

func readConfig() {
	template := cdl.Template{
		"/":            "{}services?{1,} db",
		"services":     "{}type listen protocol certpath? keypath? cacertpath?",
		"type":         serviceTypeEnum,
		"listen":       "ipport",
		"protocol":     protocolEnum,
		"db":           "{}mongoservers{1,} database collection authdatabase? username? password?",
		"mongoservers": "ipport",
	}

	if ct, err := cdl.Compile(template); err != nil {
		log.Fatalf("Cannot compile configuration template: %v", err)
	} else {

		var config []byte

		configFile := flag.String("configfile", "", "path to JSON config file")
		flag.Parse()

		if *configFile == "" {
			config = []byte(defaultConfig)
		} else {
			var err error
			config, err = ioutil.ReadFile(*configFile)
			if err != nil {
				log.Fatal("Cannot read config file " + *configFile)
			}
		}

		var conf interface{}
		if err := json.Unmarshal(config, &conf); err != nil {
			log.Fatalf("Config JSON parse error: %v ", err)
		}

		var newServ = newService()

		configurator := cdl.Configurator{
			"mongoserver": func(o interface{}, p cdl.Path) *cdl.CdlError {
				mongoDBHosts = append(mongoDBHosts, o.(string))
				return nil
			},
			"database":     &databaseName,
			"collection":   &collectionName,
			"authdatabase": &authDatabase,
			"username":     &authUserName,
			"password":     &authPassword,

			"services": func(o interface{}, p cdl.Path) *cdl.CdlError {
				if newServ.serviceType.String() == "rest" && newServ.protocol.String() != "tcp" {
					return cdl.NewError("ErrBadOption").SetSupplementary("rest service can only run over tcp")
				}
				if newServ.certpath != "" || newServ.keypath != "" || newServ.cacertpath != "" {
					if newServ.protocol.String() != "tcp" {
						return cdl.NewError("ErrBadOption").SetSupplementary("tls can only run over tcp")
					}
					if newServ.certpath == "" || newServ.keypath == "" {
						return cdl.NewError("ErrBadOption").SetSupplementary("tls needs both a keypath and a certpath")
					}
				}
				services = append(services, newServ)
				newServ = newService()
				return nil
			},
			"type":       &newServ.serviceType,
			"listen":     &newServ.listen,
			"protocol":   &newServ.protocol,
			"certpath":   &newServ.certpath,
			"keypath":    &newServ.keypath,
			"cacertpath": &newServ.cacertpath,
		}

		if err := ct.Validate(conf, configurator); err != nil {
			log.Fatalf("Error reading configuration: %s", err)
		}

		if len(mongoDBHosts) == 0 {
			mongoDBHosts = []string{"127.0.0.1:27017"}
		}
	}
}

func getServiceConfig(s service) *tls.Config {

	if cert, err := ioutil.ReadFile(s.certpath); err != nil {
		log.Fatal("Cannot read certs from " + s.certpath)
	} else {
		if key, err := ioutil.ReadFile(s.keypath); err != nil {
			log.Fatal("Cannot read key from " + s.keypath)
		} else {
			certificate, err := tls.X509KeyPair(cert, key)
			if err != nil {
				log.Fatal("Error interpreting certificate or key from %s, %s: %v", s.certpath, s.keypath, err)
			} else {
				config := tls.Config{
					ClientAuth:   tls.RequireAndVerifyClientCert,
					MinVersion:   tls.VersionTLS12,
					Certificates: []tls.Certificate{certificate},
				}

				if s.cacertpath != "" {
					capool := x509.NewCertPool()
					if cacerts, err := ioutil.ReadFile(s.cacertpath); err != nil {
						log.Fatal("Cannot read cacerts from " + s.cacertpath)
					} else {
						if ok := capool.AppendCertsFromPEM(cacerts); !ok {
							log.Fatal("Cannot add certs from " + s.cacertpath)
						}
						config.ClientCAs = capool
					}
				}

				config.Rand = rand.Reader
				return &config
			}
		}
	}
	panic("Internal error") // as apparently log.Fatal might not return (wtf?)
}

func startServices(db *Database) {

	server := syslog.NewServer()

	for _, s := range services {
		switch s.serviceType.String() {
		case "syslog":
			switch s.protocol.String() {
			case "udp":
				log.Printf("Starting syslog UDP on %s\n", s.listen)
				server.ListenUDP(s.listen)
			case "tcp":
				if s.certpath != "" {
					log.Printf("Starting syslog TCP+TLS on %s\n", s.listen)
					server.ListenTCPTLS(s.listen, getServiceConfig(s))
				} else {
					log.Printf("Starting syslog TCP on %s\n", s.listen)
					server.ListenTCP(s.listen)
				}
			}
		case "rest":
			if s.certpath != "" {
				log.Printf("Starting https on %s\n", s.listen)
				go httpsServerStart(db, s.listen, getServiceConfig(s))
			} else {
				log.Printf("Starting http on %s\n", s.listen)
				go httpServerStart(db, s.listen)
			}
		}
	}

	syslogServerRun(server, db)
}
