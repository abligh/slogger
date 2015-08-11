package main

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Route struct {
	Name        string
	Method      string
	Pattern     string
	HandlerFunc ContextHandlerFunc
}

type Context struct {
	route Route
	db    *Database
}

type ContextHandlerFunc func(c *Context, w http.ResponseWriter, r *http.Request)

type Routes []Route

var routes = Routes{
	Route{
		"CreateLogItem",
		"POST",
		"/logitem/create",
		createLogItem,
	},
	Route{
		"QueryLogItem",
		"GET",
		"/logitem/query",
		queryLogItem,
	},
}

/*
 * The following formats of query are permissible
 *
 * 0. Everything
 *   {}
 *
 * 0. Equality
 *   { fname: fval }
 *
 * 0. Multiple equality
 *   { fname1: fval1, fname2: fval2 }
 *
 * 1. Relational operators: $eq, $ne, $gt, $gte, $lt, $lte
 *   { fname: { $gt : fval1} }
 *
 * 2. List matches: $in, $nin
 *   { fname: { $in: [ fval, fval, fval ] } }
 *
 * 3. Unary operators: $not
 *   { fname1: { $not: { $eq : fval1} }
 *
 * 4. Logical operators: $or, $and, $nor
 *   { $or: [ { fname1: fval1} , {fname2: fval2}, {fname2: fval2} ] }
 */

func validateFieldQuery(t *map[string]interface{}) error {
	if len(*t) != 1 {
		return errors.New("JSON secondary query operators are a map with exactly one key")
	}
	// this for loop is redundant really as there should be only one entry
	for kk, vv := range *t {
		switch kk {
		case "$eq", "$ne", "$gt", "$gte", "$lt", "$lte":
			// Relational operator takes one value only
			switch vv.(type) {
			case bool, int, int64, uint, uint64, string, float64, time.Time:
				// These are OK - continue
			default:
				return errors.New("JSON relational operator must take simple field value")
			}
		case "$in", "$nin":
			if a, ok := vv.([]interface{}); ok {
				for i := range a {
					switch (a[i]).(type) {
					case bool, int, int64, uint, uint64, string, float64, time.Time:
						// These are OK - continue
					default:
						return errors.New("JSON list operator must take array of simple field values")
					}
				}
			} else {
				return errors.New("JSON list match operator must take an array")
			}
		case "$not":
			if m, ok := vv.(map[string]interface{}); ok {
				if err := validateFieldQuery(&m); err != nil {
					return err
				}
			} else {
				return errors.New("JSON unary primary query operators must take a map")
			}
		default:
			return errors.New("Unknown JSON secondary query operator")
		}
	}
	return nil
}

func jsonToDbKeys(i *interface{}) error {
	if m, ok := (*i).(map[string]interface{}); ok {
		// fix up a map
		nm := make(map[string]interface{})
		for k, v := range m {
			// First see if it is a valid field name and if so translate it
			if jk, ok := jsonMap[k]; ok && !hasFieldProperty(jk, fpNoQuery) {
				// The value must either be:
				// 0. a straight value
				// 1. a map containing a single element of a relational operator and a value
				// 2. a map containing a single element being an 'in' operator an an array
				// 3. a map containing a single element being 'not' then either 1 or 2
				switch t := v.(type) {
				case bool, int, int64, uint, uint64, string, float64, time.Time:
					// These are OK - continue
				case map[string]interface{}:
					if err := validateFieldQuery(&t); err != nil {
						return err
					}
				default:
					return errors.New("JSON field key with unrecognised value type")
				}
				nm[jk] = v
			} else if strings.HasPrefix(k, "$") {
				// The operator may be either
				// 3. A logical operator containing an array of 2 or more match statements
				// 4. A unary operator containing a match statement
				// In each case we recurse into this routing
				switch k {
				case "$or", "$and", "$nor":
					if a, ok := v.([]interface{}); ok && (len(a) > 0) {
						for i := range a {
							if _, ok := (a[i]).(map[string]interface{}); !ok {
								return errors.New("JSON logical primary query operators must take an array consisting only of maps")
							}
							if err := jsonToDbKeys(&a[i]); err != nil {
								return err
							}
						}
					} else {
						return errors.New("JSON logical primary query operators must take a non-empty array")
					}
				default:
					return errors.New("Bad JSON primary query operator")
				}
				nm[k] = v
			} else {
				return errors.New("Bad JSON key")
			}
		}
		*i = nm
	} else {
		return errors.New("JSON primary query must be a map")
	}
	return nil
}

func wrapLogger(inner ContextHandlerFunc) ContextHandlerFunc {
	return func(c *Context, w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		inner(c, w, r)

		log.Printf(
			"%s\t%s\t%s\t%s",
			r.Method,
			r.RequestURI,
			c.route.Name,
			time.Since(start),
		)
	}
}

func wrapPanic(inner ContextHandlerFunc) ContextHandlerFunc {
	return func(c *Context, w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("panic caught: %+v", err)
				http.Error(w, http.StatusText(500), 500)
			}
		}()
		inner(c, w, r)
	}
}

func wrapAsHandler(inner ContextHandlerFunc, c *Context) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		inner(c, w, r)
	}
}

func newRouter(db *Database) *mux.Router {

	router := mux.NewRouter().StrictSlash(true)
	for _, route := range routes {
		context := &Context{
			route,
			db,
		}
		router.
			Methods(route.Method).
			Path(route.Pattern).
			Name(route.Name).
			HandlerFunc(wrapAsHandler(wrapLogger(wrapPanic(route.HandlerFunc)), context))
	}
	return router
}

func createLogItem(c *Context, w http.ResponseWriter, r *http.Request) {
	var logItem LogItem
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1*1024*1024))
	if err != nil {
		panic(err)
	}
	if err := r.Body.Close(); err != nil {
		panic(err)
	}
	if err := json.Unmarshal(body, &logItem); err != nil {
		http.Error(w, "Cannot parse JSON", 422)
		return
	}
	logItem.OriginatorIp = ""
	logItem.OriginatorPort = 0
	if ip, po, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		logItem.OriginatorIp = ip
		if p, err := strconv.Atoi(po); err == nil {
			logItem.OriginatorPort = p
		}
	}

	if tls := r.TLS; tls != nil {
		certs := tls.PeerCertificates
		if len(certs) > 0 {
			logItem.ClientName = certs[0].Subject.CommonName
		}
	}

	logItem.normalise()
	logItem.makeHashAndInsert(c.db)

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(logItem); err != nil {
		panic(err)
	}
}

func queryLogItem(c *Context, w http.ResponseWriter, r *http.Request) {
	if err := r.Body.Close(); err != nil {
		panic(err)
	}

	var query interface{}
	qstring := r.URL.Query().Get("query")
	if len(qstring) != 0 {
		if err := json.Unmarshal([]byte(qstring), &query); err != nil {
			http.Error(w, err.Error(), 422)
			return
		}
		if err := jsonToDbKeys(&query); err != nil {
			http.Error(w, err.Error(), 422)
			return
		}
	}

	limit := 0
	lstring := r.URL.Query().Get("limit")
	if len(lstring) != 0 {
		var err error
		limit, err = strconv.Atoi(lstring)
		if err != nil {
			http.Error(w, "Cannot parse limit", 422)
			return
		}
	}

	var sortOrder []string
	sostring := r.URL.Query().Get("sort")
	if len(sostring) != 0 {
		for _, v := range strings.Split(sostring, ",") {
			n := strings.ToLower(v)
			desc := false
			if strings.HasPrefix(n, "-") {
				desc = true
				n = strings.TrimPrefix(n, "-")
			} else if strings.HasPrefix(n, "+") {
				n = strings.TrimPrefix(n, "+")
			}
			if j, ok := jsonMap[n]; ok && !hasFieldProperty(j, fpNoQuery) {
				if desc {
					sortOrder = append(sortOrder, fmt.Sprintf("-%s", j))
				} else {
					sortOrder = append(sortOrder, j)
				}
			} else {
				http.Error(w, "Cannot parse sort", 422)
				return
			}
		}
	}

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusFound)
	w.Write([]byte("{\"results\":[\n"))
	encoder := json.NewEncoder(w)
	ch := make(chan LogItem, 10)
	var wait sync.WaitGroup
	wait.Add(1)
	go func() {
		defer wait.Done()
		first := true
		for {
			select {
			case l, ok := (<-ch):
				if !ok {
					return
				}
				if !first {
					w.Write([]byte(",\n"))
				}
				first = false
				if err := encoder.Encode(l); err != nil {
					panic(err)
				}
			}
		}
	}()
	count, complete := queryLogItems(c.db, query, sortOrder, limit, ch)
	close(ch)
	wait.Wait()
	w.Write([]byte(fmt.Sprintf("],\"complete\":%t,\"count\":%d}\n", complete, count)))
}

func httpServerStart(db *Database, listen string) {
	router := newRouter(db)
	log.Fatal(http.ListenAndServe(listen, router))
}

func httpsServerStart(db *Database, listen string, tlsConfig *tls.Config) {
	// This is somewhat hacky - see tlshackery.go for why
	router := newRouter(db)
	server := &http.Server{
		Addr:      listen,
		TLSConfig: tlsConfig,
		Handler:   router,
	}
	log.Fatal(ListenAndServeTLSNoCerts(server))
}
