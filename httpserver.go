package main

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"io"
	"io/ioutil"
	"log"
	"net/http"
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
	logItem.normalise()
	c.db.insertLogItem(logItem)

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(logItem); err != nil {
		panic(err)
	}
}

func httpServerStart(db *Database) {
	router := newRouter(db)
	log.Fatal(http.ListenAndServe("127.0.0.1:8080", router))
}
