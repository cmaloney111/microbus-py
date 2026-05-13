package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/microbus-io/errors"
	"github.com/microbus-io/fabric/application"
	"github.com/microbus-io/fabric/connector"
	"github.com/microbus-io/fabric/coreservices/accesstoken"
	"github.com/microbus-io/fabric/coreservices/configurator"
	"github.com/microbus-io/fabric/coreservices/foreman"
	"github.com/microbus-io/fabric/coreservices/httpingress"
	"github.com/microbus-io/fabric/coreservices/llm"
	"github.com/microbus-io/fabric/coreservices/openapiportal"
	"github.com/microbus-io/fabric/pub"
	"github.com/microbus-io/fabric/sub"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	app := application.New()
	app.Add(configurator.NewService())
	app.Add(accesstoken.NewService())
	app.Add(foreman.NewService())
	app.Add(llm.NewService())
	app.Add(openapiportal.NewService())
	app.Add(healthService())
	app.Add(echoService())
	app.Add(callerService())
	app.Add(securedService())
	app.Add(workflowService())
	app.Add(cachePeerService())
	app.Add(httpingress.NewService())
	if err := app.Run(); err != nil {
		panic(err)
	}
}

func healthService() *connector.Connector {
	c := connector.New("health.example")
	must(c.Subscribe("Health", func(w http.ResponseWriter, r *http.Request) error {
		w.WriteHeader(200)
		w.Write([]byte("ok"))
		return nil
	}, sub.At("GET", "/health"), sub.Web()))
	return c
}

func echoService() *connector.Connector {
	c := connector.New("echo.example")
	must(c.Subscribe("Echo", func(w http.ResponseWriter, r *http.Request) error {
		body, _ := io.ReadAll(r.Body)
		if pad := r.Header.Get("Microbus-Test-Pad"); pad != "" {
			n, _ := strconv.Atoi(pad)
			if n > 0 {
				body = append(body, make([]byte, n)...)
			}
		}
		for k, vv := range r.Header {
			if len(k) > len("Microbus-Baggage-") && k[:len("Microbus-Baggage-")] == "Microbus-Baggage-" {
				for _, v := range vv {
					w.Header().Add(k, v)
				}
			}
		}
		for _, v := range r.Header.Values("Traceparent") {
			w.Header().Add("Traceparent", v)
		}
		for _, v := range r.Header.Values("Tracestate") {
			w.Header().Add("Tracestate", v)
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(body)
		return nil
	}, sub.At("POST", "/echo"), sub.Web()))
	must(c.Subscribe("SlowEcho", func(w http.ResponseWriter, r *http.Request) error {
		body, _ := io.ReadAll(r.Body)
		time.Sleep(750 * time.Millisecond)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(body)
		return nil
	}, sub.At("POST", "/slow-echo"), sub.Web()))
	must(c.Subscribe("ForceError", func(w http.ResponseWriter, r *http.Request) error {
		http.Error(w, `{"err":{"error":"sidecar forced error","statusCode":500,"trace":"","stack":[{"func":"echo.force_error","file":"main.go","line":1}]}}`, 500)
		return nil
	}, sub.At("GET", "/force-error"), sub.Web()))
	return c
}

func callerService() *connector.Connector {
	c := connector.New("caller.example")
	callPython := func(w http.ResponseWriter, r *http.Request, target string, actor map[string]any) {
		body, _ := io.ReadAll(r.Body)
		opts := []pub.Option{
			pub.POST(target),
			pub.Body(body),
		}
		if actor != nil {
			opts = append(opts, pub.Actor(actor))
		}
		res, err := c.Request(r.Context(), opts...)
		if err != nil {
			http.Error(w, err.Error(), errors.StatusCode(err))
			return
		}
		if res.Body != nil {
			defer res.Body.Close()
		}
		out, _ := io.ReadAll(res.Body)
		for k, vv := range res.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(res.StatusCode)
		w.Write(out)
	}
	must(c.Subscribe("CallPython", func(w http.ResponseWriter, r *http.Request) error {
		callPython(w, r, "https://py-target.example:443/reverse", nil)
		return nil
	}, sub.At("POST", "/call-python"), sub.Web()))
	must(c.Subscribe("CallPythonViewer", func(w http.ResponseWriter, r *http.Request) error {
		callPython(w, r, "https://py-target.example:443/reverse", map[string]any{"sub": "u", "roles": map[string]any{"viewer": true}})
		return nil
	}, sub.At("POST", "/call-python-viewer"), sub.Web()))
	must(c.Subscribe("CallPythonAdmin", func(w http.ResponseWriter, r *http.Request) error {
		callPython(w, r, "https://py-target.example:443/reverse", map[string]any{"sub": "u", "roles": map[string]any{"admin": true}})
		return nil
	}, sub.At("POST", "/call-python-admin"), sub.Web()))
	must(c.Subscribe("CallPythonBaggage", func(w http.ResponseWriter, r *http.Request) error {
		callPython(w, r, "https://py-bag-target.example:443/relay", nil)
		return nil
	}, sub.At("POST", "/call-python-baggage"), sub.Web()))
	return c
}

func securedService() *connector.Connector {
	c := connector.New("secured.example")
	must(c.Subscribe("Admin", func(w http.ResponseWriter, r *http.Request) error {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`))
		return nil
	}, sub.At("POST", "/admin"), sub.Web(), sub.RequiredClaims(`role=="admin"`)))
	return c
}

func workflowService() *connector.Connector {
	c := connector.New("workflow.example")
	must(c.Subscribe("Double", func(w http.ResponseWriter, r *http.Request) error {
		var doc map[string]any
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &doc); err != nil {
			http.Error(w, err.Error(), 400)
			return nil
		}
		state, _ := doc["state"].(map[string]any)
		if state == nil {
			state = map[string]any{}
		}
		x, _ := state["x"].(float64)
		state["x"] = x * 2
		doc["state"] = state
		out, _ := json.Marshal(doc)
		w.Header().Set("Content-Type", "application/json")
		w.Write(out)
		return nil
	}, sub.At("POST", ":428/tasks/double"), sub.Web()))
	must(c.Subscribe("DoubleTwice", func(w http.ResponseWriter, r *http.Request) error {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"graph":{"name":"workflow.example/double-twice","entryPoint":"https://workflow.example:428/tasks/double","tasks":[{"name":"https://workflow.example:428/tasks/double"}],"transitions":[{"from":"https://workflow.example:428/tasks/double","to":"END"}],"inputs":["x"],"outputs":["x"]}}`))
		return nil
	}, sub.At("GET", ":428/graphs/double-twice"), sub.Web()))
	return c
}

func cachePeerService() *connector.Connector {
	c := connector.New("cache-peer.example")
	must(c.Subscribe("CacheStore", func(w http.ResponseWriter, r *http.Request) error {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return nil
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return errors.Trace(err)
		}
		err = c.DistribCache().Store(r.Context(), key, body)
		if err != nil {
			http.Error(w, err.Error(), errors.StatusCode(err))
			return nil
		}
		w.WriteHeader(http.StatusNoContent)
		return nil
	}, sub.At("PUT", "/store"), sub.Web()))
	must(c.Subscribe("CacheLoad", func(w http.ResponseWriter, r *http.Request) error {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return nil
		}
		value, ok, err := c.DistribCache().Load(r.Context(), key)
		if err != nil {
			http.Error(w, err.Error(), errors.StatusCode(err))
			return nil
		}
		if !ok {
			http.NotFound(w, r)
			return nil
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(value)
		return nil
	}, sub.At("GET", "/load"), sub.Web()))
	return c
}
