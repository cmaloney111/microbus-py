// Capture-only: emit golden bytes for codec parity tests.
// Usage: go run ./cmd/golden <out-dir>
package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
)

type spec struct {
	Name    string     `json:"-"`
	Method  string     `json:"method"`
	URL     string     `json:"url"`
	Headers [][]string `json:"headers,omitempty"`
	BodyHex string     `json:"bodyHex,omitempty"`
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: go run ./cmd/golden <out-dir>")
		os.Exit(2)
	}
	out := os.Args[1]
	if err := os.MkdirAll(out, 0o755); err != nil {
		panic(err)
	}
	specs := []spec{
		{
			Name:   "simple_get",
			Method: "GET",
			URL:    "https://echo.example:443/echo",
			Headers: [][]string{
				{"User-Agent", "microbus/1.27.1"},
				{"Microbus-Msg-Id", "abc123"},
				{"Microbus-From-Host", "tester.example"},
				{"Microbus-From-Id", "deadbeef"},
				{"Microbus-Op-Code", "Req"},
			},
		},
		{
			Name:   "post_with_body",
			Method: "POST",
			URL:    "https://echo.example:443/echo",
			Headers: [][]string{
				{"User-Agent", "microbus/1.27.1"},
				{"Microbus-Msg-Id", "abc123"},
				{"Content-Type", "application/octet-stream"},
			},
			BodyHex: hex.EncodeToString(bytes.Repeat([]byte("a"), 32)),
		},
	}
	for _, s := range specs {
		req, _ := http.NewRequest(s.Method, s.URL, nil)
		for _, h := range s.Headers {
			req.Header.Add(h[0], h[1])
		}
		if s.BodyHex != "" {
			b, _ := hex.DecodeString(s.BodyHex)
			req.Body = nopCloser{bytes.NewReader(b)}
			req.ContentLength = int64(len(b))
		}
		var buf bytes.Buffer
		bw := bufio.NewWriter(&buf)
		if err := req.WriteProxy(bw); err != nil {
			panic(err)
		}
		bw.Flush()
		if err := os.WriteFile(filepath.Join(out, s.Name+".bin"), buf.Bytes(), 0o644); err != nil {
			panic(err)
		}
		js, _ := json.MarshalIndent(s, "", "  ")
		if err := os.WriteFile(filepath.Join(out, s.Name+".json"), js, 0o644); err != nil {
			panic(err)
		}
	}
	fmt.Printf("wrote %d golden pairs to %s\n", len(specs), out)
}

type nopCloser struct{ *bytes.Reader }

func (nopCloser) Close() error { return nil }
