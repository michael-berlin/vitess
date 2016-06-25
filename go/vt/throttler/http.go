package throttler

import "net/http"

const throttlersHTML = `
<html>
<head>
<title>Throttlers Status</title>
</head>
<body>

for each throttler:

<div>
current max rate: BLA
graph: (requires name)
</div>

{{if .Status}}
  <h2>Worker status:</h2>
  <blockquote>
    {{.Status}}
  </blockquote>
  <h2>Worker logs:</h2>
  <blockquote>
    {{.Logs}}
  </blockquote>
  {{if .Done}}
  <p><a href="/reset">Reset Job</a></p>
  {{else}}
  <p><a href="/cancel">Cancel Job</a></p>
  {{end}}
{{else}}
  <p>This worker is idle.</p>
  <p><a href="/">Toplevel Menu</a></p>
{{end}}
</body>
</html>
`

// ServeHTTP returns a status page with graphs for each registered throttler.
func (m *managerImpl) ServeHTTP(w http.ResponseWriter, r *http.Request) {

}
