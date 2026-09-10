package webterm

import _ "embed"

//go:embed index.html
var indexHTML string

//go:embed app.js
var appJS string

//go:embed app.css
var appCSS string

//go:embed ICONS-LICENSE
var iconsLicense string
