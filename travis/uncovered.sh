#!/bin/bash

# List all Go packages which won't be instrumented by a full coverage run.

# Determine which packages were not instrumented at all.
instrumented_packages_file=$(mktemp --suffix .uncovered_sh_instrumented_packages)
for pkg in $(go list -f "{{if len .TestGoFiles}}{{.ImportPath}}{{end}}" ./go/...); do
  for dep in $(deplist -include_input_pkg -p github.com/youtube/vitess -t $pkg); do
    echo $dep >> $instrumented_packages_file
  done
done

comm -13 <(sort -u $instrumented_packages_file) <(go list ./go/... | sort)

[ -f "$instrumented_packages_file" ] && rm "$instrumented_packages_file"
