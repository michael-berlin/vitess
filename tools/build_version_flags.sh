#!/bin/bash

# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

echo "\
-X 'github.com/youtube/vitess/go/vt/servenv.buildHost=$(hostname)'\
-X 'github.com/youtube/vitess/go/vt/servenv.buildUser=$(whoami)'\
-X 'github.com/youtube/vitess/go/vt/servenv.buildGitRev=$(git rev-parse HEAD)'\
-X 'github.com/youtube/vitess/go/vt/servenv.buildTime=$(LC_ALL=C date)'\
"
