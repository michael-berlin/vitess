// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpctmserver

import (
	"net"
	"testing"

	"github.com/youtube/vitess/go/vt/tabletmanager/agentrpctest"
	"github.com/youtube/vitess/go/vt/tabletmanager/grpctmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"google.golang.org/grpc"

	pbs "github.com/youtube/vitess/go/vt/proto/tabletmanagerservice"
	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// the test here creates a fake server implementation, a fake client
// implementation, and runs the test suite against the setup.
func TestGoRPCTMServer(t *testing.T) {
	// Listen on a random port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	host := listener.Addr().(*net.TCPAddr).IP.String()
	port := int32(listener.Addr().(*net.TCPAddr).Port)

	// Create a gRPC server and listen on the port
	s := grpc.NewServer()
	fakeAgent := agentrpctest.NewFakeRPCAgent(t)
	pbs.RegisterTabletManagerServer(s, &server{agent: fakeAgent})
	go s.Serve(listener)

	// Create a gRPG client to talk to the fake tablet
	client := &grpctmclient.Client{}
	ti := topo.NewTabletInfo(&pb.Tablet{
		Alias: &pb.TabletAlias{
			Cell: "test",
			Uid:  123,
		},
		Hostname: host,
		PortMap: map[string]int32{
			"grpc": port,
		},
	}, 0)

	// and run the test suite
	agentrpctest.Run(t, client, ti, fakeAgent)
}
