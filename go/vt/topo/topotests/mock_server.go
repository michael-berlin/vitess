package topotests

import (
	"github.com/golang/mock/gomock"
	"github.com/youtube/vitess/go/vt/topo"
)

func NewMockServer(ctrl *gomock.Controller) (topo.Server, *MockImpl) {
	mockImpl := NewMockImpl(ctrl)
	return topo.Server{Impl: mockImpl}, mockImpl
}
