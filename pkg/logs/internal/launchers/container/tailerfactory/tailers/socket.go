// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

//go:build docker
// +build docker

package tailers

import (
	"context"
	"time"

	"github.com/DataDog/datadog-agent/pkg/logs/auditor"
	dockerLaunchersPkg "github.com/DataDog/datadog-agent/pkg/logs/internal/launchers/docker"
	dockerTailerPkg "github.com/DataDog/datadog-agent/pkg/logs/internal/tailers/docker"
	"github.com/DataDog/datadog-agent/pkg/logs/message"
	"github.com/DataDog/datadog-agent/pkg/logs/sources"
	dockerutilPkg "github.com/DataDog/datadog-agent/pkg/util/docker"
	"github.com/DataDog/datadog-agent/pkg/util/log"
)

var (
	backoffInitialDuration = 1 * time.Second
	backoffMaxDuration     = 60 * time.Second
)

// DockerSocketTailer wraps pkg/logs/internal/tailers/docker.Tailer to satisfy
// the container launcher's `Tailer` interface, and to handle the
// erroredContainerID channel.
//
// NOTE: once the docker launcher is removed, the inner Docker tailer can be
// modified to suit the Tailer interface directly and to handle connection
// failures on its own, and this wrapper will no longer be necessary.
type DockerSocketTailer struct {
	// arguments to dockerTailerPkg.NewTailer (except erroredContainerID)

	dockerutil  *dockerutilPkg.DockerUtil
	ContainerID string
	source      *sources.LogSource
	pipeline    chan *message.Message
	readTimeout time.Duration

	// registry is used to calculate `since`
	registry auditor.Registry

	// ctx controls the run loop
	ctx context.Context

	// cancel stops the run loop
	cancel context.CancelFunc

	// stopped is closed when the run loop finishes
	stopped chan struct{}
}

func NewDockerSocketTailer(dockerutil *dockerutilPkg.DockerUtil, containerID string, source *sources.LogSource, pipeline chan *message.Message, readTimeout time.Duration, registry auditor.Registry) *DockerSocketTailer {
	return &DockerSocketTailer{
		dockerutil:  dockerutil,
		ContainerID: containerID,
		source:      source,
		pipeline:    pipeline,
		readTimeout: readTimeout,
		registry:    registry,
		ctx:         nil,
		cancel:      nil,
		stopped:     nil,
	}
}

// tryStartTailer tries to start the inner tailer, returning an erroredContainerID channel if
// successful.
func (t *DockerSocketTailer) tryStartTailer() (*dockerTailerPkg.Tailer, chan string, error) {
	erroredContainerID := make(chan string)
	inner := dockerTailerPkg.NewTailer(
		t.dockerutil,
		t.ContainerID,
		t.source,
		t.pipeline,
		erroredContainerID,
		t.readTimeout)
	since, err := dockerLaunchersPkg.Since(t.registry, inner.Identifier())
	if err != nil {
		log.Warnf("Could not recover tailing from last committed offset %v: %v",
			dockerutilPkg.ShortContainerID(t.ContainerID), err)
		// (the `since` value is still valid)
	}

	err = inner.Start(since)
	if err != nil {
		return nil, nil, err
	}
	return inner, erroredContainerID, nil
}

// stopTailer stops the inner tailer.
func (t *DockerSocketTailer) stopTailer(inner *dockerTailerPkg.Tailer) {
	inner.Stop()
}

// Start implements Tailer#Start.
func (t *DockerSocketTailer) Start() error {
	t.ctx, t.cancel = context.WithCancel(context.Background())
	t.stopped = make(chan struct{})
	go t.run(t.tryStartTailer, t.stopTailer)
	return nil
}

// Stop implements Tailer#Stop.
func (t *DockerSocketTailer) Stop() {
	t.cancel()
	t.cancel = nil
	<-t.stopped
}

// run implements a loop to monitor the tailer and re-create it if it fails.  It takes
// pointers to tryStartTailer and stopTailer to support testing.
func (t *DockerSocketTailer) run(
	tryStartTailer func() (*dockerTailerPkg.Tailer, chan string, error),
	stopTailer func(*dockerTailerPkg.Tailer),
) {
	defer close(t.stopped)

	backoffDuration := backoffInitialDuration

	for {
		var backoffTimerC <-chan time.Time

		// try to start the inner tailer
		inner, erroredContainerID, err := tryStartTailer()
		if err != nil {
			if backoffDuration > backoffMaxDuration {
				log.Warnf("Could not tail container %v: %v",
					dockerutilPkg.ShortContainerID(t.ContainerID), err)
				return
			}
			// set up to wait before trying again
			backoffTimerC = time.After(backoffDuration)
			backoffDuration *= 2
		} else {
			// success, so reset backoff
			backoffTimerC = nil
			backoffDuration = backoffInitialDuration
		}

		select {
		case <-t.ctx.Done():
			// the launcher has requested that the tailer stop
			if inner != nil {
				stopTailer(inner)
			}
			return

		case <-erroredContainerID:
			// the inner tailer has failed after it has started
			if inner != nil {
				stopTailer(inner)
			}
			continue // retry

		case <-backoffTimerC:
			// it's time to retry starting the tailer
			continue
		}
	}
}
