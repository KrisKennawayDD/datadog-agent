//  Unless explicitly stated otherwise all files in this repository are licensed
//  under the Apache License Version 2.0.
//  This product includes software developed at Datadog (https://www.datadoghq.com/).
//  Copyright 2016-present Datadog, Inc.

package ebpftest

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

const tracePipeFile = "/sys/kernel/debug/tracing/trace_pipe"

// TracePipe to read from /sys/kernel/debug/tracing/trace_pipe
// Note that data can be read only once, i.e. if you have more than
// one tracer / channel, only one will receive an event:
// "Once data is read from this file, it is consumed, and will not be
// read again with a sequential read."
// https://www.kernel.org/doc/Documentation/trace/ftrace.txt
type TracePipe struct {
	file   *os.File
	reader *bufio.Reader
	stop   chan struct{}
}

// TraceEvent contains the raw event as well as the contents of
// every field as string, as defined under "Output format" in
// https://www.kernel.org/doc/Documentation/trace/ftrace.txt
type TraceEvent struct {
	Raw       string
	Task      string
	PID       string
	CPU       string
	Flags     string
	Timestamp string
	Function  string
	Message   string
}

// NewTracePipe instantiates a new trace pipe
func NewTracePipe() (*TracePipe, error) {
	f, err := os.Open(tracePipeFile)
	if err != nil {
		return nil, err
	}
	return &TracePipe{
		file:   f,
		reader: bufio.NewReader(f),
		stop:   make(chan struct{}),
	}, nil
}

// A line from trace_pipe looks like (leading spaces included):
// `        chromium-15581 [000] d... 92783.722567: : Hello, World!`
var traceLineRegexp = regexp.MustCompile(`(.{16})-(\d+) +\[(\d{3})\] (.{4}) +(\d+\.\d+)\: (.*?)\: (.*)`)

func parseTraceLine(raw string) (*TraceEvent, error) {
	fields := traceLineRegexp.FindStringSubmatch(raw)
	if len(fields) != 8 {
		return nil, fmt.Errorf("received unexpected input %q", raw)
	}
	return &TraceEvent{
		Raw:       raw,
		Task:      strings.Trim(fields[1], " "),
		PID:       fields[2],
		CPU:       fields[3],
		Flags:     fields[4],
		Timestamp: fields[5],
		Function:  fields[6],
		Message:   fields[7],
	}, nil
}

// ReadLine reads a line
func (t *TracePipe) ReadLine() (*TraceEvent, error) {
	line, err := t.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	traceEvent, err := parseTraceLine(strings.TrimSuffix(line, "\n"))
	if err != nil {
		return nil, err
	}
	return traceEvent, nil
}

// Channel returns a channel of events and an other for errors
func (t *TracePipe) Channel() (<-chan *TraceEvent, <-chan error) {
	channelEvents := make(chan *TraceEvent)
	channelErrors := make(chan error)
	go func() {
		for {
			select {
			case <-t.stop:
				return
			default:
			}
			traceEvent, err := t.ReadLine()
			if err != nil {
				if err == io.EOF {
					continue
				}
				channelErrors <- err
			} else {
				channelEvents <- traceEvent
			}
		}
	}()
	return channelEvents, channelErrors
}

// Close the trace pipe
func (t *TracePipe) Close() error {
	close(t.stop)
	return t.file.Close()
}