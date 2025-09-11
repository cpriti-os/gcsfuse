// Copyright 2015 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gcsx

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/googlecloudplatform/gcsfuse/v3/internal/storage/gcs"
	"github.com/googlecloudplatform/gcsfuse/v3/internal/storage/storageutil"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"github.com/googlecloudplatform/gcsfuse/v3/internal/logger"
)

func garbageCollectOnce(
	ctx context.Context,
	tmpObjectPrefix string,
	bucket gcs.Bucket) (objectsDeleted uint64, err error) {
	const stalenessThreshold = 30 * time.Minute
	group, ctx := errgroup.WithContext(ctx)

	// List all objects with the temporary prefix.
	minObjects := make(chan *gcs.MinObject, 100)
	group.Go(func() (err error) {
		defer close(minObjects)
		err = storageutil.ListPrefix(ctx, bucket, tmpObjectPrefix, minObjects)
		if err != nil {
			err = fmt.Errorf("ListPrefix: %w", err)
			return
		}

		return
	})

	// Filter to the names of objects that are stale.
	now := time.Now()
	staleNames := make(chan string, 100)
	group.Go(func() (err error) {
		defer close(staleNames)
		for o := range minObjects {
			if now.Sub(o.Updated) < stalenessThreshold {
				continue
			}

			select {
			case <-ctx.Done():
				err = ctx.Err()
				return

			case staleNames <- o.Name:
			}
		}

		return
	})

	// Delete those objects.
	group.Go(func() (err error) {
		for name := range staleNames {
			err = bucket.DeleteObject(
				ctx,
				&gcs.DeleteObjectRequest{
					Name:       name,
					Generation: 0, // Latest generation of stale object.
				})

			if err != nil {
				err = fmt.Errorf("DeleteObject(%q): %w", name, err)
				return
			}

			atomic.AddUint64(&objectsDeleted, 1)
		}

		return
	})

	err = group.Wait()
	return
}

// Periodically delete stale temporary objects from the supplied bucket until
// the context is cancelled.
func garbageCollect(
	ctx context.Context,
	tmpObjectPrefix string,
	bucket gcs.Bucket) {
	const period = 10 * time.Minute
	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
		}

		logger.Info("Starting a garbage collection run.")

		startTime := time.Now()
		objectsDeleted, err := garbageCollectOnce(ctx, tmpObjectPrefix, bucket)

		if err != nil {
			logger.Infof(
				"Garbage collection failed after deleting %d objects in %v, "+
					"with error: %v",
				objectsDeleted,
				time.Since(startTime),
				err)
		} else {
			logger.Infof(
				"Garbage collection succeeded after deleted %d objects in %v.",
				objectsDeleted,
				time.Since(startTime))
		}
	}
}

// TCPStats holds the counts for active and idle TCP connections.
type TCPStats struct {
	Active int
	Idle   int
}

// startTCPMonitoring contains all the monitoring logic in a single function.
// It sets up a ticker and, on each tick, reads and parses connection stats.
func startTCPMonitoring(ctx context.Context) {
	logger.Info("TCP Monitoring: goroutine is now running...")
	const period = 10 * time.Second
	ticker := time.NewTicker(period)
	defer ticker.Stop()

	// The infinite loop runs within the single goroutine.
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		// The logic for getting stats is now directly inside the loop.
		// We reset the stats counter for each check.
		stats := TCPStats{}
		data, err := os.ReadFile("/proc/net/tcp")
		if err != nil {
			logger.Errorf("TCP Monitoring: Error reading /proc/net/tcp: %v", err)
			continue // Skip this tick on error
		}

		lines := strings.Split(string(data), "\n")

		// Iterate over each line, skipping the header.
		for _, line := range lines[1:] {
			fields := strings.Fields(line)
			if len(fields) < 4 {
				continue
			}

			// The connection's state is the 4th field (index 3).
			// "01" means ESTABLISHED.
			state := fields[3]
			if state == "01" {
				stats.Active++
			} else {
				stats.Idle++
			}
		}

		// Print the final counts for this interval.
		logger.Infof("TCP Monitoring: Active TCP Connections: %d, Idle TCP Connections: %d", stats.Active, stats.Idle)
	}
}
