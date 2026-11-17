package device

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sagernet/sing/common/logger"
)

type testLocationClient struct {
	done   chan struct{}
	close  sync.Once
	access sync.Mutex
	points [][2]float64
	clears int
}

func (l *testLocationClient) Set(ctx context.Context, latitude, longitude float64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := l.Err(); err != nil {
		return err
	}
	l.access.Lock()
	l.points = append(l.points, [2]float64{latitude, longitude})
	l.access.Unlock()
	return nil
}

func (l *testLocationClient) Clear(context.Context) error {
	l.access.Lock()
	l.clears++
	l.access.Unlock()
	return nil
}

func (l *testLocationClient) Close() error {
	l.close.Do(func() { close(l.done) })
	return nil
}

func (l *testLocationClient) Done() <-chan struct{} { return l.done }

func (l *testLocationClient) Err() error {
	select {
	case <-l.done:
		return net.ErrClosed
	default:
		return nil
	}
}

func locationTestPhone(t *testing.T) (*Phone, *conn, context.CancelFunc) {
	ctx, cancel := context.WithCancel(t.Context())
	c := &conn{ctx: ctx, owner: ctx, logger: logger.NOP(), locationWake: make(chan struct{}, 1)}
	p := &Phone{
		connection: &Connection{attached: c}, logger: logger.NOP(),
		location: &simulatedLocation{latitude: 41.42, longitude: -107.05, release: func() {}},
	}
	return p, c, cancel
}

func TestLocationReconnectsWithoutDeviceDetach(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, c, cancel := locationTestPhone(t)
		defer cancel()
		var clients []*testLocationClient
		c.locationDial = func(context.Context) (locationClient, error) {
			l := &testLocationClient{done: make(chan struct{})}
			clients = append(clients, l)
			return l, nil
		}
		for range 5 {
			p.attached(c)
		}
		synctest.Wait()
		if len(clients) != 1 {
			t.Fatalf("repeated attachment started %d sessions, want 1", len(clients))
		}
		_ = clients[0].Close()
		synctest.Wait()
		if len(clients) != 2 || c.ctx.Err() != nil {
			t.Fatalf("location channel was not restored on the same device connection: sessions=%d, error=%v", len(clients), c.ctx.Err())
		}
		if got := clients[1].points; len(got) != 1 || got[0] != [2]float64{41.42, -107.05} {
			t.Fatalf("restored coordinates = %v", got)
		}
		if err := p.ClearLocation(context.Background()); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if len(clients) != 2 || clients[1].clears != 1 {
			t.Fatalf("stopping the simulation reopened it: sessions=%d, clears=%d", len(clients), clients[1].clears)
		}
	})
}

func TestLocationRetriesTransientRestoreFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, c, cancel := locationTestPhone(t)
		defer cancel()
		var attempts atomic.Int32
		client := &testLocationClient{done: make(chan struct{})}
		c.locationDial = func(context.Context) (locationClient, error) {
			if attempts.Add(1) == 1 {
				return nil, errors.New("temporary service failure")
			}
			return client, nil
		}
		p.attached(c)
		synctest.Wait()
		if attempts.Load() != 1 {
			t.Fatalf("restore attempts before backoff = %d, want 1", attempts.Load())
		}
		time.Sleep(retryDelays[0])
		synctest.Wait()
		if attempts.Load() != 2 || len(client.points) != 1 {
			t.Fatalf("transient failure was not retried: attempts=%d, sets=%d", attempts.Load(), len(client.points))
		}
	})
}

func TestClearingLocationCancelsPendingRestore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, c, cancel := locationTestPhone(t)
		defer cancel()
		var attempts atomic.Int32
		c.locationDial = func(context.Context) (locationClient, error) {
			attempts.Add(1)
			return nil, errors.New("service unavailable")
		}
		p.attached(c)
		synctest.Wait()
		if err := p.ClearLocation(context.Background()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(30 * time.Second)
		synctest.Wait()
		if attempts.Load() != 1 {
			t.Fatalf("stopped location was retried %d times", attempts.Load())
		}
	})
}
