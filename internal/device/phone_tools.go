package device

import (
	"context"
	"time"

	E "github.com/sagernet/sing/common/exceptions"
)

type locationClient interface {
	Set(context.Context, float64, float64) error
	Clear(context.Context) error
	Close() error
	Err() error
	Done() <-chan struct{}
}

func (c *conn) closeLocation() {
	if c.location == nil {
		return
	}
	if err := c.location.Close(); err != nil {
		c.logger.Debug(E.Cause(err, "close the location simulation"))
	}
	c.location = nil
}

func (c *conn) setLocation(ctx context.Context, latitude, longitude float64) error {
	c.locationAccess.Lock()
	defer c.locationAccess.Unlock()
	if c.location != nil && c.location.Err() != nil {
		c.closeLocation()
	}
	if c.location == nil {
		location, err := c.locationDial(ctx)
		if err != nil {
			return err
		}
		c.location = location
	}
	if err := c.location.Set(ctx, latitude, longitude); err != nil {
		c.closeLocation()
		return err
	}
	return nil
}

func (p *Phone) SetLocation(ctx context.Context, latitude, longitude float64) error {
	c, ctx, done, err := p.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	p.locationAccess.Lock()
	defer p.locationAccess.Unlock()
	if err = c.setLocation(ctx, latitude, longitude); err != nil {
		p.attached(c)
		return err
	}
	p.keepLocation(latitude, longitude)
	p.attached(c)
	return nil
}

func (p *Phone) RestoreLocation(latitude, longitude float64) {
	p.locationAccess.Lock()
	p.keepLocation(latitude, longitude)
	p.locationAccess.Unlock()
	if c := p.connection.currentConn(); c != nil {
		p.attached(c)
	}
}

func (p *Phone) keepLocation(latitude, longitude float64) {
	if p.location == nil {
		p.location = &simulatedLocation{release: p.connection.hold()}
	}
	p.location.latitude, p.location.longitude = latitude, longitude
}

func (p *Phone) ClearLocation(ctx context.Context) error {
	p.locationAccess.Lock()
	defer p.locationAccess.Unlock()
	if p.location != nil {
		defer p.location.release()
		p.location = nil
	}
	// DTLocationSimulationService stops the simulation when its channel disconnects
	// (DVTInstrumentsFoundation 24B5110a, -[DTLocationSimulationService initWithChannel:]).
	c := p.connection.currentConn()
	if c == nil {
		return nil
	}
	ctx, cancel := c.operation(ctx)
	defer cancel()
	c.locationAccess.Lock()
	defer c.locationAccess.Unlock()
	if c.location == nil || c.location.Err() != nil {
		c.closeLocation()
		return nil
	}
	err := c.location.Clear(ctx)
	c.closeLocation()
	return err
}

func (p *Phone) attached(c *conn) {
	c.locationMonitor.Do(func() { go p.maintainLocation(c) })
	select {
	case c.locationWake <- struct{}{}:
	default:
	}
}

func (c *conn) locationState() (<-chan struct{}, bool) {
	c.locationAccess.Lock()
	defer c.locationAccess.Unlock()
	if c.location == nil {
		return nil, false
	}
	return c.location.Done(), c.location.Err() == nil
}

// The location DTX channel can disconnect while the device connection remains
// usable. Monitor that channel as well as device attachments, and retry only
// while a simulated location is still requested.
func (p *Phone) maintainLocation(c *conn) {
	failures := 0
	for c.ctx.Err() == nil {
		select {
		case <-c.locationWake:
		default:
		}
		var done <-chan struct{}
		var err error
		p.locationAccess.Lock()
		if p.location != nil {
			var live bool
			done, live = c.locationState()
			if !live {
				ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
				err = c.setLocation(ctx, p.location.latitude, p.location.longitude)
				cancel()
				if err == nil {
					done, _ = c.locationState()
				} else {
					done = nil
				}
			}
		}
		p.locationAccess.Unlock()
		if c.ctx.Err() != nil {
			return
		}
		var retry <-chan time.Time
		var timer *time.Timer
		if err != nil {
			p.logger.Warn(E.Cause(err, "simulate the location again"))
			timer = time.NewTimer(retryDelays[min(failures, len(retryDelays)-1)])
			retry = timer.C
			failures++
		} else {
			failures = 0
		}
		select {
		case <-c.ctx.Done():
		case <-c.locationWake:
		case <-done:
		case <-retry:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}
