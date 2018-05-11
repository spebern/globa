package globa

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// ErrTimeout will be returned if host blocks longer than the timeout
// set in the load balancer.
var ErrTimeout = errors.New("timeout")

// ErrNoRemainingHosts will be returned if there are no more remaining hosts.
// Old removed hosts can be restored with the "Recover" method.
var ErrNoRemainingHosts = errors.New("no remaining hosts")

// ErrNotExistingHost will be returned if someone tries to increase load on not
// existing host
var ErrNotExistingHost = errors.New("host doesn't exist anymore")

type host struct {
	*sync.Mutex
	avgResponseTime float64
	pendingRequests int64
	token           chan struct{}
}

func newHost(concurrentRequests int) *host {
	h := &host{
		Mutex: &sync.Mutex{},
		token: make(chan struct{}, concurrentRequests)}

	for i := 0; i < concurrentRequests; i++ {
		h.token <- struct{}{}
	}

	return h
}

func (h *host) load() float64 {
	h.Lock()
	// pendingRequests + 1 because otherwise it can be 0
	// however the host with least avgResponsetime should still be
	// prefered
	load := h.avgResponseTime * float64(h.pendingRequests+1)
	h.Unlock()
	return load
}

// LoadBalancer is used to balance the load between different URLs.
type LoadBalancer interface {
	Add(string)
	Remove(string)
	RemovePermanent(string)
	Recover()
	IncLoad(string) (time.Time, error)
	Done(string, time.Time)
	GetLeastBusyURL() (string, error)
}

type loadBalancer struct {
	hostsMu            sync.RWMutex
	hosts              map[string]*host
	allHosts           map[string]*host
	totalRequests      int64
	alpha              float64
	concurrentRequests int
	hostFailed         bool
	timeout            time.Duration
}

// NewLoadBalancer returns a new LoadBalancer.
func NewLoadBalancer(URLs []string, concurrentRequests int, timeout time.Duration, alpha float64) LoadBalancer {
	if alpha < 0.0 || alpha > 1.0 {
		panic("alpha must be between 0 and 1.0")
	}

	lb := &loadBalancer{
		hosts:              make(map[string]*host),
		allHosts:           make(map[string]*host),
		concurrentRequests: concurrentRequests,
		timeout:            timeout,
		alpha:              alpha}

	for _, URL := range URLs {
		h := newHost(concurrentRequests)
		lb.hosts[URL] = h
		lb.allHosts[URL] = h
	}

	return lb
}

// Add URL as a new host.
func (lb *loadBalancer) Add(URL string) {
	lb.hostsMu.Lock()
	defer lb.hostsMu.Unlock()

	if _, ok := lb.hosts[URL]; ok {
		return
	}

	h := newHost(lb.concurrentRequests)

	lb.hosts[URL] = h
	lb.allHosts[URL] = h
}

// Remove a host based on its URL.
func (lb *loadBalancer) Remove(URL string) {
	lb.hostsMu.Lock()
	lb.hostFailed = true
	delete(lb.hosts, URL)
	lb.hostsMu.Unlock()
}

// Remove a host based on its URL. This host cannot be restored by the "Recover" method.
func (lb *loadBalancer) RemovePermanent(URL string) {
	lb.hostsMu.Lock()
	delete(lb.hosts, URL)
	delete(lb.allHosts, URL)
	lb.hostsMu.Unlock()
}

// Recover removed hosts.
func (lb *loadBalancer) Recover() {
	if !lb.hostFailed {
		return
	}

	lb.hostFailed = false
	lb.hostsMu.Lock()
	for URL := range lb.allHosts {
		lb.hosts[URL] = newHost(lb.concurrentRequests)
	}
	lb.hostsMu.Unlock()
}

// Increase the load on a host. Can be called before sending a request. Done has
// to be called with the same URL afterwards.
func (lb *loadBalancer) IncLoad(URL string) (time.Time, error) {
	lb.hostsMu.RLock()
	defer lb.hostsMu.RUnlock()

	h, ok := lb.hosts[URL]
	if !ok {
		return time.Now(), ErrNotExistingHost
	}

	for {
		select {
		case <-h.token:
			h.Lock()
			h.pendingRequests++
			h.Unlock()
			atomic.AddInt64(&lb.totalRequests, 1)
			return time.Now(), nil
		case <-time.After(lb.timeout):
			return time.Now(), ErrTimeout
		}

	}
}

// Tell the load balancer request on the URL finished. Should only be called
// after an increasing the load.
func (lb *loadBalancer) Done(URL string, startTime time.Time) {
	lb.hostsMu.RLock()
	defer lb.hostsMu.RUnlock()

	h, ok := lb.hosts[URL]
	if !ok {
		return
	}

	h.Lock()
	h.pendingRequests--
	atomic.AddInt64(&lb.totalRequests, -1)

	responseTime := time.Since(startTime)
	h.avgResponseTime = lb.alpha*float64(responseTime/time.Millisecond) + (1.0-lb.alpha)*h.avgResponseTime

	if len(h.token) < lb.concurrentRequests {
		h.token <- struct{}{}
	}
	h.Unlock()
}

// Get the URL with the least load.
// If none is available an error will be returned.
func (lb *loadBalancer) GetLeastBusyURL() (string, error) {
	lb.hostsMu.RLock()
	defer lb.hostsMu.RUnlock()

	minLoad := math.MaxFloat64
	var bestURL string
	for URL, h := range lb.hosts {
		load := h.load()

		if load == 0.0 {
			return URL, nil
		}

		if load < minLoad {
			minLoad = load
			bestURL = URL
		}
	}

	if bestURL == "" {
		return "", ErrNoRemainingHosts
	}

	return bestURL, nil
}
