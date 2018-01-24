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
	defer h.Unlock()

	// pendingRequests + 1 because otherwise it can be 0
	// however the host with least avgResponsetime should still be
	// prefered
	return h.avgResponseTime * float64(h.pendingRequests+1)
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
	defer lb.hostsMu.Unlock()
	lb.hostFailed = true
	delete(lb.hosts, URL)
}

// Remove a host based on its URL. This host cannot be restored by the "Recover" method.
func (lb *loadBalancer) RemovePermanent(URL string) {
	lb.hostsMu.Lock()
	defer lb.hostsMu.Unlock()
	delete(lb.hosts, URL)
	delete(lb.allHosts, URL)
}

// Recover removed hosts.
func (lb *loadBalancer) Recover() {
	if !lb.hostFailed {
		return
	}
	lb.hostsMu.Lock()
	defer lb.hostsMu.Unlock()

	lb.hostFailed = false
	for URL := range lb.allHosts {
		lb.hosts[URL] = lb.allHosts[URL]
	}
}

// Increase the load on a host. Can be called before sending a request. Done has
// to be called with the same URL afterwards.
func (lb *loadBalancer) IncLoad(URL string) (time.Time, error) {
	lb.hostsMu.RLock()
	defer lb.hostsMu.RUnlock()

	h, ok := lb.hosts[URL]
	if !ok {
		panic("increased load of non existing host")
	}

	for {
		select {
		case <-h.token:
			h.Lock()
			defer h.Unlock()
			h.pendingRequests++
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
		panic("called done for of non existing host")
	}

	h.Lock()
	defer h.Unlock()

	h.pendingRequests--
	atomic.AddInt64(&lb.totalRequests, -1)

	responseTime := time.Since(startTime)
	h.avgResponseTime = lb.alpha*float64(responseTime/time.Millisecond) + (1.0-lb.alpha)*h.avgResponseTime

	if len(h.token) < lb.concurrentRequests {
		h.token <- struct{}{}
	}
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
		return "", errors.New("no remaining hosts")
	}

	return bestURL, nil
}
