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
	load int64

	*sync.Mutex
	token chan struct{}
}

// LoadBalancer is used to balance the load between different URLs.
type LoadBalancer interface {
	Add(string)
	Remove(string)
	RemovePermanent(string)
	Recover()
	IncLoad(string) error
	Done(string)
	GetLeastBusyURL() (string, error)
}

type loadBalancer struct {
	hostsMu            sync.RWMutex
	hosts              map[string]*host
	allHosts           map[string]*host
	totalLoad          int64
	concurrentRequests int
	hostFailed         bool
	timeout            time.Duration
}

// NewLoadBalancer returns a new LoadBalancer.
func NewLoadBalancer(URLs []string, concurrentRequests int, timeout time.Duration) LoadBalancer {
	lb := &loadBalancer{
		hosts:              make(map[string]*host),
		allHosts:           make(map[string]*host),
		concurrentRequests: concurrentRequests,
		timeout:            timeout}

	for _, URL := range URLs {
		h := host{0, &sync.Mutex{}, make(chan struct{}, concurrentRequests)}
		for i := 0; i < concurrentRequests; i++ {
			h.token <- struct{}{}
		}

		lb.hosts[URL] = &h
		lb.allHosts[URL] = &h
	}

	return lb
}

// Add URL as a new host.
func (lb *loadBalancer) Add(URL string) {
	if _, ok := lb.hosts[URL]; ok {
		return
	}

	lb.hostsMu.Lock()
	defer lb.hostsMu.Unlock()

	h := &host{0, &sync.Mutex{}, make(chan struct{}, lb.concurrentRequests)}
	for i := 0; i < lb.concurrentRequests; i++ {
		h.token <- struct{}{}
	}
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
func (lb *loadBalancer) IncLoad(URL string) error {
	lb.hostsMu.RLock()
	defer lb.hostsMu.RUnlock()

	h, ok := lb.hosts[URL]
	if !ok {
		panic("increased load of non existing host")
	}

	for {
		select {
		case <-h.token:
			atomic.AddInt64(&h.load, 1)
			atomic.AddInt64(&lb.totalLoad, 1)
			return nil
		case <-time.After(lb.timeout):
			return ErrTimeout
		}

	}
}

// Tell the load balancer request on the URL finished. Should only be called
// after an increasing the load.
func (lb *loadBalancer) Done(URL string) {
	lb.hostsMu.RLock()
	defer lb.hostsMu.RUnlock()

	h, ok := lb.hosts[URL]
	if !ok {
		panic("called done for of non existing host")
	}

	atomic.AddInt64(&h.load, -1)
	atomic.AddInt64(&lb.totalLoad, -1)

	h.Lock()
	defer h.Unlock()
	if len(h.token) < lb.concurrentRequests {
		h.token <- struct{}{}
	}
}

// Get the URL with the least load.
// If none is available an error will be returned.
func (lb *loadBalancer) GetLeastBusyURL() (string, error) {
	lb.hostsMu.RLock()
	defer lb.hostsMu.RUnlock()

	var minLoad int64 = math.MaxInt64
	var bestURL string
	for URL, h := range lb.hosts {
		load := atomic.LoadInt64(&h.load)

		if load == 0 {
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
