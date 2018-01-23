package globa

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
)

type host struct {
	load    int64
	pending chan struct{}
}

// LoadBalancer is used to balance the load between different URLs.
type LoadBalancer interface {
	Add(string)
	Remove(string)
	RemovePermanent(string)
	Recover()
	IncLoad(string)
	Done(string)
	GetLeastBusyURL() (string, error)
}

type loadBalancer struct {
	hostsMu          sync.RWMutex
	hosts            map[string]*host
	allHosts         map[string]*host
	totalLoad        int64
	maxConcurrentReq int
	hostFailed       bool
}

// NewLoadBalancer returns a new LoadBalancer.
func NewLoadBalancer(URLs []string, maxConcurrentReq int) LoadBalancer {
	lb := &loadBalancer{
		hosts:    make(map[string]*host),
		allHosts: make(map[string]*host)}

	for _, URL := range URLs {
		var h host
		if maxConcurrentReq == 0 {
			h = host{0, make(chan struct{})}
		} else {
			h = host{0, make(chan struct{}, maxConcurrentReq)}
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

	h := &host{0, make(chan struct{}, lb.maxConcurrentReq)}
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
func (lb *loadBalancer) IncLoad(URL string) {
	lb.hostsMu.RLock()
	defer lb.hostsMu.RUnlock()

	h, ok := lb.hosts[URL]
	if !ok {
		panic("increased load of non existing host")
	}

	atomic.AddInt64(&h.load, 1)
	atomic.AddInt64(&lb.totalLoad, 1)
	h.pending <- struct{}{}
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

	<-h.pending
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
