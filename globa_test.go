package globa

import (
	"log"
	"sync/atomic"
	"testing"
	"time"
)

var googleURL = string("www.google.com")
var baiduURL = string("www.baidu.com")
var bingURL = string("www.bing.com")

func TestAdd(t *testing.T) {
	lb := NewLoadBalancer([]string{}, 2, time.Second, 0.2).(*loadBalancer)
	if len(lb.hosts) != 0 {
		t.Fatal("initial length of hosts should be 0")
	}
	if len(lb.allHosts) != 0 {
		t.Fatal("initial length of all hosts should be 0")
	}

	URL := "www.google.de"
	lb.Add(URL)
	lb.Add(URL)

	host, ok := lb.hosts[URL]
	if !ok {
		t.Fatal("URL should be mapped to a host")
	}
	if host.pendingRequests != 0 {
		t.Fatal("hosts initial pendingRequests should be 0")
	}

	hostInAllHosts, ok := lb.allHosts[URL]
	if !ok {
		t.Fatal("URL should also be added to all hosts")
	}
	if host != hostInAllHosts {
		t.Fatal("host in all hosts and hosts map should be the same")
	}
}

func TestRemove(t *testing.T) {
	URL := "www.google.de"
	lb := NewLoadBalancer([]string{URL}, 0, time.Second, 0.2).(*loadBalancer)

	lb.Remove(URL)
	lb.Remove(URL)

	if len(lb.hosts) != 0 {
		t.Fatal("host should be removed")
	}

	if len(lb.allHosts) != 1 {
		t.Fatal("all hosts should have same length after simple remove")
	}
}

func TestRemovePermanent(t *testing.T) {
	URL := "www.google.de"
	lb := NewLoadBalancer([]string{URL}, 2, time.Second, 0.2).(*loadBalancer)

	lb.RemovePermanent(URL)
	lb.RemovePermanent(URL)

	if len(lb.hosts) != 0 {
		t.Fatal("host should be removed")
	}
	if len(lb.allHosts) != 0 {
		t.Fatal("host should also be removed from all hosts")
	}
}

func TestRecover(t *testing.T) {
	URL1 := "www.google.de"
	URL2 := "www.baidu.com"
	URL3 := "www.bing.de"
	lb := NewLoadBalancer([]string{URL1, URL2, URL3}, 2, time.Second, 0.2).(*loadBalancer)
	lb.Remove(URL1)
	lb.RemovePermanent(URL2)

	lb.Recover()
	if len(lb.hosts) != 2 {
		t.Fatal("there should be two hosts left after recovering")
	}

	lb.Recover()
	if len(lb.hosts) != 2 {
		t.Fatal("there should be two hosts left after second time recovering")
	}
}

func TestIncLoadAndDone(t *testing.T) {
	URL := "www.google.de"
	lb := NewLoadBalancer([]string{URL}, 2, time.Second, 0.2).(*loadBalancer)

	startTime1, _ := lb.IncLoad(URL)
	startTime2, _ := lb.IncLoad(URL)

	if lb.hosts[URL].pendingRequests != 2 {
		t.Fatal("host should have a load of 2")
	}

	lb.Done(URL, startTime1)

	if lb.hosts[URL].pendingRequests != 1 {
		t.Fatal("host should have a load of 1")
	}

	lb.Done(URL, startTime2)

	if lb.hosts[URL].pendingRequests != 0 {
		t.Fatal("host should have a load of 0")
	}

	_, err := lb.IncLoad("www.not-existing.de")
	if err == nil {
		t.Fatal("increasing load on not existing host should return error")
	}

	lb.Done("www.not-existing.de", time.Now())
}

func TestGetLeastBusyHost(t *testing.T) {
	URL1 := "www.google.de"
	URL2 := "www.baidu.com"

	lb := NewLoadBalancer([]string{URL1, URL2}, 2, time.Second, 0.2).(*loadBalancer)

	startTime, _ := lb.IncLoad(URL1)
	time.Sleep(time.Second)
	lb.Done(URL1, startTime)

	bestURL, err := lb.GetLeastBusyURL()

	if err != nil {
		t.Fatal("no error should have occurred")
	}
	if bestURL != URL2 {
		t.Fatal("did not get best URL")
	}

	lb.Remove(URL2)

	bestURL, err = lb.GetLeastBusyURL()
	if err != nil {
		t.Fatal("no error should have occurred")
	}
	if bestURL != URL1 {
		t.Fatal("did not get best URL")
	}

	lb.Done(URL1, startTime)

	lb.Remove(URL1)

	bestURL, err = lb.GetLeastBusyURL()
	if err == nil {
		t.Fatal("error should have occurred")
	}
	if bestURL != "" {
		t.Fatal("empty URL should be returned")
	}
}

var timeouts = int64(0)

func request(lb LoadBalancer, done chan bool) {
	URL, err := lb.GetLeastBusyURL()

	if err != nil {
		log.Fatal("should have gotten least busy url")
	}

	startTime, timeout := lb.IncLoad(URL)

	if timeout != nil {
		atomic.AddInt64(&timeouts, 1)
	}

	defer func() {
		lb.Done(URL, startTime)
		done <- true
	}()

	switch URL {
	case googleURL:
		time.Sleep(1 * time.Millisecond)
	case bingURL:
		time.Sleep(2 * time.Millisecond)
	case baiduURL:
		time.Sleep(4 * time.Millisecond)
	}
}

func TestRace(t *testing.T) {
	done := make(chan bool)
	lb := NewLoadBalancer([]string{googleURL, bingURL, baiduURL}, 4, 100*time.Millisecond, 0.2).(*loadBalancer)

	var requestCount = 600
	for i := 0; i < requestCount; i++ {
		go request(lb, done)
	}

	var i int
	for range done {
		i++
		if i == requestCount {
			break
		}
	}

	for URL, host := range lb.hosts {
		switch URL {
		case googleURL:
			if host.avgResponseTime < 0.5 || host.avgResponseTime > 2.5 {
				t.Fatal("avg response time of google should be between 0.5 and 1.5, but is:",
					host.avgResponseTime)
			}
		case bingURL:
			if host.avgResponseTime < 1.0 || host.avgResponseTime > 3.0 {
				t.Fatal("avg response time of bing should be between 1.5 and 2.5, but is:",
					host.avgResponseTime)
			}
		case baiduURL:
			if host.avgResponseTime < 3.0 || host.avgResponseTime > 5.0 {
				t.Fatal("avg response time of baidu should be between 1.5 and 2.5, but is:",
					host.avgResponseTime)
			}
		}
	}

	if timeouts > 20 {
		t.Fatal("there shouldn't be more than 20 timeouts, but there were:", timeouts)
	}
}
