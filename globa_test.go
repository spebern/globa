package globa

import (
	"testing"
	"time"
)

func TestAdd(t *testing.T) {
	lb := NewLoadBalancer([]string{}, 2).(*loadBalancer)
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
	if host.load != 0 {
		t.Fatal("hosts initial load should be 0")
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
	lb := NewLoadBalancer([]string{URL}, 0).(*loadBalancer)

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
	lb := NewLoadBalancer([]string{URL}, 2).(*loadBalancer)

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
	lb := NewLoadBalancer([]string{URL1, URL2, URL3}, 2).(*loadBalancer)
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
	lb := NewLoadBalancer([]string{URL}, 2).(*loadBalancer)

	lb.IncLoad(URL)
	lb.IncLoad(URL)

	if lb.hosts[URL].load != 2 {
		t.Fatal("host should have a load of 2")
	}

	lb.Done(URL)

	if lb.hosts[URL].load != 1 {
		t.Fatal("host should have a load of 1")
	}

	lb.Done(URL)

	if lb.hosts[URL].load != 0 {
		t.Fatal("host should have a load of 0")
	}

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("increasing load of non existing host should panic")
		}
	}()

	lb.IncLoad("www.not-existing.de")
}

func TestDonePanic(t *testing.T) {
	lb := NewLoadBalancer([]string{}, 2).(*loadBalancer)

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("calling done on non existing host should panic")
		}
	}()

	lb.Done("www.not-existing.de")
}

func TestGetLeastBusyHost(t *testing.T) {
	URL1 := "www.google.de"
	URL2 := "www.baidu.com"

	lb := NewLoadBalancer([]string{URL1, URL2}, 2).(*loadBalancer)

	lb.IncLoad(URL1)

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

	lb.Done(URL1)

	lb.Remove(URL1)

	bestURL, err = lb.GetLeastBusyURL()
	if err == nil {
		t.Fatal("error should have occurred")
	}
	if bestURL != "" {
		t.Fatal("empty URL should be returned")
	}
}

func request(lb LoadBalancer, done chan bool) {
	URL, err := lb.GetLeastBusyURL()

	lb.IncLoad(URL)
	defer func() {
		lb.Done(URL)
		done <- true
	}()

	if err != nil {
		lb.Recover()
	}

	time.Sleep(100 * time.Millisecond)
}

func TestRace(t *testing.T) {
	done := make(chan bool)
	lb := NewLoadBalancer([]string{"www.google.de", "www.baidu.de", "www.bing.de"}, 3)

	var requestCount = 100
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
}
