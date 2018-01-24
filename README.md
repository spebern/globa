[![Build Status](https://travis-ci.org/spebern/globa.svg?branch=master)](https://travis-ci.org/spebern/globa)

# Go load balancer

This package provides a simple load balancer. The balancing is based on the total load on an URL.
There is also an option for setting the maximum number of concurrent requests on an URL.

``` go
package main

import (
	"time"

	"github.com/spebern/globa"
)

func main() {
	URLs := []string{"www.google.de", "www.bing.de"}
	maxConcurrentRequests := 3
	timeout := 5 * time.Second

	// impact of new response times on average response time
	// avgResponseTime = alpha * newResponseTime + (1.0 - alpha) * avgResponseTime
	alpha := 0.2

	lb := globa.NewLoadBalancer(URLs, maxConcurrentRequests, timeout, alpha)

	u, err := lb.GetLeastBusyURL()
	if err == nil {
		startTime, timeoutErr := lb.IncLoad(u)

		// do your request here
		lb.Done(u, startTime)
	}

	u, err = lb.GetLeastBusyURL()

	if err == nil {
		startTime, timeoutErr := lb.IncLoad(u)

		// do your request here
		// ups failed!

		// done also has to be called after failing!
		lb.Done(u, startTime)

		// remove this url since it seems broken
		lb.Remove(u)
	}

	// after some time recover to all initial urls

	lb.Recover()
}
```
