# Go load balancer

This package provides a simple load balancer. The balancing is based on the total load on an URL.
There is also an option for setting the maximum number of concurrent requests on an URL.

``` go
package main

import "github.com/spebern/globa"

func main() {
	us := []string{"www.google.de", "www.bing.de"}
	maxConcurrentRequests := 3

	lb := globa.NewLoadBalancer(URLs, maxConcurrentRequests)

	u, err := lb.GetLeastBusyURL()
	if err != nil {
		lb.IncLoad(u)

		// do your request here

		lb.Done(u)
	}

	u, err = lb.GetLeastBusyURL()

	if err != nil {
		lb.IncLoad(u)

		// do your request here
		// ups failed!

		lb.Remove(u)

		// done also has to be called after failing!
		lb.Done(u)
	}

	// after some time recover to all initial urls

	lb.Recover()
}
```
