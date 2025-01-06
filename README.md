# pubsub-go-demo

Demo program for Danube Go library. More details [in this article](https://dev-state.com/posts/danube_demo/).

This Go program creates HTTP endpoints that accept messages in different formats (JSON, string, and number) and send these messages to the Danube Cluster using the Danube Go client library.

It can be used for Danube load testing, as in includes [a bash script](script/loop_requests.sh) that generate traffic to all exposed producer HTTP endpoints.
