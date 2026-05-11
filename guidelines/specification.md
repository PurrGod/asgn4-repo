# Project API

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD",
"SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be
interpreted as described in [RFC
2119](https://datatracker.ietf.org/doc/html/rfc2119).

The server MUST run on port 8081.

## GET `/ping`

### Request

The request SHALL have an empty body.

### Response

The service MUST return an HTTP response with a status code of 200 and an empty
body within a timely manner (at most N seconds from receiving the request).

## PUT `/view`

Gives the server a list of all known nodes, views, and shards. The term "shard"
will be defined in a future assignment and for now all servers will always
belong to the "defaultShard".

This endpoint is useful for upholding eventual consistency because it alerts the
replicas of their neighbors so they know who to periodically sync with. It is
also useful to onboard a new node to the view and have a client wait for the new
node to be ready to receive requests.

A client SHALL broadcast any new view to all alive nodes in the network. When a
new node comes online the current view SHALL be broadcasted to all nodes and no
additional requests will be made by any client after reanimation until all
replicas reply to their respective `/view` request with a 200 status code.

Once there has been at least N seconds of no partitions between all pairs of
replicas (both nodes in the pair can message each another with a bounded latency
of N/10 seconds) and no replicas in the view have crashed then all replicas MUST
have already replied with a 200 status code once this condition is met. Notably
the N seconds of no partitions between any pair might not happen at the same
time as the other N seconds of no partitions between any pair.

For example, in a three node view of nodes labeled A, B, and C, for the first N
seconds after the view change, node A and B are not partitioned from one another
while C is partitioned from both. Then in the next N seconds A and B are
partitioned and C is not partitioned from either. So for this execution, within
2N seconds of the view change, all nodes MUST have acknowledged the view with a
200 status code.

Furthermore, if only the IP addresses of the nodes changed, then all nodes MUST
reply with a 200 status code within N seconds of the view change, regardless of
the network topology.

During a view change, any pending PUT request MAY become implicitly
acknowledged, even without notifying the client of such an acknowledgement.

### Request

The HTTP request SHALL have the following HTTP headers:

- Content-Type: application/json
- Content-Length: <length>

The body of the request SHALL be JSON in the following format:

```json
{ "defaultShard": [ {"address": "196.168.0.1:8081", "id": 1} ] }
```

Please note that the "id" field is an integer, not a string.

### Response

The service MUST return an HTTP response with a status code of 200 and an empty
body within a timely manner (at most five seconds from receiving the request).

## PUT `/data/{key}`

A request for the service to store the body content as the value under the
specified `key`.

The {key} placeholder above in the path has a maximum length of 128 characters
and can consist of the alphanumeric characters along with the dash (-). The body
content SHALL be ASCII text. 

### Parameters


- key: guaranteed to match the following regex: `[0-9a-zA-Z-]{0,128}`.
- body: guaranteed to be ASCII text

### Response

The response MUST only be sent once the body content has been stored under the
specified `key` across all servers.

You MAY return a response with a 500 status code if the server has not received
a view yet. Otherwise, the response status code MUST be a 200 and the service
MUST reply within a timely manner (at most N seconds from receiving the request)
UNLESS replying that soon would violate strong consistency due to a partition in
the network or a crashed node. Once the response to a PUT request has been sent
it is considered acknowledged.

## GET `/data/{key}`

The {key} placeholder above in the path has a maximum length of 128 characters
and can consist of the alphanumeric characters along with the dash (-).

### Parameters
- key: guaranteed to match the following regex: `[0-9a-zA-Z-]{0,128}`.
- body: guaranteed to be empty

### Response

You MAY return a response with a 500 status code if the server has not received
a view yet. Otherwise, if this server has not acknowledged any write under the
specified `key` yet, the status code MUST be 404 and the body MUST be empty.
Otherwise, the server MUST reply with a status code of 200 and the most recently
acknowledged PUT request's body for the specified `key`. Either way, the server
MUST reply within a timely manner (at most N seconds from receiving the request)
UNLESS replying would violate strong consistency due to a partition in the
network or a crashed node.
