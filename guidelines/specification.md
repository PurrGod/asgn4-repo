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

Gives the server a list of all known nodes, views, and shards. A shard is a
group of replicas which all are strongly consistent across the same set of keys.
Different shards MAY store different sets of keys. This facilitates storing more
data than what any individual shard could store by itself and allows for
throughput on some keys even if another shard has a partition between its
replicas.

A client SHALL broadcast any new view to all alive nodes in the network. 

When a new node comes online or there is a change in the shard membership of any
node all nodes will remain alive, there will not be any partitions in the
network, and no additional requests will be made by any client. These guarantees
will last up until all replicas reply to their respective `/view` request with a
200 status code. Notably these guarantees are not in effect when a node is
removed from the network, when the IP address of nodes are changed, or when
the order of nodes in their shard is changed.

All replicas MUST always reply with a 200 status code within N seconds. 


During a view change, any pending PUT request MAY become implicitly
acknowledged, even without notifying the client of such an acknowledgement.

Once all alive replicas have returned a 200 status code, all nodes within a
shard MUST be strongly consistent with one another.

### Request

The HTTP request SHALL have the following HTTP headers:

- Content-Type: application/json
- Content-Length: <length>

The body of the request SHALL be JSON in the following format:

```ts
{ [shard_id]: [ {"address": "196.168.0.1:8081", "id": 1} ] }
```

For example:
```json

{ 
  "shard1": [ 
    {"address": "196.168.0.1:8081", "id": 1}, 
    {"address": "196.168.0.2:8081", "id": 2}
  ],
  "shard2": [ {"address": "196.168.0.3:8081", "id": 3} ] 
}
```

Please note that the "id" field is an integer, not a string.

### Response

The service MUST return an HTTP response with a status code of 200 and an empty
body within a timely manner (as specified above).

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
a view yet. You MAY also reply with a 307 status code. See the section in the
assignment 3 doc on temporary redirect responses for more information about when
you are allowed to reply with a 307 response code. Otherwise, the response
status code MUST be a 200 and the service MUST reply within a timely manner (at
most N seconds from receiving the request) UNLESS replying that soon would
violate strong consistency due to a partition in the network or a crashed node.
Once the response to a PUT request has been sent it is considered acknowledged.

## GET `/data/{key}`

The {key} placeholder above in the path has a maximum length of 128 characters
and can consist of the alphanumeric characters along with the dash (-).

### Parameters
- key: guaranteed to match the following regex: `[0-9a-zA-Z-]{0,128}`.
- body: guaranteed to be empty

### Response

You MAY return a response with a 500 status code if the server has not received
a view yet. You MAY also reply with a 307 status code. See the section in the
assignment 3 doc on temporary redirect responses for more information about when
you are allowed to reply with a 307 response code. Otherwise, if this server has
not acknowledged any write under the specified `key` yet, the status code MUST
be 404 and the body MUST be empty. Otherwise, the server MUST reply with a
status code of 200 and the most recently acknowledged PUT request's body for the
specified `key`. Either way, the server MUST reply within a timely manner (at
most N seconds from receiving the request) UNLESS replying would violate strong
consistency due to a partition in the network or a crashed node.
