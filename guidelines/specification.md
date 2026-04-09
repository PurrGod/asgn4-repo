# Project API

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD",
"SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be
interpreted as described in [RFC
2119](https://datatracker.ietf.org/doc/html/rfc2119).

## GET `/ping`

### Request

The request SHALL have an empty body.

### Response

The service MUST return an HTTP response with a status code of 200 and an empty
body.

## PUT `/view`

A list of all known nodes and views. For the purpose of assignment 1, there will
always be only one shard and one replica within that one shard. For future
assignments, this request will become more useful and these terms will be
defined.

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
body.

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
specified `key` so that future GET requests will return the value. The response
status code MUST be a 200 and the service MUST reply within a timely manner (at
most five seconds). Once the response to a PUT request has been sent it is considered
acknowledged.

## GET `/data/{key}`

The {key} placeholder above in the path has a maximum length of 128 characters
and can consist of the alphanumeric characters along with the dash (-). The HTTP
request SHALL have the following HTTP headers:

### Parameters
- key: guaranteed to match the following regex: `[0-9a-zA-Z-]{0,128}`.
- body: guaranteed to be empty

### Response

If this server has not acknowledged any write under the specified `key` yet, the
status code MUST be 404 and the body MUST be empty. Otherwise, the server MUST
reply with a status code of 200 and the most recently acknowledged PUT request's
body for the specified `key`.
