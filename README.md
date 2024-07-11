# lua-grpc-client
An *extremely* minimal gRPC client for Lua

By minimal I mean:
- It has only been used over a Unix socket
- It doesn't implement client-streaming requests
- Error handling isn't great
- You need to use [`lua-protobuf`](https://github.com/starwing/lua-protobuf) and manually load protobuf definitions
