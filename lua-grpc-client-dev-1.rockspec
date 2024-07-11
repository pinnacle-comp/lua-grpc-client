package = "lua-grpc-client"
version = "dev-1"
source = {
	url = "git+https://github.com/pinnacle-comp/lua-grpc-client",
}
description = {
	homepage = "https://github.com/pinnacle-comp/lua-grpc-client",
	license = "MPL 2.0",
}
dependencies = {
	"lua >= 5.2",
	"cqueues ~> 20200726",
	"http ~> 0.4",
	"lua-protobuf ~> 0.5",
	"compat53 ~> 0.13",
}
build = {
	type = "builtin",
	modules = {
		grpc_client = "grpc_client.lua",
		["grpc_client.status"] = "grpc_client/status.lua",
	},
}
