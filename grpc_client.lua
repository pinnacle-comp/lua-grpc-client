-- Support for Lua 5.2 and below
require("compat53")

local socket = require("cqueues.socket")
local headers = require("http.headers")
local h2_connection = require("http.h2_connection")
local pb = require("pb")

local grpc_client = {}

---@class grpc_client.Client
---@field conn grpc_client.h2.Conn
---@field loop grpc_client.cqueues.Loop
local Client = {}

---Create a new gRPC client that connects to the socket specified with `sock_args`.
---See `socket.connect` in the cqueues manual for more information.
---
---@param sock_args table A table of named arguments from `cqueues.socket.connect`
---@return grpc_client.Client
function grpc_client.new(sock_args)
	local sock = socket.connect(sock_args)
	sock:connect()

	local conn = h2_connection.new(sock, "client")
	conn:connect()

	---@type grpc_client.Client
	local ret = {
		conn = conn,
		loop = require("cqueues").new(),
	}

	setmetatable(ret, { __index = Client })

	return ret
end

---Encodes the given `data` as the protobuf `type`.
---
---@param type string The absolute protobuf type
---@param data table The table of data, conforming to its protobuf definition
---@return string bytes The encoded bytes
local function encode(type, data)
	local success, obj = pcall(pb.encode, type, data)
	if not success then
		error("failed to encode `" .. type .. "`: " .. obj)
	end

	local encoded_protobuf = obj

	-- The packed flag; one byte, 0 if not packed, 1 if packed.
	local packed_prefix = string.pack("I1", 0)
	-- The payload length as a 4-byte big-endian integer
	local payload_len = string.pack(">I4", encoded_protobuf:len())

	local body = packed_prefix .. payload_len .. encoded_protobuf

	return body
end

---Creates headers for a gRPC request.
---
---@param service string The desired service
---@param method string The desired method within the service
local function create_request_headers(service, method)
	local req_headers = headers.new()
	req_headers:append(":method", "POST")
	req_headers:append(":scheme", "http")
	req_headers:append(":path", "/" .. service .. "/" .. method)
	req_headers:append("te", "trailers")
	req_headers:append("content-type", "application/grpc")
	return req_headers
end

---Perform a unary request.
---
---@param request_specifier grpc_client.RequestSpecifier
---@param data table The message to send. This should be in the structure of `request_specifier.request`.
---
---@return table|nil response The response as a table in the structure of `request_specifier.response`, or `nil` if there as an error.
---@return string|nil error An error string, if any.
function Client:unary_request(request_specifier, data)
	local stream = self.conn:new_stream()

	local service = request_specifier.service
	local method = request_specifier.method
	local request_type = request_specifier.request
	local response_type = request_specifier.response

	local body = encode(request_type, data)

	stream:write_headers(create_request_headers(service, method), false)
	stream:write_chunk(body, true)

	local headers = stream:get_headers()
	local grpc_status = headers:get("grpc-status")
	if grpc_status then
		local grpc_status = tonumber(grpc_status)
		if grpc_status ~= 0 then
			local err_name = require("grpc_client.status").name(grpc_status)
			local err_str = "error from response: " .. (err_name or "unknown grpc status code")
			return nil, err_str
		end
	end

	local response_body = stream:get_next_chunk()

	local trailers = stream:get_headers()
	if trailers then -- idk if im big dummy or not but there are never any trailers
		for name, value, never_index in trailers:each() do
			print(name, value, never_index)
		end
	end

	stream:shutdown()

	-- string:sub(6) to skip the 1-byte compressed flag and the 4-byte message length
	local response = pb.decode(response_type, response_body:sub(6))

	return response, nil
end

---Perform a server-streaming request.
---
---`callback` will be called with every streamed response.
---
---@param request_specifier grpc_client.RequestSpecifier
---@param data table The message to send. This should be in the structure of `request_specifier.request`.
---@param callback fun(response: table) A callback that will be run with every response
---
---@return string|nil error An error string, if any.
function Client:server_streaming_request(request_specifier, data, callback)
	local stream = self.conn:new_stream()

	local service = request_specifier.service
	local method = request_specifier.method
	local request_type = request_specifier.request
	local response_type = request_specifier.response

	local body = encode(request_type, data)

	stream:write_headers(create_request_headers(service, method), false)
	stream:write_chunk(body, true)

	local headers = stream:get_headers()
	local grpc_status = headers:get("grpc-status")
	if grpc_status then
		local grpc_status = tonumber(grpc_status)
		if grpc_status ~= 0 then
			local err_name = require("grpc_client.status").name(grpc_status)
			local err_str = "error from response: " .. (err_name or "unknown grpc status code")
			return err_str
		end
	end

	self.loop:wrap(function()
		for response_body in stream:each_chunk() do
			while response_body:len() > 0 do
				local msg_len = string.unpack(">I4", response_body:sub(2, 5))

				-- Skip the 1-byte compressed flag and the 4-byte message length
				local body = response_body:sub(6, 6 + msg_len - 1)

				---@diagnostic disable-next-line: redefined-local
				local success, obj = pcall(pb.decode, response_type, body)
				if not success then
					print(obj)
					os.exit(1)
				end

				local response = obj
				callback(response)

				response_body = response_body:sub(msg_len + 6)
			end
		end

		local trailers = stream:get_headers()
		if trailers then
			for name, value, never_index in trailers:each() do
				print(name, value, never_index)
			end
		end
	end)

	return nil
end

---@param request_specifier grpc_client.RequestSpecifier
---@param data table The message to send. This should be in the structure of `request_specifier.request`.
---@param callback fun(response: table, stream: grpc_client.h2.Stream) A callback that will be run with every response
---
---@return grpc_client.h2.Stream|nil
---@return string|nil error An error string, if any.
function Client:bidirectional_streaming_request(request_specifier, data, callback)
	local stream = self.conn:new_stream()

	local service = request_specifier.service
	local method = request_specifier.method
	local request_type = request_specifier.request
	local response_type = request_specifier.response

	local body = encode(request_type, data)

	stream:write_headers(create_request_headers(service, method), false)
	stream:write_chunk(body, false)

	local headers = stream:get_headers()
	local grpc_status = headers:get("grpc-status")
	if grpc_status then
		local grpc_status = tonumber(grpc_status)
		if grpc_status ~= 0 then
			local err_name = require("grpc_client.status").name(grpc_status)
			local err_str = "error from response: " .. (err_name or "unknown grpc status code")
			return nil, err_str
		end
	end

	self.loop:wrap(function()
		for response_body in stream:each_chunk() do
			while response_body:len() > 0 do
				local msg_len = string.unpack(">I4", response_body:sub(2, 5))

				-- Skip the 1-byte compressed flag and the 4-byte message length
				local body = response_body:sub(6, 6 + msg_len - 1)

				---@diagnostic disable-next-line: redefined-local
				local success, obj = pcall(pb.decode, response_type, body)
				if not success then
					print(obj)
					os.exit(1)
				end

				local response = obj
				callback(response, stream)

				response_body = response_body:sub(msg_len + 6)
			end
		end

		local trailers = stream:get_headers()
		if trailers then
			for name, value, never_index in trailers:each() do
				print(name, value, never_index)
			end
		end
	end)

	return stream, nil
end

return grpc_client

-- Definitions

---@class grpc_client.h2.Conn
---@field new_stream fun(self: self): grpc_client.h2.Stream
---@field ping fun(self: self, timeout_secs: integer)

---@class grpc_client.cqueues.Loop
---@field loop function
---@field wrap fun(self: self, fn: function)

---@class grpc_client.h2.Stream
---@field write_chunk function
---@field shutdown function
---@field write_headers function
---@field get_headers function
---@field get_next_chunk function
---@field each_chunk function

---@class grpc_client.RequestSpecifier
---@field service string The fully-qualified service name
---@field method string The method name
---@field request string The fully-qualified request type
---@field response string The fully-qualified response type
