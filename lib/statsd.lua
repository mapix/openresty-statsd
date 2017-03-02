local Statsd = {}
local os = require "os"
local math = require "math"

math.randomseed(os.time())

Statsd.time  = function (bucket, time, sample_rate)
  if sample_rate and (not (sample_rate == 1 or math.random() <= sample_rate)) then
    return
  end
  Statsd.register(bucket, time .. "|ms")
end

Statsd.count = function (bucket, n, sample_rate)
  if sample_rate and (not (sample_rate == 1 or math.random() <= sample_rate)) then
    return
  end
  local suffix
  if sample_rate and sample_rate ~= 1 then
    suffix = "|c|@" .. sample_rate
  else
    suffix = "|c"
  end
  Statsd.register(bucket, n .. suffix)
end

Statsd.incr  = function (bucket, sample_rate)
  Statsd.count(bucket, 1, sample_rate)
end

Statsd.buffer = {} -- this table will be shared per worker process
                   -- if lua_code_cache is off, it will be cleared every request

Statsd.flush = function(sock, host, port)
   if sock then -- send buffer
     pcall(function()
               local udp = sock()
               udp:setpeername(host, port)
               for k in pairs(Statsd.buffer) do
                 udp:send(Statsd.buffer[k])
               end
               udp:close()
            end)
   end
   Statsd.buffer = {}
end

Statsd.register = function (bucket, suffix, sample_rate)
   table.insert(Statsd.buffer, bucket .. ":" .. suffix .. "\n")
end

return Statsd 
