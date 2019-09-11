local require = require
local assert, print = assert, print
local table = table
local ipairs = ipairs
local mqttclient = require("luamqttc/client")
local rex = require("rex_pcre")

local host = "localhost"
local port = 1883
local timeout = 1 -- 1 seconds

local topics = { "room/+/res/create", 
  "room/+/res/close", 
  "room/+/res/invite", 
  "room/+/res/join", 
  "room/+/res/accept_join", 
  "room/+/res/kick", 
  "room/+/res/leave", 
  "room/+/res/prestart", 
  "room/+/res/start", 
  "member/+/res/login", 
  "member/+/res/logout"}
local wildtopics = { "TopicA/+", "+/C", "#", "/#", "/+", "+/+", "TopicA/#" }
local nosubscribe_topics = { "nosubscribe" }

local cb_buf = {}

s = 1
logincount = 1
loopsize = 1

idcount = 0;
function getid()
  idcount = idcount + 1
  return idcount
end


cell = {
  id = getid(),
  isLogin = false,
  hero = "",
  start = false,
  prestart = false,
  nextaction = function(cell, aclient)
    end,
  }

local basic = function()
    print("Basic test")
    cb_buf = {}
    local aclient = mqttclient.new("myclientid", {
        clean_session = true,
        will_flag = true,
        will_options =
        {
            topic_name = "member/damody/send/logout",
            message = [[{"id":"damody"}]],
            retained = true
        }
    })
    local callback = function(topic, data, packet_id, dup, qos, retained)
      --print("cb 1: ", topic, data, qos)
      a = rex.match(topic , "room/(\\w+)/res/prestart")
      if a then
        aclient:publish(string.format("room/%s/send/prestart", a), string.format([[{"id":"%s", "room":"%s", "accept": true}]], a, a), { qos = 1 })
        aclient:message_loop(0.1)
        end
    end
    assert(aclient:connect(host, port, {timeout = timeout}))
    for k,v in pairs(topics) do
      print(v)
      assert(aclient:subscribe(v, 2, callback))
    end
    for q=1,loopsize do
      for i = s,s+logincount do
        
        local msg = string.format([[{"id":"da_%02d"}]], i)
        local topic = string.format("member/da_%02d/send/login", i)
        assert(aclient:publish(topic, msg, { qos = 1 }))
        local msg = string.format([[{"id":"da_%02d"}]], i)
        local topic = string.format("room/da_%02d/send/create", i)
        assert(aclient:publish(topic, msg, { qos = 1 }))
        local msg = string.format([[{"id":"da_%02d","hero":"freyja"}]], i)
        local topic = string.format("member/da_%02d/send/choose_hero", i)
        assert(aclient:publish(topic, msg, { qos = 1 }))
        local msg = string.format([[{"room":"da_%02d", "action":"start game"}]], i)
        local topic = string.format("room/da_%02d/send/start_queue", i)
        assert(aclient:publish(topic, msg, { qos = 1 }))
        
        local id = string.format("da_%02d", i)
        --aclient:publish(string.format("room/%s/send/prestart", id), string.format([[{"id":"%s", "room":"%s", "accept": true}]], id, id), { qos = 1 })
      end
      aclient:message_loop(0.2)
      s = s+logincount+1
    end
    aclient:message_loop(1)
    
    aclient:disconnect()
    print("Basic test finished")
end

basic()