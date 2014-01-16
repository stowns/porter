# redis namespace
exports.namespace = 'porter'
_ = require 'underscore'

# timeout for a job to get reaped (set on pop)
exports.timeout = 30
exports.redis = {
  host: 'localhost'
  port: 6379
  create_client: ->
    # possible pooling?
    return exports.redis.__client__ if exports.redis.__client__?
    
    redis = require 'redis'
    
    url_config = require('url').parse(exports.redis.url) if exports.redis.url?
    [url_username, url_password] = url_config.auth.split(':') if url_config?.auth?
    
    host = url_config?.hostname || exports.redis.host
    port = url_config?.port || exports.redis.port
    username = url_username || exports.redis.username
    password = url_password || exports.redis.password
    
    client = redis.createClient(port, host)
    client.auth(password) if password?

    client['scan_keys'] = (arg, cb) ->
      start = Date.now();
      keys = []
      doScan = (args, cb) ->
        args.push("COUNT");
        args.push(1000);
        client.scan args, (err, result) ->
          return cb(err, result) if err? 
          keys.push(result[1]) if result[1].length > 0
          if result[0] is "0"
            duration = Date.now() - start;
            console.log "Total execution for #{arg} : #{duration}"
            keys = _.flatten(keys)
            keys = _.unique(keys)
            return cb(err, keys) 
          doScan([result[0], "MATCH", arg], cb)

      doScan([0, "MATCH", arg], cb)

    exports.redis.__client__ = client
}

exports.worker = {
  queues: null
  min_poll_timeout: 100
  max_poll_timeout: 5000
  concurrent_commands: 1
}

exports.RedisPayloadStorage = require './redis_payload_storage'

exports.payload_storage = exports.RedisPayloadStorage
