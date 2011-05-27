(function() {
  var Job, JsonLineProtocol, Manager, Peer, argv, handle_connection, manager, net, tcp_server, unix_server, util;
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };
  argv = (require('optimist')).options('sock', {
    alias: 's'
  }).options('host', {
    alias: 'h',
    "default": '127.0.0.1'
  }).options('port', {
    alias: 'p',
    "default": 11746
  }).options('verbose', {
    alias: 'v',
    "default": false
  }).argv;
  JsonLineProtocol = (require('json-line-protocol')).JsonLineProtocol;
  util = require('util');
  net = require('net');
  Array.prototype.remove = function(element) {
    var e, i, _len;
    for (i = 0, _len = this.length; i < _len; i++) {
      e = this[i];
      if (e === element) {
        return this.splice(i, 1);
      }
    }
  };
  Peer = (function() {
    function Peer(socket, manager) {
      var protocol, _ref;
      this.socket = socket;
      this.manager = manager;
      this.handle_command = __bind(this.handle_command, this);
      _ref = [null, {}], this.current_job = _ref[0], this.can_do = _ref[1];
      socket.setKeepAlive(true);
      socket.setNoDelay(true);
      socket.setEncoding('utf8');
      protocol = new JsonLineProtocol;
      socket.on('data', __bind(function(data) {
        return protocol.feed(data);
      }, this));
      protocol.on('value', __bind(function(command) {
        util.debug("<RECV> " + (util.inspect(command)));
        return this.handle_command(command);
      }, this));
      socket.on('close', __bind(function() {
        this.manager.remove_peer(this);
        return this.closed = true;
      }, this));
      protocol.on('protocol-error', __bind(function(error, line) {
        socket.destroy();
        return this.manager.remove_peer(peer);
      }, this));
    }
    Peer.prototype.handle_command = function(command) {
      var job, job_type, _i, _len, _ref;
      switch (command.cmd) {
        case 'can_do':
          _ref = command.types;
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            job_type = _ref[_i];
            this.can_do[job_type] = true;
          }
          return this.manager.add_worker(this);
        case 'do':
          job = new Job(this, command.id, command.type, command.arg);
          return this.manager.add_job(job);
        case 'did':
          return this.manager.job_done(this, command.id, command.res);
        case 'metrics':
          return this.write(manager.metrics);
      }
    };
    Peer.prototype.write = function(value) {
      if (this.closed == null) {
        util.debug("<SEND> " + (util.inspect(value)));
        return this.socket.write((JSON.stringify(value)) + '\r\n');
      }
    };
    return Peer;
  })();
  Job = (function() {
    function Job(submitter, id, type, arg) {
      this.submitter = submitter;
      this.id = id;
      this.type = type;
      this.arg = arg;
    }
    return Job;
  })();
  Manager = (function() {
    function Manager() {
      var _ref;
      _ref = [{}, {}, {}, {}], this.jobs = _ref[0], this.workers = _ref[1], this.queues = _ref[2], this.metrics = _ref[3];
      this.metrics = {
        up_since: Date.now()
      };
    }
    Manager.prototype.increment_metric = function(key) {
      var _ref;
      return this.metrics[key] = ((_ref = this.metrics[key]) != null ? _ref : 0) + 1;
    };
    Manager.prototype.add_worker = function(peer) {
      var job_type, _, _base, _ref, _results;
      this.increment_metric('workers_added');
      _ref = peer.can_do;
      _results = [];
      for (job_type in _ref) {
        _ = _ref[job_type];
        ((_base = this.workers)[job_type] || (_base[job_type] = [])).push(peer);
        _results.push(this.dispatch_jobs(job_type));
      }
      return _results;
    };
    Manager.prototype.remove_peer = function(peer) {
      var job_type, _, _ref;
      _ref = peer.can_do;
      for (job_type in _ref) {
        _ = _ref[job_type];
        this.workers[job_type].remove(peer);
      }
      if (peer.current_job != null) {
        this.increment_metric('jobs_reassigned');
        return this.add_job(peer.current_job);
      }
    };
    Manager.prototype.add_job = function(job) {
      var _base, _name;
      this.increment_metric('jobs_added');
      this.jobs[job.id] = job;
      ((_base = this.queues)[_name = job.type] || (_base[_name] = [])).push(job);
      return this.dispatch_jobs(job.type);
    };
    Manager.prototype.job_done = function(worker, job_id, result) {
      var job_type, _, _ref, _results;
      this.increment_metric('jobs_done');
      worker.current_job = null;
      this.jobs[job_id].submitter.write({
        cmd: 'did',
        id: job_id,
        res: result
      });
      delete this.jobs[job_id];
      _ref = worker.can_do;
      _results = [];
      for (job_type in _ref) {
        _ = _ref[job_type];
        util.debug("dispatching more work now that I'm done: " + job_type);
        _results.push(this.dispatch_jobs(job_type));
      }
      return _results;
    };
    Manager.prototype.dispatch_jobs = function(job_type) {
      var job, peer, _i, _len, _ref, _ref2, _results;
      util.debug("dispath " + job_type + " qsize=" + ((_ref = this.queues[job_type]) != null ? _ref.length : void 0));
      _ref2 = this.queues[job_type] || [];
      _results = [];
      for (_i = 0, _len = _ref2.length; _i < _len; _i++) {
        job = _ref2[_i];
        util.debug("finding worker for job " + job.id);
        _results.push((function() {
          var _j, _len2, _ref3, _results2;
          _ref3 = this.workers[job_type] || [];
          _results2 = [];
          for (_j = 0, _len2 = _ref3.length; _j < _len2; _j++) {
            peer = _ref3[_j];
            if (peer.current_job != null) {
              continue;
            }
            peer.current_job = job;
            peer.write({
              cmd: 'do',
              id: job.id,
              type: job.type,
              arg: job.arg
            });
            this.queues[job_type].shift();
            if (this.queues[job_type].length > 0) {
              delete this.queues[job_type];
            }
            this.increment_metric('jobs_dispatched');
            break;
          }
          return _results2;
        }).call(this));
      }
      return _results;
    };
    return Manager;
  })();
  manager = new Manager;
  handle_connection = function(socket) {
    var peer;
    manager.increment_metric('connections');
    return peer = new Peer(socket, manager);
  };
  tcp_server = net.createServer(handle_connection);
  tcp_server.listen(parseInt(argv.port, 10), argv.host);
  tcp_server.on('error', function(error) {
    return console.error("tcp server error: " + (error.toString()));
  });
  if (argv.sock) {
    unix_server = net.createServer(handle_connection);
    unix_server.listen(argv.sock);
    unix_server.on('error', function(error) {
      return console.error("unix server error: " + (error.toString()));
    });
  }
  if (argv.verbose) {
    setInterval((function() {
      return console.error(JSON.stringify(manager.metrics));
    }), 5000);
  }
}).call(this);
