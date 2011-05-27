#!/usr/bin/env coffee

argv = (require 'optimist')
  .options('sock',    alias:'s')
  .options('host',    alias:'h', default:'127.0.0.1')
  .options('port',    alias:'p', default:11746)
  .options('verbose', alias:'v', default:false)
  .argv

JsonLineProtocol = (require 'json-line-protocol').JsonLineProtocol

util = require 'util'
net  = require 'net'

Array::remove = (element) ->
  for e, i in @ when e is element
    return @splice i, 1

class Peer
  constructor: (@socket, @manager) ->
    [@current_job, @can_do] = [null, {}]

    socket.setKeepAlive true
    socket.setNoDelay   true
    socket.setEncoding 'utf8'

    protocol = new JsonLineProtocol

    socket.on 'data', (data) =>
      protocol.feed data

    protocol.on 'value', (command) =>
      @handle_command command 

    socket.on 'close', =>
      @manager.remove_peer @
      @closed = true

    protocol.on 'protocol-error', (error, line) =>
      socket.destroy()
      @manager.remove_peer peer

  handle_command: (command) =>
    switch command.cmd
      when 'can_do'
        for job_type in command.types
          @can_do[job_type] = true
        @manager.add_worker @

      when 'do'
        job = new Job @, command.id, command.type, command.arg
        @manager.add_job job

      when 'did'
        @manager.job_done @, command.id, command.res

      when 'metrics'
        @write cmd:'metrics', metrics:manager.metrics

  write: (value) ->
    unless @closed? # work results can be returned to clients that have left.
      @socket.write (JSON.stringify value) + '\r\n'

class Job
  constructor: (@submitter, @id, @type, @arg) ->

class Manager
  constructor: ->
    [@jobs, @workers, @queues, @metrics] = [{}, {}, {}, {}]
    @metrics= up_since:Date.now()

  increment_metric: (key) ->
    @metrics[key] = (@metrics[key] ? 0) + 1

  add_worker: (peer) ->
    @increment_metric 'workers_added'
    for job_type, _ of peer.can_do
      (@workers[job_type] or= []).push peer
      @dispatch_jobs job_type

  remove_peer: (peer) ->
    for job_type, _ of peer.can_do
      @workers[job_type].remove peer
    if peer.current_job?
      @increment_metric 'jobs_reassigned'
      @add_job peer.current_job

  add_job: (job) ->
    @increment_metric 'jobs_added'
    @jobs[job.id] = job
    (@queues[job.type] or= []).push job
    @dispatch_jobs job.type

  job_done: (worker, job_id, result) ->
    @increment_metric 'jobs_done'
    worker.current_job = null
    @jobs[job_id].submitter.write cmd:'did', id:job_id, res:result
    delete @jobs[job_id]
    for job_type, _ of worker.can_do
      @dispatch_jobs job_type

  dispatch_jobs: (job_type) ->
    return unless (q = @queues[job_type]  ? []).length > 0 # nothing to do?
    return unless (w = @workers[job_type] ? []).length > 0 # no one to do it?
    assigned = 0
    for job in q
      for peer in w when not peer.current_job?
        assigned++
        peer.current_job = job
        peer.write cmd:'do', id:job.id, type:job.type, arg:job.arg
        @increment_metric 'jobs_dispatched'
        break
    if assigned > 0
      if q.length is assigned
        delete @queues[job_type]
      else
        @queues[job_type].splice 0, assigned
    return

manager = new Manager

handle_connection = (socket) ->
  manager.increment_metric 'connections'
  peer = new Peer socket, manager

tcp_server = net.createServer handle_connection
tcp_server.listen (parseInt argv.port, 10), argv.host
tcp_server.on 'error', (error) ->
  console.error "tcp server error: #{error.toString()}"

if argv.sock
  unix_server = net.createServer handle_connection
  unix_server.listen argv.sock
  unix_server.on 'error', (error) ->
    console.error "unix server error: #{error.toString()}"

process.on 'uncaughtException', (error) ->
  console.error "uncaught error: #{error.toString()}"

if argv.verbose
  setInterval (-> console.error JSON.stringify manager.metrics), 5000
