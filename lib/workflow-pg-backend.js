// Copyright 2013 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.

// This module requires some PostgreSQL 9.1+ features

var util = require('util');
var vasync = require('vasync');
var Logger = require('bunyan');
var pg = require('pg');
var wf = require('wf');
var makeEmitter = wf.makeEmitter;
var sprintf = util.format;
var assert = require('assert');
var ta = require('./tables-definitions');

// Returns true when "obj" (Object) has all the properties "kv" (Object) has,
// and with exactly the same values, otherwise, false
function hasPropsAndVals(obj, kv) {
    if (typeof (obj) !== 'object' || typeof (kv) !== 'object') {
        return (false);
    }

    if (Object.keys(kv).length === 0) {
        return (true);
    }

    return (Object.keys(kv).every(function (k) {
        return (obj[k] && obj[k] === kv[k]);
    }));
}

var WorkflowPgBackend = module.exports = function (config) {

    if (typeof (config) !== 'object') {
        throw new TypeError('\'config\' (Object) required');
    }

    var log;
    if (config.log) {
        log = config.log.child({component: 'wf-pg-backend'});
    } else {
        if (!config.logger) {
            config.logger = {};
        }

        config.logger.name = 'wf-pg-backend';
        config.logger.serializers = {
            err: Logger.stdSerializers.err
        };

        config.logger.streams = config.logger.streams || [ {
            level: 'info',
            stream: process.stdout
        }];

        log = new Logger(config.logger);
    }

    var client;

    // Private
    function _areParamsEqual(a, b) {
        var aKeys = Object.keys(a),
            bKeys = Object.keys(b),
            diff = aKeys.filter(function (k) {
                return (bKeys.indexOf(k) === -1);
            }),
            p;

        // Forget if we just don't have the same number of members:
        if (aKeys.length !== bKeys.length) {
            return false;
        }

        // Forget if we don't have the same keys exactly
        if (diff.length > 0) {
            return false;
        }

        for (p in a) {
            if (b[p] !== a[p]) {
                return false;
            }
        }

        return true;
    }

    function _execQuery(query, vals, callback) {

        if (typeof (vals) === 'function') {
            callback = vals;
            vals = null;
        }

        var cb = function (err, res) {
            if (err) {
                log.error({err: err}, 'PG Query Error');
                return callback(err);
            } else {
                return callback(null, res);
            }
        };

        if (vals) {
            log.debug({query: query, vals: vals}, 'PG Query');
            return client.query(query, vals, cb);
        } else {
            log.debug({query: query}, 'PG Query');
            return client.query(query, cb);
        }
    }

    var backend = {
        log: log,
        config: config,
        client: client,
        quit: function quit(callback) {
            if (backend.connected) {
                client.end();
            }
            callback();
        },
        ping: function ping(callback) {
            _execQuery('select 1 as ok', function (err, res) {
                if (err) {
                    return callback(err);
                }
                return callback(null, 'OK');
            });
        },
        connected: false
    };

    makeEmitter(backend);

    function _tableExists(table_name, callback) {
        var query = 'select count (*) from' +
              ' information_schema.tables where table_name=$1';
        var vals = [table_name];

        _execQuery(query, vals, function (err, res) {
            if (err || Number(res.rows[0].count) === 0) {
                return callback(false);
            } else {
                return callback(true);
            }
        });
    }

    function _createTables(callback) {
        var queries = [];

        vasync.forEachParallel({
            inputs: Object.keys(ta.tables),
            func: function tableQueries(table, next) {
                _tableExists(table, function (exists) {
                    if (!exists) {
                        queries = queries.concat(ta.tables[table]);
                    }
                    return next(null);
                });
            }
        }, function (err, res) {
            var pipeline = queries.map(function (q) {
                return (function (_, cb) {
                    _execQuery(q, cb);
                });
            });

            return vasync.pipeline({
                funcs: pipeline
            }, function (er1, res2) {
                log.trace({results: res2}, 'Pipeline results');
                return callback(er1);
            });
        });
    }

    function _decodeJob(job) {
        if (job.chain) {
            job.chain = JSON.parse(job.chain);
        }

        if (job.onerror) {
            job.onerror = JSON.parse(job.onerror);
        }

        if (job.chain_results) {
            job.chain_results = JSON.parse(job.chain_results);
        }

        if (job.onerror_results) {
            job.onerror_results = JSON.parse(job.onerror_results);
        }

        if (job.params && typeof (job.params) === 'string') {
            job.params = JSON.parse(job.params);
        }

        return job;
    }


    function init(callback) {
        var pg_opts = {
            port: config.port || 5432,
            host: config.host || 'localhost',
            database: config.database || 'node_workflow',
            user: config.user || 'postgres',
            password: config.password || ''
        };

        client = new pg.Client(pg_opts);

        client.on('notice', function (msg) {
            log.info({notice: msg}, 'PG notice');
        });

        client.on('error', function (err) {
            log.error({err: err}, 'PG client error');
            // Mark as disconnected?
            backend.emit('error', err);
        });

        return client.connect(function (err) {
            if (err) {
                return callback(err);
            }
            backend.connected = true;
            backend.client = client;
            backend.emit('connected');
            return _createTables(callback);
        });
    }

    backend.init = init;

    // workflow - Workflow object
    // callback - f(err, workflow)
    function createWorkflow(workflow, callback) {
        var keys = Object.keys(workflow);
        var idx = 0;
        var val_places = [];
        var vals = [];

        keys.forEach(function (p) {
            idx += 1;
            if (typeof (workflow[p]) === 'object' && workflow[p] !== null) {
                vals.push(JSON.stringify(workflow[p]));
            } else {
                vals.push(workflow[p]);
            }
            val_places.push('$' + idx);
        });

        var query = 'INSERT INTO wf_workflows(' +
                keys.join(', ') + ') VALUES (' +
                val_places.join(', ') + ')';

        _execQuery(query, vals, function (err, res) {
            if (err) {
                if (err.code === '23505') {
                    return callback(new wf.BackendInvalidArgumentError(
                    'Workflow.name must be unique. A workflow with name "' +
                    workflow.name + '" already exists'));
                } else {
                    return callback(new wf.BackendInternalError(err.Error));
                }
            } else {
                return callback(null, workflow);
            }
        });
    }

    backend.createWorkflow = createWorkflow;


    // uuid - Workflow.uuid
    // callback - f(err, workflow)
    function getWorkflow(uuid, callback) {
        var workflow = null;
        var row = null;
        var query = 'SELECT * FROM wf_workflows WHERE uuid=$1';
        var vals = [uuid];

        _execQuery(query, vals, function (err, res) {
            if (err) {
                return callback(new wf.BackendInternalError(err.Error));
            } else {
                if (res.rows.length === 0) {
                    return callback(
                        new wf.BackendResourceNotFoundError(sprintf(
                        'Workflow with uuid \'%s\' does not exist', uuid)));
                } else {
                    workflow = {};
                    row = res.rows[0];
                    Object.keys(row).forEach(function (p) {
                        if (p === 'chain' || p === 'onerror') {
                            workflow[p] = JSON.parse(row[p]);
                        } else {
                            workflow[p] = row[p];
                        }
                    });
                    return callback(null, workflow);
                }
            }
        });
    }

    backend.getWorkflow = getWorkflow;


    // workflow - update workflow object.
    // callback - f(err, workflow)
    function updateWorkflow(workflow, callback) {
        var keys = [];
        var idx = 0;
        var val_places = [];
        var vals = [];

        Object.keys(workflow).forEach(function (p) {
            if (p === 'uuid') {
                return;
            }
            idx += 1;
            keys.push(p);
            if (typeof (workflow[p]) === 'object' && workflow[p] !== null) {
                vals.push(JSON.stringify(workflow[p]));
            } else if (p !== 'uuid') {
                vals.push(workflow[p]);
            }
            val_places.push('$' + idx);
        });

        vals.push(workflow.uuid);
        var query = 'UPDATE wf_workflows SET (' + keys.join(',') +
            ')=(' + val_places.join(',') + ') WHERE uuid=$' + (idx + 1);

        _execQuery(query, vals, function (err, res) {
            if (err) {
                return callback(new wf.BackendInternalError(err.Error));
            } else {
                return callback(null, workflow);
            }
        });
    }

    backend.updateWorkflow = updateWorkflow;


    // workflow - the workflow object
    // callback - f(err, boolean)
    function deleteWorkflow(workflow, callback) {
        var query = 'DELETE FROM wf_workflows WHERE uuid=$1 RETURNING *';
        var vals = [workflow.uuid];

        _execQuery(query, vals, function (err, result) {
            if (err) {
                return callback(new wf.BackendInternalError(err));
            } else {
                return callback(null, (result.rows.length === 1));
            }
        });
    }

    backend.deleteWorkflow = deleteWorkflow;




    // Get all the workflows:
    // - params - JSON Object (Optional). Can include the value of the
    //  workflow's "name" to search for into workflow's definition.
    // - callback - f(err, workflows)
    function getWorkflows(params, callback) {
        if (typeof (params) === 'function') {
            callback = params;
            params = {};
        }
        var workflows = [];
        var query = 'SELECT * FROM wf_workflows';
        var vals = null;
        if (params.name) {
            query += ' WHERE name=$1';
            vals = [params.name];
        }

        _execQuery(query, vals, function (err, res) {
            if (err) {
                return callback(new wf.BackendInternalError(err.Error));
            } else {
                res.rows.forEach(function (row) {
                    var workflow = {};
                    Object.keys(row).forEach(function (p) {
                        if (p === 'chain' || p === 'onerror') {
                            workflow[p] = JSON.parse(row[p]);
                        } else {
                            workflow[p] = row[p];
                        }
                    });
                    workflows.push(workflow);
                });
                return callback(null, workflows);
            }
        });
    }

    backend.getWorkflows = getWorkflows;

    // job - Job object
    // callback - f(err, job)
    function createJob(job, callback) {
        if (job.workflow) {
            job.workflow_uuid = job.workflow;
            delete job.workflow;
        }
        var keys = Object.keys(job);
        var idx = 0;
        var val_places = [];
        var vals = [];

        keys.forEach(function (p) {
            idx += 1;
            if (typeof (job[p]) === 'object' && job[p] !== null &&
                p !== 'params') {
                vals.push(JSON.stringify(job[p]));
            } else {
                vals.push(job[p]);
            }
            val_places.push('$' + idx);
        });

        var query = 'INSERT INTO wf_jobs(' +
            keys.join(', ') + ') VALUES (' +
            val_places.join(', ') + ')';

        _execQuery(query, vals, function (err, res) {
            if (err) {
                return callback(new wf.BackendInternalError(err.Error));
            } else {
                if (typeof (job.locks) !== 'undefined') {
                    var q = 'INSERT INTO wf_locked_targets(job_uuid, target)' +
                    ' VALUES ($1, $2)';
                    return _execQuery(q, [job.uuid, job.locks],
                        function (er1, res1) {
                        return callback(er1, job);
                    });
                } else {
                    return callback(null, job);
                }
            }
        });
    }

    backend.createJob = createJob;


    // uuid - Job.uuid
    // callback - f(err, job)
    function getJob(uuid, callback) {
        var job = null;
        var query = 'SELECT * FROM wf_jobs WHERE uuid=$1';
        var vals = [uuid];

        if (typeof (uuid) === 'undefined') {
            return callback(new wf.BackendInternalError(
                  'WorkflowPgBackend.getJob uuid(String) required'));
        }

        return _execQuery(query, vals, function (err, res) {
            if (err) {
                return callback(new wf.BackendInternalError(err.Error));
            } else {
                if (res.rows.length === 0) {
                    return callback(new wf.BackendResourceNotFoundError(
                            sprintf('Workflow with uuid \'%s\' does not exist',
                                uuid)));
                } else {
                    job = _decodeJob(res.rows[0]);
                    return callback(null, job);
                }
            }
        });
    }

    backend.getJob = getJob;


    // Get a single job property
    // uuid - Job uuid.
    // prop - (String) property name
    // cb - callback f(err, value)
    function getJobProperty(uuid, prop, cb) {
        var value = null;
        var encoded_props = [
            'chain', 'chain_results',
            'onerror', 'onerror_results'
        ];
        var query = 'SELECT (' + prop + ') FROM wf_jobs WHERE uuid=$1';
        var vals = [uuid];

        _execQuery(query, vals, function (err, res) {
            if (err) {
                return cb(new wf.BackendInternalError(err.Error));
            } else {
                if (res.rows.length === 0) {
                    return cb(new wf.BackendResourceNotFoundError(sprintf(
                        'Job with uuid \'%s\' does not exist', uuid)));
                } else {
                    if (encoded_props.indexOf(prop) !== -1) {
                        value = JSON.parse(res.rows[0][prop]);
                    } else {
                        value = res.rows[0][prop];
                    }
                    return cb(null, value);
                }
            }
        });
    }

    backend.getJobProperty = getJobProperty;

    function _isTargetLocked(target, cb) {
        var q = 'SELECT COUNT(*) FROM wf_locked_targets WHERE ' +
                '$1 ~* target';
        _execQuery(q, [target], function (err, res) {
            if (err) {
                return cb(new wf.BackendInternalError(err.Error));
            } else {
                var locked = (res.rows.length !== 0 &&
                    Number(res.rows[0].count) !== 0);
                return cb(null, locked);
            }
        });
    }
    // job - the job object
    // callback - f(err) called with error in case there is a duplicated
    // job with the same target and same params
    function validateJobTarget(job, callback) {
        if (typeof (job) === 'undefined') {
            return callback(new wf.BackendInternalError(
                'WorkflowPgBackend.validateJobTarget job(Object) required'));
        }

        // If no target is given, we don't care:
        if (!job.target) {
            return callback(null);
        }

        return _isTargetLocked(job.target, function (err1, locked) {
            if (err1) {
                return callback(new wf.BackendInternalError(err1.message));
            }

            if (locked) {
                return callback(new wf.BackendInvalidArgumentError(
                    'Job target is currently locked by another job'));
            }

            var query = 'SELECT COUNT(*) FROM wf_jobs WHERE ' +
                'workflow_uuid=$1 AND target=$2 AND ' +
                '(execution=\'queued\' OR execution=\'running\' OR ' +
                'execution=\'waiting\')';

            var vals = [job.workflow_uuid, job.target];

            var idx = 2;
            Object.keys(job.params).forEach(function (p) {
                idx += 1;
                query += ' AND params->>\'' + p + '\'=$' + idx;
                vals.push(job.params[p]);
            });


            return _execQuery(query, vals, function (err, res) {
                if (err) {
                    return callback(new wf.BackendInternalError(err.Error));
                } else {
                    if (res.rows.length !== 0 &&
                        Number(res.rows[0].count) !== 0) {
                        return callback(new wf.BackendInvalidArgumentError(
                            'Another job with the same target' +
                            ' and params is already queued'));
                    } else {
                        return callback(null);
                    }
                }
            });

        });
    }

    backend.validateJobTarget = validateJobTarget;


    // Get the next queued job.
    // index - Integer, optional. When given, it'll get the job at index
    //         position (when not given, it'll return the job at position zero)
    // callback - f(err, job)
    function nextJob(index, callback) {
        var job = null;

        if (typeof (index) === 'function') {
            callback = index;
            index = 0;
        }

        var query = 'SELECT * FROM wf_jobs WHERE execution=\'queued\' ' +
            'ORDER BY created_at ASC LIMIT 1 OFFSET ' + index;

        _execQuery(query, function (err, res) {
            if (err) {
                return callback(new wf.BackendInternalError(err.Error));
            } else {
                if (res.rows.length !== 0) {
                    job = _decodeJob(res.rows[0]);
                }
                return callback(null, job);
            }
        });
    }

    backend.nextJob = nextJob;


    // Get the given number of queued jobs uuids.
    // - start - Integer - Position of the first job to retrieve
    // - stop - Integer - Position of the last job to retrieve, _included_
    // - callback - f(err, jobs)
    function nextJobs(start, stop, callback) {
        var jobs = [];
        var index = start;
        var limit = (stop - start) + 1;
        var query = 'SELECT uuid FROM wf_jobs WHERE execution=\'queued\' ' +
              'ORDER BY created_at ASC LIMIT ' + limit + ' OFFSET ' + index;

        _execQuery(query, function (err, res) {
            if (err) {
                return callback(new wf.BackendInternalError(err.Error));
            } else {
                res.rows.forEach(function (row) {
                    jobs.push(row.uuid);
                });
                return callback(null, jobs);
            }
        });
    }

    backend.nextJobs = nextJobs;


    // Register a runner on the backend and report it's active:
    // - runner_id - String, unique identifier for runner.
    // - active_at - ISO String timestamp. Optional. If none is given,
    // current time
    // - callback - f(err)
    function registerRunner(runner_id, active_at, callback) {
        var query = 'UPDATE wf_runners SET (active_at)=($1) ' +
                'WHERE uuid=$2 RETURNING *';

        if (typeof (active_at) === 'function') {
            callback = active_at;
            active_at = new Date().toISOString();
        }
        var vals = [active_at, runner_id];
        _execQuery(query, vals, function (err, res) {
            if (err) {
                return callback(new wf.BackendInternalError(err.Error));
            }
            if (res.rows.length !== 1) {
                query = 'INSERT INTO wf_runners (active_at, uuid) ' +
                    'VALUES ($1, $2)';
                return _execQuery(query, vals, function (err, res) {
                    if (err) {
                        return callback(
                            new wf.BackendInternalError(err.Error));
                    } else {
                        return callback(null);
                    }
                });
            } else {
                return callback(null);
            }
        });
    }

    backend.registerRunner = registerRunner;


    // Report a runner remains active:
    // - runner_id - String, unique identifier for runner. Required.
    // - active_at - ISO String timestamp. Optional. If none is given,
    // current time
    // - callback - f(err)
    function runnerActive(runner_id, active_at, callback) {
        registerRunner(runner_id, active_at, callback);
    }

    backend.runnerActive = runnerActive;

    // Get the given runner id details
    // - runner_id - String, unique identifier for runner. Required.
    // - callback - f(err, runner)
    function getRunner(runner_id, callback) {
        var runner = null;
        var query = 'SELECT * FROM wf_runners WHERE uuid=$1';
        var vals = [runner_id];

        _execQuery(query, vals, function (err, res) {
            if (err) {
                return callback(new wf.BackendInternalError(err.Error));
            } else {
                if (res.rows.length === 0) {
                    return callback(new wf.BackendResourceNotFoundError(
                        sprintf(
                            'WorkflowRunner with uuid \'%s\' does not exist',
                            runner_id)));
                } else {
                    runner = res.rows[0];
                    return callback(null, runner.active_at);
                }
            }
        });
    }

    backend.getRunner = getRunner;


    // Get all the registered runners:
    // - callback - f(err, runners)
    function getRunners(callback) {
        var runners = {};
        var query = 'SELECT * FROM wf_runners';

        _execQuery(query, function (err, res) {
            if (err) {
                return callback(new wf.BackendInternalError(err.Error));
            } else {
                res.rows.forEach(function (row) {
                    runners[row.uuid] = row.active_at;
                });
                return callback(null, runners);
            }
        });
    }

    backend.getRunners = getRunners;

    // Set a runner as idle:
    // - runner_id - String, unique identifier for runner
    // - callback - f(err)
    function idleRunner(runner_id, callback) {
        var query = 'UPDATE wf_runners SET (idle)=(TRUE) ' +
                  'WHERE uuid=$1 RETURNING *';
        var vals = [runner_id];

        _execQuery(query, vals, function (err, res) {
            if (err) {
                return callback(new wf.BackendInternalError(err.Error));
            }
            if (res.rows.length !== 1) {
                return new wf.BackendResourceNotFoundError(callback(
                    'Cannot idle unexisting runners'));
            } else {
                return callback(null);
            }
        });
    }

    backend.idleRunner = idleRunner;

    // Check if the given runner is idle
    // - runner_id - String, unique identifier for runner
    // - callback - f(boolean)
    function isRunnerIdle(runner_id, callback) {
        var is_idle = false;
        var query = 'SELECT * FROM wf_runners WHERE uuid=$1';
        var vals = [runner_id];

        _execQuery(query, vals, function (err, res) {
            if (err) {
                return callback(err);
            }
            if (res.rows.length !== 0) {
                is_idle = res.rows[0].idle;
            }
            return callback(is_idle);
        });
    }

    backend.isRunnerIdle = isRunnerIdle;

    // Remove idleness of the given runner
    // - runner_id - String, unique identifier for runner
    // - callback - f(err)
    function wakeUpRunner(runner_id, callback) {
        var query = 'UPDATE wf_runners SET (idle)=(FALSE) ' +
                  'WHERE uuid=$1 RETURNING *';
        var vals = [runner_id];

        _execQuery(query, vals, function (err, res) {
            if (err) {
                return callback(new wf.BackendInternalError(err.Error));
            }
            if (res.rows.length !== 1) {
                return callback(new wf.BackendResourceNotFoundError(
                    'Cannot wake up unexisting runners'));
            } else {
                return callback(null);
            }
        });
    }

    backend.wakeUpRunner = wakeUpRunner;


    // Lock a job, mark it as running by the given runner, update job status.
    // uuid - the job uuid (String)
    // runner_id - the runner identifier (String)
    // callback - f(err, job) callback will be called with error if something
    //            fails, otherwise it'll return the updated job using getJob.
    function runJob(uuid, runner_id, callback) {

        var error = null;
        var job = null;

        try {
            client.query('BEGIN');
            var query = 'SELECT * FROM wf_jobs WHERE uuid=$1 AND ' +
            'runner_id IS NULL AND execution=\'queued\' FOR UPDATE NOWAIT';
            var vals = [uuid];
            log.debug({query: query, vals: vals});
            client.query(query, vals, function (err, res) {
                if (err) {
                    log.error({err: err});
                    throw new Error(err.Error);
                }
                if (res.rows.length === 0) {
                    error = new wf.BackendPreconditionFailedError(sprintf(
                    'Job with uuid \'%s\' is not queued', uuid));
                }
            });

            var q2 = 'UPDATE wf_jobs SET (runner_id, execution)=($1,' +
                    ' \'running\') WHERE uuid=$2 AND execution=\'queued\' ' +
                'AND runner_id IS NULL RETURNING *';
            var v2 = [runner_id, uuid];
            log.debug({query: q2, vals: v2});
            client.query(q2, v2, function (err, res) {
                if (err) {
                    log.error({err: err});
                    throw err;
                }
                if (res.rows.length === 0) {
                    error = new wf.BackendPreconditionFailedError(sprintf(
                    'Unable to lock job \'%s\'', uuid));
                } else {
                    job = res.rows[0];
                }
            });

            return client.query('COMMIT', function () {
                if (job) {
                    return callback(null, _decodeJob(job));
                } else {
                    return callback(error);
                }
            });
        } catch (e) {
            error = e.message;
            return callback(new wf.BackendInternalError(error));
        }
    }

    backend.runJob = runJob;

    // Update the job while it is running with information regarding progress
    // job - the job object. It'll be saved to the backend with the provided
    //       properties.
    // callback - f(err, job) callback will be called with error if something
    //            fails, otherwise it'll return the updated job using getJob.
    function updateJob(job, callback) {
        var keys = [];
        var idx = 0;
        var val_places = [];
        var vals = [];
        var theJob = null;
        var query;
        var sql_props = [
            'name', 'uuid', 'chain', 'onerrror', 'chain_results',
            'onerror_results', 'execution', 'workflow_uuid', 'target',
            'params', 'locks', 'exec_after', 'created_at', 'runner_id',
            'timeout', 'num_attempts', 'max_attempts'
        ];

        Object.keys(job).forEach(function (p) {
            if (p === 'uuid' || sql_props.indexOf(p) < 0) {
                return;
            }
            idx += 1;
            keys.push(p);
            if (typeof (job[p]) === 'object' && job[p] !== null &&
                p !== 'params') {
                vals.push(JSON.stringify(job[p]));
            } else if (p !== 'uuid') {
                vals.push(job[p]);
            }
            val_places.push('$' + idx);
        });

        vals.push(job.uuid);
        query = 'UPDATE wf_jobs SET (' + keys.join(',') +
            ')=(' + val_places.join(',') + ') WHERE uuid=$' + (idx + 1) +
            ' RETURNING *';

        _execQuery(query, vals, function (err, result) {
            if (err) {
                return callback(new wf.BackendInternalError(err.Error));
            } else {
                theJob = _decodeJob(result.rows[0]);
                Object.keys(job).forEach(function (p) {
                    if (!theJob[p]) {
                        theJob[p] = job[p];
                    }
                });
                return callback(null, theJob);
            }
        });
    }

    backend.updateJob = updateJob;


    // Update only the given Job property. Intendeed to prevent conflicts with
    // two sources updating the same job at the same time, but different props
    // uuid - the job's uuid
    // prop - the name of the property to update
    // val - value to assign to such property
    // meta - object, meaningless for this module right now, but present due to
    // compatibility with other modules.
    // callback - f(err) called with error if something fails, otherwise  null.
    function updateJobProperty(uuid, prop, val, meta, callback) {
        var query = 'UPDATE wf_jobs SET (' + prop + ')=($1) WHERE uuid=$2';

        if (typeof (val) === 'object' && val !== null && prop !== 'params') {
            val = JSON.stringify(val);
        }

        var vals = [val, uuid];
        _execQuery(query, vals, function (err, res) {
            if (err) {
                return callback(new wf.BackendInternalError(err.Error));
            } else {
                return callback(null);
            }
        });
    }

    backend.updateJobProperty = updateJobProperty;

    // Unlock the job, mark it as finished, update the status, add the results
    // for every job's task.
    // job - the job object. It'll be saved to the backend with the provided
    //       properties.
    // cb - f(err, job) callback will be called with error if something
    //            fails, otherwise it'll return the updated job using getJob.
    function finishJob(job, cb) {
        var query = 'SELECT * FROM wf_jobs WHERE uuid=$1 AND ' +
            'execution=\'running\' OR execution=\'canceled\'';
        var vals = [job.uuid];

        _execQuery(query, vals, function (err, res) {
            if (err) {
                return cb(new wf.BackendInternalError(err.Error));
            } else if (res.rows.length === 0) {
                return cb(new wf.BackendPreconditionFailedError(
                    'Only running jobs can be finished'));
            } else {
                if (job.execution === 'running') {
                    job.execution = 'succeeded';
                }
                job.runner_id = null;
                return updateJob(job, function (er0, aJob) {
                    if (er0) {
                        return cb(new wf.BackendInternalError(err.Error));
                    }
                    if (typeof (job.locks) !== 'undefined') {
                        var q = 'DELETE FROM wf_locked_targets WHERE' +
                        ' job_uuid=$1';
                        return _execQuery(q, [job.uuid], function (er1, res1) {
                            return cb(er1, job);
                        });
                    } else {
                        return cb(null, job);
                    }
                });
            }
        });
    }

    backend.finishJob = finishJob;


    // Unlock the job, mark it as canceled, and remove the runner_id
    // uuid - string, the job uuid.
    // callback - f(err, job) callback will be called with error if something
    //            fails, otherwise it'll return the updated job using getJob.
    function cancelJob(uuid, callback) {
        if (typeof (uuid) === 'undefined') {
            return callback(new wf.BackendInternalError(
              'WorkflowPgBackend.cancelJob uuid(String) required'));
        }

        return getJob(uuid, function (err, aJob) {
            if (err) {
                log.error({err: err});
                return callback(err);
            } else {
                aJob.execution = 'canceled';
                return finishJob(aJob, callback);
            }
        });
    }

    backend.cancelJob = cancelJob;


    // Queue a job which has been running; i.e, due to whatever the reason,
    // re-queue the job. It'll unlock the job, update the status, add the
    // results for every finished task so far ...
    // job - the job Object. It'll be saved to the backend with the provided
    //       properties to ensure job status persistence.
    // callback - f(err, job) callback will be called with error if something
    //            fails, otherwise it'll return the updated job using getJob.
    function queueJob(job, callback) {
        var query = 'SELECT * FROM wf_jobs WHERE uuid=$1 AND ' +
            'execution=\'running\'';
        var vals = [job.uuid];

        _execQuery(query, vals, function (err, res) {
            if (err) {
                return callback(new wf.BackendInternalError(err.Error));
            } else if (res.rows.length === 0) {
                return new wf.BackendPreconditionFailedError(callback(
                    'Only running jobs can be queued'));
            } else {
                job.execution = 'queued';
                job.runner_id = null;
                return updateJob(job, callback);
            }
        });
    }

    backend.queueJob = queueJob;


    // Get all jobs associated with the given runner_id
    // - runner_id - String, unique identifier for runner
    // - callback - f(err, jobs). `jobs` is an array of job's UUIDs.
    //   Note `jobs` will be an array, even when empty.
    function getRunnerJobs(runner_id, callback) {
        var jobs = [];
        var query = 'SELECT uuid FROM wf_jobs WHERE runner_id=$1';
        var vals = [runner_id];

        _execQuery(query, vals, function (err, res) {
            if (err) {
                return callback(new wf.BackendInternalError(err.Error));
            } else {
                res.rows.forEach(function (row) {
                    jobs.push(row.uuid);
                });
                return callback(null, jobs);
            }
        });
    }

    backend.getRunnerJobs = getRunnerJobs;

    // Get all the jobs:
    // - params - JSON Object. Can include the value of the job's "execution"
    //   status, and any other key/value pair to search for into job's params.
    //   - execution - String, the execution status for the jobs to return.
    //                 Return all jobs if no execution status is given.
    //   - limit - Integer, max number of Jobs to retrieve. By default 1000.
    //   - offset - Integer, start retrieving Jobs from the given one. Note
    //              jobs are sorted by "created_at" DESCending.
    // - callback - f(err, jobs)
    function getJobs(params, callback) {
        var jobs = [];
        var executions = ['queued', 'failed', 'succeeded',
            'canceled', 'running', 'retried', 'waiting'
        ];
        var execution;
        var offset;
        var limit;

        if (typeof (params) === 'object') {
            execution = params.execution;
            delete params.execution;
            offset = params.offset;
            delete params.offset;
            limit = params.limit;
            delete params.limit;
        }

        if (typeof (params) === 'function') {
            callback = params;
            params = {};
        }

        if (typeof (limit) === 'undefined') {
            limit = 1000;
        }

        if (typeof (offset) === 'undefined') {
            offset = 0;
        }

        var query = 'SELECT * FROM wf_jobs WHERE ';
        var keys = [];
        var vals = [];
        var idx = 0;

        if (typeof (execution) !== 'undefined' &&
                executions.indexOf(execution !== -1)) {
            keys.push('execution');
            vals.push(execution);
        } else if (typeof (execution) !== 'undefined') {
            return callback(new wf.BackendInvalidArgumentError(
                'excution is required and must be one of' +
                '"queued", "failed", "succeeded", "canceled", "running"' +
                '"waiting", "retried"'));
        }

        Object.keys(params).forEach(function (p) {
            keys.push(p);
            vals.push(params[p]);
        });

        var k;
        var pieces = [];
        for (k = 0; k < keys.length; k += 1) {
            idx = k + 1;
            if (keys[k] === 'name' || keys[k] === 'execution') {
                pieces.push(keys[k] + '=$' + idx);
            } else {
                pieces.push('params->>\'' + keys[k] + '\'=$' + idx);
            }
        }

        query = query + pieces.join(' AND ');

        if (keys.length === 0) {
            query += '1=1';
        }

        query += util.format(' ORDER BY created_at DESC LIMIT %d OFFSET %d',
                limit, offset);

        return _execQuery(query, vals, function (err, res) {
            if (err) {
                return callback(new wf.BackendInternalError(err.Error));
            } else {
                res.rows.forEach(function (row) {
                    jobs.push(_decodeJob(row));
                });
                return callback(null, jobs);
            }
        });
    }

    backend.getJobs = getJobs;

    // Add progress information to an existing job:
    // - uuid - String, the Job's UUID.
    // - info - Object, {'key' => 'Value'}
    // - callback - f(err)
    function addInfo(uuid, info, callback) {
        var existing_info = null;
        var query = 'SELECT name FROM wf_jobs WHERE uuid=$1';
        var vals = [uuid];

        _execQuery(query, vals, function (err, res) {
            if (err) {
                return callback(new wf.BackendInternalError(err.Error));
            } else if (res.rows.length === 0) {
                return callback(new wf.BackendResourceNotFoundError(
                'Job does not exist. Cannot Update.'));
            } else {
                var q = 'SELECT info FROM wf_jobs_info WHERE job_uuid=$1';
                return _execQuery(q, vals, function (err2, res2) {
                    if (err2) {
                        return callback(new wf.BackendInternalError(
                                err2.Error));
                    }
                    var q2;
                    if (res2.rows.length === 0) {
                        existing_info = [info];
                        q2 = 'INSERT INTO wf_jobs_info (info, job_uuid)' +
                            ' VALUES ($1, $2)';
                    } else {
                        existing_info = JSON.parse(res2.rows[0].info);
                        existing_info.push(info);
                        q2 = 'UPDATE wf_jobs_info SET (info)=($1) WHERE ' +
                            'job_uuid=$2';
                    }
                    var v2 = [JSON.stringify(existing_info), uuid];
                    return _execQuery(q2, v2, function (err3, res3) {
                        if (err3) {
                            return callback(new wf.BackendInternalError(
                                    err2.Error));
                        }
                        return callback(null);
                    });
                });
            }
        });
    }

    backend.addInfo = addInfo;

    // Get progress information from an existing job:
    // - uuid - String, the Job's UUID.
    // - callback - f(err, info)
    function getInfo(uuid, callback) {

        var query = 'SELECT name FROM wf_jobs WHERE uuid=$1';
        var vals = [uuid];

        _execQuery(query, vals, function (err, res) {
            if (err) {
                return callback(new wf.BackendInternalError(err.Error));
            } else if (res.rows.length === 0) {
                return callback(new wf.BackendResourceNotFoundError(
                'Job does not exist. Cannot get info.'));
            } else {
                var q2 = 'SELECT info FROM wf_jobs_info WHERE job_uuid=$1';

                return _execQuery(q2, vals, function (err2, res2) {
                    if (err2) {
                        return callback(
                            new wf.BackendInternalError(err2.Error));
                    } else if (res2.rows.length === 0) {
                        return callback(null, []);
                    } else {
                        return callback(null, JSON.parse(res2.rows[0].info));
                    }
                });
            }
        });
    }

    backend.getInfo = getInfo;

    return backend;

};
