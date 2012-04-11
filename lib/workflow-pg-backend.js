// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.

// This module requires some PostgreSQL 9.1+ features

var util = require('util'),
    async = require('async'),
    Logger = require('bunyan'),
    pg = require('pg').native,
    wf = require('wf'),
    WorkflowBackend = wf.WorkflowBackend;

var sprintf = util.format;

var WorkflowPgBackend = module.exports = function (config) {
  WorkflowBackend.call(this);

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

  this.log = new Logger(config.logger);
  this.config = config;
  this.client = null;
};

util.inherits(WorkflowPgBackend, WorkflowBackend);

// - callback - f(err)
WorkflowPgBackend.prototype.init = function (callback) {
  var self = this,
    pg_opts = {
      port: self.config.port || 5432,
      host: self.config.host || 'localhost',
      database: self.config.database || 'node_workflow',
      user: self.config.user || 'postgres',
      password: self.config.password || ''
    };

  self.client = new pg.Client(pg_opts);

  self.client.on('notice', function (msg) {
    self.log.info({notice: msg});
  });

  self.client.on('error', function (err) {
    self.log.error({error: err});
  });

  self.client.connect(function (err) {
    if (err) {
      return callback(err);
    }
    return self._createTables(callback);
  });
};

// Callback - f(err, res);
WorkflowPgBackend.prototype.quit = function (callback) {
  var self = this;
  if (self.client._connected === true) {
    self.client.end();
  }
  callback();
};


// workflow - Workflow object
// callback - f(err, workflow)
WorkflowPgBackend.prototype.createWorkflow = function (workflow, callback) {
  var self = this,
      keys = Object.keys(workflow),
      idx = 0,
      val_places = [],
      vals = [],
      query;

  // TODO: A good place to verify that the same tasks are not on the chain
  // and into the onerror callback (GH-1).

  keys.forEach(function (p) {
    idx += 1;
    if (typeof (workflow[p]) === 'object' && workflow[p] !== null) {
      vals.push(JSON.stringify(workflow[p]));
    } else {
      vals.push(workflow[p]);
    }
    val_places.push('$' + idx);
  });

  query = 'INSERT INTO wf_workflows(' +
    keys.join(', ') + ') VALUES (' +
    val_places.join(', ') + ')';

  self._execQuery(query, vals, function (err, res) {
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
};

// uuid - Workflow.uuid
// callback - f(err, workflow)
WorkflowPgBackend.prototype.getWorkflow = function (uuid, callback) {
  var self = this,
      workflow = null,
      row = null,
      query = 'SELECT * FROM wf_workflows WHERE uuid=$1',
      vals = [uuid];

  self._execQuery(query, vals, function (err, res) {
    if (err) {
      return callback(new wf.BackendInternalError(err.Error));
    } else {
      if (res.rows.length === 0) {
        return callback(new wf.BackendResourceNotFoundError(sprintf(
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
};


// workflow - update workflow object.
// callback - f(err, workflow)
WorkflowPgBackend.prototype.updateWorkflow = function (workflow, callback) {
  var self = this,
      keys = [],
      idx = 0,
      val_places = [],
      vals = [],
      query;

  // TODO: A good place to verify that the same tasks are not on the chain
  // and into the onerror callback (GH-1).

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
  query = 'UPDATE wf_workflows SET (' + keys.join(',') +
    ')=(' + val_places.join(',') + ') WHERE uuid=$' + (idx + 1);

  self._execQuery(query, vals, function (err, res) {
    if (err) {
      return callback(new wf.BackendInternalError(err.Error));
    } else {
      return callback(null, workflow);
    }
  });
};


// workflow - the workflow object
// callback - f(err, boolean)
WorkflowPgBackend.prototype.deleteWorkflow = function (workflow, callback) {
  var self = this,
      query = 'DELETE FROM wf_workflows WHERE uuid=$1 RETURNING *',
      vals = [workflow.uuid];

  self._execQuery(query, vals, function (err, result) {
    if (err) {
      return callback(new wf.BackendInternalError(err));
    } else {
      return callback(null, (result.rows.length === 1));
    }
  });
};


// Get all the workflows:
// - callback - f(err, workflows)
WorkflowPgBackend.prototype.getWorkflows = function (callback) {
  var self = this,
      workflows = [],
      query = 'SELECT * FROM wf_workflows';

  self._execQuery(query, function (err, res) {
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
};


// job - Job object
// callback - f(err, job)
WorkflowPgBackend.prototype.createJob = function (job, callback) {
  var self = this,
      keys = Object.keys(job),
      idx = 0,
      val_places = [],
      vals = [],
      query;

  keys.forEach(function (p) {
    idx += 1;
    if (typeof (job[p]) === 'object' && job[p] !== null) {
      vals.push(JSON.stringify(job[p]));
    } else {
      vals.push(job[p]);
    }
    val_places.push('$' + idx);
  });

  query = 'INSERT INTO wf_jobs(' +
    keys.join(', ') + ') VALUES (' +
    val_places.join(', ') + ')';

  self._execQuery(query, vals, function (err, res) {
    if (err) {
      return callback(new wf.BackendInternalError(err.Error));
    } else {
      return callback(null, job);
    }

  });
};

// uuid - Job.uuid
// callback - f(err, job)
WorkflowPgBackend.prototype.getJob = function (uuid, callback) {
  var self = this,
      job = null,
      query = 'SELECT * FROM wf_jobs WHERE uuid=$1',
      vals = [uuid];

  self._execQuery(query, vals, function (err, res) {
    if (err) {
      return callback(new wf.BackendInternalError(err.Error));
    } else {
      if (res.rows.length === 0) {
        return callback(new wf.BackendResourceNotFoundError(sprintf(
          'Workflow with uuid \'%s\' does not exist', uuid)));
      } else {
        job = self._decodeJob(res.rows[0]);
        return callback(null, job);
      }
    }
  });
};


// Get a single job property
// uuid - Job uuid.
// prop - (String) property name
// cb - callback f(err, value)
WorkflowPgBackend.prototype.getJobProperty = function (uuid, prop, cb) {
  var self = this,
      value = null,
      encoded_props = ['chain', 'chain_results', 'onerror', 'onerror_results',
      'params', 'info'],
      query = 'SELECT (' + prop + ') FROM wf_jobs WHERE uuid=$1',
      vals = [uuid];

  self._execQuery(query, vals, function (err, res) {
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
};

// job - the job object
// callback - f(err) called with error in case there is a duplicated
// job with the same target and same params
WorkflowPgBackend.prototype.validateJobTarget = function (job, callback) {
  // If no target is given, we don't care:
  if (!job.target) {
    return callback(null);
  }

  var self = this,
      query = 'SELECT COUNT(*) FROM wf_jobs WHERE ' +
      'workflow_uuid=$1 AND target=$2 AND params=$3 AND ' +
      'execution=\'queued\' OR execution=\'running\'',
      vals = [job.workflow_uuid, job.target, JSON.stringify(job.params)];


  return self._execQuery(query, vals, function (err, res) {
    if (err) {
      return callback(new wf.BackendInternalError(err.Error));
    } else {
      if (res.rows.length !== 0 && res.rows[0].count !== 0) {
        return callback(new wf.BackendInvalidArgumentError(
        'Another job with the same target' +
        ' and params is already queued'));
      } else {
        return callback(null);
      }
    }
  });
};


// Get the next queued job.
// index - Integer, optional. When given, it'll get the job at index position
//         (when not given, it'll return the job at position zero).
// callback - f(err, job)
WorkflowPgBackend.prototype.nextJob = function (index, callback) {
  var self = this,
      job = null,
      query;

  if (typeof (index) === 'function') {
    callback = index;
    index = 0;
  }

  query = 'SELECT * FROM wf_jobs WHERE execution=\'queued\' ORDER BY ' +
    'created_at ASC LIMIT 1 OFFSET ' + index;

  self._execQuery(query, function (err, res) {
    if (err) {
      return callback(new wf.BackendInternalError(err.Error));
    } else {
      if (res.rows.length !== 0) {
        job = self._decodeJob(res.rows[0]);
      }
      return callback(null, job);
    }
  });
};


// Lock a job, mark it as running by the given runner, update job status.
// uuid - the job uuid (String)
// runner_id - the runner identifier (String)
// callback - f(err, job) callback will be called with error if something
//            fails, otherwise it'll return the updated job using getJob.
WorkflowPgBackend.prototype.runJob = function (uuid, runner_id, callback) {
  var self = this,
      error = null,
      job = null,
      query,
      vals;

  try {
    self.client.query('BEGIN');
    query = 'SELECT * FROM wf_jobs WHERE uuid=$1 AND ' +
      'runner_id IS NULL AND execution=\'queued\' FOR UPDATE NOWAIT';
    vals = [uuid];
    self.log.debug({query: query, vals: vals});
    self.client.query(query, vals, function (err, res) {
      if (err) {
        self.log.error({error: err});
        throw new Error(err.Error);
      }
      if (res.rows.length === 0) {
        error = new wf.BackendPreconditionFailedError(sprintf(
          'Job with uuid \'%s\' is not queued', uuid));
      }
    });

    query = 'UPDATE wf_jobs SET (runner_id, execution)=($1, \'running\') ' +
      'WHERE uuid=$2 AND execution=\'queued\' AND runner_id IS NULL ' +
      'RETURNING *';
    vals = [runner_id, uuid];
    self.log.debug({query: query, vals: vals});
    self.client.query(query, vals, function (err, res) {
      if (err) {
        self.log.error({error: err});
        throw err;
      }
      if (res.rows.length === 0) {
        error = new wf.BackendPreconditionFailedError(sprintf(
          'Unable to lock job \'%s\'', uuid));
      } else {
        job = res.rows[0];
      }
    });

    return self.client.query('COMMIT', function () {
      if (job) {
        return callback(null, self._decodeJob(job));
      } else {
        return callback(error);
      }
    });
  } catch (e) {
    error = e.message;
    return callback(new wf.BackendInternalError(error));
  }
};

// Unlock the job, mark it as finished, update the status, add the results
// for every job's task.
// job - the job object. It'll be saved to the backend with the provided
//       properties.
// callback - f(err, job) callback will be called with error if something
//            fails, otherwise it'll return the updated job using getJob.
WorkflowPgBackend.prototype.finishJob = function (job, callback) {
  var self = this,
      query = 'SELECT * FROM wf_jobs WHERE uuid=$1 AND ' +
    'execution=\'running\' OR execution=\'canceled\'',
    vals = [job.uuid];

  self._execQuery(query, vals, function (err, res) {
    if (err) {
      return callback(new wf.BackendInternalError(err.Error));
    } else if (res.rows.length === 0) {
      return new wf.BackendPreconditionFailedError(callback(
        'Only running jobs can be finished'));
    } else {
      if (job.execution === 'running') {
        job.execution = 'succeeded';
      }
      job.runner_id = null;
      return self.updateJob(job, callback);
    }
  });
};


// Update the job while it is running with information regarding progress
// job - the job object. It'll be saved to the backend with the provided
//       properties.
// callback - f(err, job) callback will be called with error if something
//            fails, otherwise it'll return the updated job using getJob.
WorkflowPgBackend.prototype.updateJob = function (job, callback) {
  var self = this,
      keys = [],
      idx = 0,
      val_places = [],
      vals = [],
      theJob = null,
      query,
      sql_props = [
        'name', 'uuid', 'chain', 'onerrror', 'chain_results',
        'onerror_results', 'execution', 'workflow_uuid', 'target',
        'params', 'info', 'exec_after', 'created_at', 'runner_id',
        'timeout'
      ];

  Object.keys(job).forEach(function (p) {
    if (p === 'uuid' || sql_props.indexOf(p) < 0) {
      return;
    }
    idx += 1;
    keys.push(p);
    if (typeof (job[p]) === 'object' && job[p] !== null) {
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

  self._execQuery(query, vals, function (err, result) {
    if (err) {
      return callback(new wf.BackendInternalError(err.Error));
    } else {
      theJob = self._decodeJob(result.rows[0]);
      Object.keys(job).forEach(function (p) {
        if (!theJob[p]) {
          theJob[p] = job[p];
        }
      });
      return callback(null, theJob);
    }
  });
};


// Update only the given Job property. Intendeed to prevent conflicts with
// two sources updating the same job at the same time, but different properties
// uuid - the job's uuid
// prop - the name of the property to update
// val - value to assign to such property
// callback - f(err) called with error if something fails, otherwise with null.
WorkflowPgBackend.prototype.updateJobProperty = function (
  uuid,
  prop,
  val,
  callback)
{

  var self = this,
      query = 'UPDATE wf_jobs SET (' + prop + ')=($1) WHERE uuid=$2',
      vals;

  if (typeof (val) === 'object' && val !== null) {
    val = JSON.stringify(val);
  }

  vals = [val, uuid];
  self._execQuery(query, vals, function (err, res) {
    if (err) {
      return callback(new wf.BackendInternalError(err.Error));
    } else {
      return callback(null);
    }
  });
};




// Queue a job which has been running; i.e, due to whatever the reason,
// re-queue the job. It'll unlock the job, update the status, add the
// results for every finished task so far ...
// job - the job Object. It'll be saved to the backend with the provided
//       properties to ensure job status persistence.
// callback - f(err, job) callback will be called with error if something
//            fails, otherwise it'll return the updated job using getJob.
WorkflowPgBackend.prototype.queueJob = function (job, callback) {
  var self = this,
      query = 'SELECT * FROM wf_jobs WHERE uuid=$1 AND execution=\'running\'',
      vals = [job.uuid];

  self._execQuery(query, vals, function (err, res) {
    if (err) {
      return callback(new wf.BackendInternalError(err.Error));
    } else if (res.rows.length === 0) {
      return new wf.BackendPreconditionFailedError(callback(
        'Only running jobs can be queued'));
    } else {
      job.execution = 'queued';
      job.runner_id = null;
      return self.updateJob(job, callback);
    }
  });
};



// Get the given number of queued jobs uuids.
// - start - Integer - Position of the first job to retrieve
// - stop - Integer - Position of the last job to retrieve, _included_
// - callback - f(err, jobs)
WorkflowPgBackend.prototype.nextJobs = function (start, stop, callback) {
  var self = this,
      jobs = [],
      index = start,
      limit = (stop - start) + 1,
      query = 'SELECT uuid FROM wf_jobs WHERE execution=\'queued\' ' +
              'ORDER BY created_at ASC LIMIT ' + limit + ' OFFSET ' + index;

  self._execQuery(query, function (err, res) {
    if (err) {
      return callback(new wf.BackendInternalError(err.Error));
    } else {
      res.rows.forEach(function (row) {
        jobs.push(row.uuid);
      });
      return callback(null, jobs);
    }
  });
};



// Register a runner on the backend and report it's active:
// - runner_id - String, unique identifier for runner.
// - active_at - ISO String timestamp. Optional. If none is given, current time
// - callback - f(err)
WorkflowPgBackend.prototype.registerRunner = function (
  runner_id,
  active_at,
  callback
) {
  var self = this,
      query = 'UPDATE wf_runners SET (active_at)=($1) ' +
              'WHERE uuid=$2 RETURNING *',
      vals;

  if (typeof (active_at) === 'function') {
    callback = active_at;
    active_at = new Date().toISOString();
  }
  vals = [active_at, runner_id];
  self._execQuery(query, vals, function (err, res) {
    if (err) {
      return callback(new wf.BackendInternalError(err.Error));
    }
    if (res.rows.length !== 1) {
      query = 'INSERT INTO wf_runners (active_at, uuid) VALUES ($1, $2)';
      return self._execQuery(query, vals, function (err, res) {
        if (err) {
          return callback(new wf.BackendInternalError(err.Error));
        } else {
          return callback(null);
        }
      });
    } else {
      return callback(null);
    }
  });

  
};


// Report a runner remains active:
// - runner_id - String, unique identifier for runner. Required.
// - active_at - ISO String timestamp. Optional. If none is given, current time
// - callback - f(err)
WorkflowPgBackend.prototype.runnerActive = function (
  runner_id,
  active_at,
  callback
) {
  var self = this;
  return self.registerRunner(runner_id, active_at, callback);
};

// Get the given runner id details
// - runner_id - String, unique identifier for runner. Required.
// - callback - f(err, runner)
WorkflowPgBackend.prototype.getRunner = function (runner_id, callback) {
  var self = this,
      runner = null,
      query = 'SELECT * FROM wf_runners WHERE uuid=$1',
      vals = [runner_id];

  self._execQuery(query, vals, function (err, res) {
    if (err) {
      return callback(new wf.BackendInternalError(err.Error));
    } else {
      if (res.rows.length === 0) {
        return callback(new wf.BackendResourceNotFoundError(sprintf(
          'WorkflowRunner with uuid \'%s\' does not exist', runner_id)));
      } else {
        runner = res.rows[0];
        return callback(null, runner.active_at);
      }
    }
  });
};


// Get all the registered runners:
// - callback - f(err, runners)
WorkflowPgBackend.prototype.getRunners = function (callback) {
  var self = this,
      runners = {},
      query = 'SELECT * FROM wf_runners';

  self._execQuery(query, function (err, res) {
    if (err) {
      return callback(new wf.BackendInternalError(err.Error));
    } else {
      res.rows.forEach(function (row) {
        runners[row.uuid] = row.active_at;
      });
      return callback(null, runners);
    }
  });
};

// Set a runner as idle:
// - runner_id - String, unique identifier for runner
// - callback - f(err)
WorkflowPgBackend.prototype.idleRunner = function (runner_id, callback) {
  var self = this,
      query = 'UPDATE wf_runners SET (idle)=(TRUE) ' +
              'WHERE uuid=$1 RETURNING *',
      vals = [runner_id];

  self._execQuery(query, vals, function (err, res) {
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
};

// Check if the given runner is idle
// - runner_id - String, unique identifier for runner
// - callback - f(boolean)
WorkflowPgBackend.prototype.isRunnerIdle = function (runner_id, callback) {
  var self = this,
      is_idle = false,
      query = 'SELECT * FROM wf_runners WHERE uuid=$1',
      vals = [runner_id];

  self._execQuery(query, vals, function (err, res) {
    if (res.rows.length !== 0) {
      is_idle = res.rows[0].idle;
    }
    return (err || is_idle) ? callback(true) : callback(false);
  });
};

// Remove idleness of the given runner
// - runner_id - String, unique identifier for runner
// - callback - f(err)
WorkflowPgBackend.prototype.wakeUpRunner = function (runner_id, callback) {
  var self = this,
      query = 'UPDATE wf_runners SET (idle)=(FALSE) ' +
              'WHERE uuid=$1 RETURNING *',
      vals = [runner_id];

  self._execQuery(query, vals, function (err, res) {
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
};

// Get all jobs associated with the given runner_id
// - runner_id - String, unique identifier for runner
// - callback - f(err, jobs). `jobs` is an array of job's UUIDs.
//   Note `jobs` will be an array, even when empty.
WorkflowPgBackend.prototype.getRunnerJobs = function (runner_id, callback) {
  var self = this,
      jobs = [],
      query = 'SELECT uuid FROM wf_jobs WHERE runner_id=$1',
      vals = [runner_id];

  self._execQuery(query, vals, function (err, res) {
    if (err) {
      return callback(new wf.BackendInternalError(err.Error));
    } else {
      res.rows.forEach(function (row) {
        jobs.push(row.uuid);
      });
      return callback(null, jobs);
    }
  });
};


// Get all the jobs:
// - execution - String, the execution status for the jobs to return.
//               Return all jobs if no execution status is given.
// - callback - f(err, jobs)
WorkflowPgBackend.prototype.getJobs = function (execution, callback) {
  var self = this,
      jobs = [],
      executions = ['queued', 'failed', 'succeeded', 'canceled', 'running'],
      query = 'SELECT * FROM wf_jobs';

  if (typeof (execution) === 'function') {
    callback = execution;
  } else if (executions.indexOf(execution !== -1)) {
    query += ' WHERE execution=\'' + execution + '\'';
  } else {
    return callback(new wf.BackendInvalidArgumentError(
      'excution is required and must be one of' +
      '"queued", "failed", "succeeded", "canceled", "running"'));
  }

  return self._execQuery(query, function (err, res) {
    if (err) {
      return callback(new wf.BackendInternalError(err.Error));
    } else {
      res.rows.forEach(function (row) {
        jobs.push(self._decodeJob(row));
      });
      return callback(null, jobs);
    }
  });
};


// Add progress information to an existing job:
// - uuid - String, the Job's UUID.
// - info - Object, {'key' => 'Value'}
// - callback - f(err)
WorkflowPgBackend.prototype.addInfo = function (uuid, info, callback) {
  var self = this,
      existing_info = null,
      query = 'SELECT * FROM wf_jobs WHERE uuid=$1',
      vals = [uuid];

  self._execQuery(query, vals, function (err, res) {
    if (err) {
      return callback(new wf.BackendInternalError(err.Error));
    } else if (res.rows.length === 0) {
      return callback(new wf.BackendResourceNotFoundError(
        'Job does not exist. Cannot Update.'));
    } else {
      existing_info = JSON.parse(res.rows[0].info) || [];
      existing_info.push(info);
      return self.updateJobProperty(
        uuid,
        'info',
        existing_info,
        callback);
    }
  });
};


// Get progress information from an existing job:
// - uuid - String, the Job's UUID.
// - callback - f(err, info)
WorkflowPgBackend.prototype.getInfo = function (uuid, callback) {
  var self = this,
      query = 'SELECT info FROM wf_jobs WHERE uuid=$1',
      vals = [uuid];

  self._execQuery(query, vals, function (err, res) {
    if (err) {
      return callback(new wf.BackendInternalError(err.Error));
    } else if (res.rows.length === 0) {
      return callback(new wf.BackendResourceNotFoundError(
        'Job does not exist. Cannot get info.'));
    } else {
      return callback(null, (JSON.parse(res.rows[0].info) || []));
    }
  });

};



WorkflowPgBackend.prototype._tableExists = function (table_name, callback) {
  var self = this,
      query = 'select count (*) from' +
              ' information_schema.tables where table_name=$1',
      vals = [table_name];

  return self._execQuery(query, vals, function (err, res) {
    if (err || res.rows[0].count === 0) {
      return callback(false);
    } else {
      return callback(true);
    }
  });
};




// Create required tables when needed:
// - callback - f(err)
WorkflowPgBackend.prototype._createTables = function (callback) {
  var self = this,
      series = {
    wf_workflows: function (cbk) {
      return self._tableExists('wf_workflows', function (exists) {
        if (!exists) {
          var q = 'CREATE TABLE wf_workflows(' +
            'name VARCHAR(255) UNIQUE NOT NULL, ' +
            'uuid UUID PRIMARY KEY, ' +
            'chain TEXT,' +
            'onerror TEXT,' +
            'timeout INTEGER)';

          return self._execQuery(q, function (err, res) {
            if (err) {
              return cbk(err);
            } else {
              return cbk(null, true);
            }
          });
        } else {
          return cbk(null, false);
        }
      });
    },
    wf_jobs: function (cbk) {
      return self._tableExists('wf_jobs', function (exists) {
        if (!exists) {
          var q = 'CREATE TABLE wf_jobs(' +
            'name VARCHAR(255) NOT NULL, ' +
            'uuid UUID PRIMARY KEY, ' +
            'chain TEXT NOT NULL,' +
            'onerror TEXT,' +
            'chain_results TEXT,' +
            'onerror_results TEXT, ' +
            'execution VARCHAR(32) NOT NULL DEFAULT \'queued\' , ' +
            'workflow_uuid UUID NOT NULL, ' +
            'target VARCHAR(255), ' +
            'params TEXT, ' +
            'info TEXT, ' +
            'exec_after TIMESTAMP WITH TIME ZONE, ' +
            'created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(), ' +
            'runner_id UUID DEFAULT NULL, ' +
            'timeout INTEGER)',
            idx = [
              'CREATE INDEX idx_wf_jobs_execution ON wf_jobs (execution)',
              'CREATE INDEX idx_wf_jobs_target ON wf_jobs (target)',
              'CREATE INDEX idx_wf_jobs_params ON wf_jobs (params)',
              'CREATE INDEX idx_wf_jobs_exec_after ON wf_jobs (exec_after)'
            ];

          return self._execQuery(q, function (err, res) {
            if (err) {
              return cbk(err);
            } else {
              return async.forEachSeries(idx, function (query, cb) {
                self._execQuery(query, function (err, res) {
                  if (err) {
                    return cb(err);
                  } else {
                    return cb(null);
                  }
                });
              }, function (err) {
                if (err) {
                  return cbk(err);
                } else {
                  return cbk(null, true);
                }
              });
            }
          });

        } else {
          return cbk(null, false);
        }
      });
    },
    wf_runners: function (cbk) {
      return self._tableExists('wf_runners', function (exists) {
        if (!exists) {
          var q = 'CREATE TABLE wf_runners(' +
            'uuid UUID PRIMARY KEY, ' +
            'active_at TIMESTAMP WITH TIME ZONE, ' +
            'idle BOOLEAN NOT NULL DEFAULT FALSE)',
            idx = [
            'CREATE INDEX idx_wf_runners_exec_after ON wf_runners (active_at)',
            'CREATE INDEX idx_wf_runners_runner_id ON wf_jobs (runner_id)'
            ];
          return self._execQuery(q, function (err, res) {
            if (err) {
              return cbk(err);
            } else {
              return async.forEachSeries(idx, function (query, cb) {
                self._execQuery(query, function (err, res) {
                  if (err) {
                    return cb(err);
                  } else {
                    return cb(null);
                  }
                });
              }, function (err) {
                if (err) {
                  return cbk(err);
                } else {
                  return cbk(null, true);
                }
              });
            }
          });
        } else {
          return cbk(null, false);
        }
      });
    }
      };
  async.series(series, function (err, results) {
    if (err) {
      return callback(err);
    } else {
      return callback(null);
    }
  });

};

// Return all the JSON.stringified job properties decoded back to objects
// - job - (object) raw job from postgres to decode
WorkflowPgBackend.prototype._decodeJob = function (job) {
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
  if (job.params) {
    job.params = JSON.parse(job.params);
  }
  if (job.info) {
    job.info = JSON.parse(job.info);
  }
  return job;
};

WorkflowPgBackend.prototype._execQuery = function (query, vals, callback) {
  var self = this,
      cb;

  if (typeof (vals) === 'function') {
    callback = vals;
    vals = null;
  }

  cb = function (err, res) {
    if (err) {
      self.log.error({error: err});
      return callback(err);
    } else {
      return callback(null, res);
    }
  };

  if (vals) {
    self.log.debug({query: query, vals: vals});
    return self.client.query(query, vals, cb);
  } else {
    self.log.debug({query: query});
    return self.client.query(query, cb);
  }
};
