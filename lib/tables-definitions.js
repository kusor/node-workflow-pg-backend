// Copyright 2013 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.

module.exports = {
    tables: {
        wf_workflows: [
            'CREATE TABLE wf_workflows(' +
            'name VARCHAR(255) UNIQUE NOT NULL, ' +
            'uuid UUID PRIMARY KEY, ' +
            'chain TEXT,' +
            'onerror TEXT,' +
            'max_attempts INTEGER,' +
            'chain_md5 VARCHAR(255),' +
            'onerror_md5 VARCHAR(255),' +
            'timeout INTEGER)'
        ],
        wf_jobs: [
            'CREATE TABLE wf_jobs(' +
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
            'exec_after TIMESTAMP WITH TIME ZONE, ' +
            'created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(), ' +
            'runner_id UUID DEFAULT NULL, ' +
            'num_attempts INTEGER,' +
            'max_attempts INTEGER,' +
            'timeout INTEGER)',
            'CREATE INDEX idx_wf_jobs_execution ON wf_jobs (execution)',
            'CREATE INDEX idx_wf_jobs_target ON wf_jobs (target)',
            'CREATE INDEX idx_wf_jobs_exec_after ON wf_jobs (exec_after)'
        ],
        wf_runners: [
            'CREATE TABLE wf_runners(' +
            'uuid UUID PRIMARY KEY, ' +
            'active_at TIMESTAMP WITH TIME ZONE, ' +
            'idle BOOLEAN NOT NULL DEFAULT FALSE)',
            'CREATE INDEX idx_wf_runners_exec_after ON wf_runners (active_at)',
            'CREATE INDEX idx_wf_runners_runner_id ON wf_jobs (runner_id)'
        ],
        wf_jobs_info: [
            'CREATE TABLE wf_jobs_info(' +
            'job_uuid UUID PRIMARY KEY, ' +
            'created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(), ' +
            'info TEXT)'
        ],
        wf_locked_targets: [
            'CREATE TABLE wf_locked_targets(' +
            'job_uuid UUID PRIMARY KEY, ' +
            'target VARCHAR(255) NOT NULL)'
        ]
    }
};
