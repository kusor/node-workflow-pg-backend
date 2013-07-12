// Copyright 2013 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.

var path = require('path');
var fs = require('fs');

var cfg = path.resolve(__dirname, './config.json');
var cfg_file = fs.existsSync(cfg) ? cfg :
               path.resolve(__dirname, './config.json.sample');
var config;

module.exports = {
    config: function () {
        if (!config) {
            config = JSON.parse(fs.readFileSync(cfg_file, 'utf-8'));
        }
        return config;
    }
};
