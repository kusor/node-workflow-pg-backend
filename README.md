A backend for [node-workflow](http://kusor.github.com/node-workflow/) built
over [PostgreSQL](http://www.postgresql.org/).

Note this module uses JSON PostgreSQL data type. A version of PostgreSQL
supporting this datatype is thereby required (9.3+).

# Installation

npm install wf-pg-backend

# Usage

Add the following to the config file of your application using wf:

    {
      "backend": {
        "module": "wf-pg-backend",
        "opts": {
          "port": 5432,
          "host": "localhost",
          "database:": "node_workflow",
          "user": "postgres",
          "password": ""
        }
      }
    }

Where all the values are obviously related to your PostgreSQL server config.

If any of your configuration values matches the ones on the block
above, you don't need to specify them since those are the default values.

Please, note that this module will not try to create the `database` especified,
it must exists in order to be used by this module.

And that should be it. `wf` REST API and Runners should take care of
properly loading the module on init.


# TODO:
- Update types so everything requiring a JSON objects is of JSON type instead
  of TEXT.

# Issues

See [node-workflow issues](https://github.com/kusor/node-workflow/issues).

# LICENSE

The MIT License (MIT) Copyright (c) 2013 Pedro Palazón Candel

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
