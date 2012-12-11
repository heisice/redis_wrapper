# PostgreSQL Redis Extension

### Building and installing

    PATH=/path/to/postgresql/bin:$PATH make USE_PGXS=1
    PATH=/path/to/postgresql/bin:$PATH make USE_PGXS=1 install

### Requirements

hiredis client library. On Fedora this is the hiredis and hiredis-devel packages.

### Example Use

    create extension redis;
    -- connect to default local redis server
    select redis_connect(0);
    -- add a key, uses connection number from redis_connect
    select redis_command(0,'SET %s %s','a.key','a.value');
    -- add a bunch of values
    select redis_command_argv(0,'MSET','key1','val1','key2','val2','key3','val3');
	-- disconnect
    select redis_disconnect(0);

### Functions

* redis_connect
  * required param: con_num  - connection number, allowed range 0 .. 15
  * optional param: con_host - IP address or host name - default '127.0.0.1'
  * optional param: con_port - port number - default 6379
  * optional param: con_pass - authentication password - default ''
  * optional param: con_db   - database id - default 0
  * returns: void
* redis_disconnect
  * required param: con_num - connection number from redis_connect
  * returns: void
* redis_command
  * required param: con_num - connection number from redis_connect
  * required param: command - command strings as used in hiredisCommand(), see https://github.com/redis/hiredis/blob/master/README.md
  * optional params:  0 to 4 params as required by command string
  * returns: string (status or value or values, as appropriate)
* redis_command_argv
  * required param: con_num - connection number from redis_connect
  * required param: command - any redis command name
  * optional params: any number of params as allowed by the command 
  * returns: string (status or value or values, as appropriate)


### To Do:

* regression tests
* full docco
* return proper postgres array strings for array returns 
* array-returning versions of redis_command and redis_command_argv
* set-returning versions of redis_command and redis_command_argv

### Issues

* Hiredis API doc says "Redis may reply with nested arrays but this is fully supported." Presumably this is for handling returns of lists, sets and zsets. How would we handle this when the dimensions aren't uniform? 
* Also, can these be nested more than two deep? If so, when?

    
