-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION redis" to load this file. \quit

CREATE FUNCTION redis_connect(con_num int, 
                              con_host text = '127.0.0.1', 
                              con_port int = 6379,
                              con_pass text = '',
                              con_db int  = 0,
                              ignore_duplicate bool = false)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION redis_disconnect(con_num int = 0)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION redis_command(
       con_num int, 
       command text, 
       variadic args text[] DEFAULT '{}')
RETURNS text
AS 'MODULE_PATHNAME' , 'redis_command'
LANGUAGE C;

CREATE FUNCTION redis_command_argv(con_num int, variadic args text[])
RETURNS text
AS 'MODULE_PATHNAME' , 'redis_command_argv'
LANGUAGE C;

CREATE FUNCTION redis_push_record(
       con_num       int,
       data          record,
       push_keys     boolean,
       key_set       text,
       key_prefix    text,
       key_fields    text[])
returns void
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION redis_drop_table(
       con_num       int,
       key_set       text DEFAULT NULL,
       key_prefix    text DEFAULT NULL)
returns void
AS 'MODULE_PATHNAME'
LANGUAGE C;








