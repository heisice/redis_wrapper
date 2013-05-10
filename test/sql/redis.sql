
-- our connection handle is 1, the redis db is number 15 on localhost
select redis_connect(1, con_db:=15);

-- make sure the db is empty

\set ON_ERROR_STOP
do $$
  declare
    retval text;
  begin 
    select into retval redis_command(1,'KEYS *');
    if retval != '{}'
    then
       raise EXCEPTION 'db 15 not empty';
    end if;
  end;
$$;
\unset ON_ERROR_STOP

-- basic set and get
select redis_command(1,'SET %s %s','string_key','foo');
select redis_command(1,'SET %s %s','int_key','1');
select redis_command(1,'INCRBY %s %s','int_key','5');
select redis_command(1,'GET %s','string_key');
select redis_command(1,'GET %s','int_key');

-- redis_command_argv lets you use as many args as you like
select redis_command_argv(1,'HMSET','foo:'||id,
            'data1','val '|| id+99,
            'data2', 'val ' || id+299) 
    from generate_series(0,9) as id;

-- look at what we just put in redis
select 'foo:' || id as key, 
       (redis_command(1,'HGETALL %s','foo:'||id)::text[]) as vals
    from generate_series(0,9) as id;

-- prepare to push records
create temp table abc (id int, id2 int, t text);
insert into abc 
   select id, 99-id, 'blurfl ' || (157 - id)
   from generate_series(0,9) as id;

-- push the records as a table with a key set
select count(*) from (
   select redis_push_record(1,r,true,'abcset','abc','{id,id2}') from abc r) a;

-- select the data from the pushed table
-- normally you would use the FDW to do this more naturally
with keys as
(
        select unnest(redis_command(1,'SMEMBERS abcset')::text[]) as key
        order by 1
)
select key, redis_command(1,'HGETALL %s',key)::text[] as vals 
from keys;

-- drop the table
select redis_drop_table(1,key_set:='abcset');

-- show that the keyset is gone
select redis_command(1,'SMEMBERS abcset');

-- and the table hashes too
select redis_command(1,'KEYS abc:*');

--clean up
select redis_command(1,'FLUSHALL');

select redis_disconnect(1);

drop table abc;

