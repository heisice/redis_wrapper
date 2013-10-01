#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#if PG_VERSION_NUM >= 90300
#include "access/htup_details.h"
#endif
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "nodes/nodes.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#include <hiredis/hiredis.h>
#include <stdarg.h>

PG_MODULE_MAGIC;

#define NUM_REDIS_CONTEXTS 16

static redisContext *contexts[NUM_REDIS_CONTEXTS];


static char *get_reply_array(redisReply *reply);
static void delete_members(redisContext *ctx, redisReply *reply);


static inline void 
check_reply(int con_num, redisContext *ctx, redisReply *reply, char * msg)
{

	if (reply == NULL)
	{
			char *ctxerr = pstrdup(ctx->errstr);
			redisFree(ctx);
			contexts[con_num] = NULL;
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE), /* ??? */
					 errmsg("%s: %s",msg,ctxerr)));

	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		char *reperr = pstrdup(reply->str);
		freeReplyObject(reply);
		/* the context is still valid, we just had a command failure */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), /* ??? */
				 errmsg("%s: %s:",msg,reperr)));
	}


}

PG_FUNCTION_INFO_V1(redis_connect);

extern Datum redis_connect(PG_FUNCTION_ARGS);

Datum
redis_connect(PG_FUNCTION_ARGS)
{
	int con_num = PG_GETARG_INT32(0);
	text *con_host = PG_GETARG_TEXT_P(1);
	int con_port = PG_GETARG_INT32(2);
	text *con_pass = PG_GETARG_TEXT_P(3);
	int con_db = PG_GETARG_INT32(4);
	bool ignore_dup = PG_GETARG_BOOL(5);

	char *chost = text_to_cstring(con_host);
	char *cpass  = text_to_cstring(con_pass);
	redisContext *ctx;
	redisReply *reply;

	if (con_num < 0 || con_num >= NUM_REDIS_CONTEXTS)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("con_num must be between 0 and %d", 
						NUM_REDIS_CONTEXTS-1)));

	if (contexts[con_num] != NULL)
	{
		if (ignore_dup)
			PG_RETURN_VOID();

        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("connection number %d is already open", con_num)));
	}

	if ((ctx = redisConnect(chost, con_port)) == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), /* ??? */
                 errmsg("failed to connect to redis at %s:%d", 
						chost, con_port)));

	if (strlen(cpass) > 0)
	{
		reply = redisCommand(ctx,"AUTH %s",cpass);
		check_reply(con_num, ctx, reply, "authenitcation failure");
	}

	if (con_db != 0)
	{
		char arg[32];
		
		snprintf(arg,32, "%d", con_db);

		reply = redisCommand(ctx,"SELECT %s",arg);
		check_reply(con_num, ctx, reply, "selecting db failure");
	}

	/* everything is kosher, so save the connection */
	contexts[con_num] = ctx;

	PG_RETURN_VOID();
}


PG_FUNCTION_INFO_V1(redis_disconnect);

extern Datum redis_disconnect(PG_FUNCTION_ARGS);

Datum
redis_disconnect(PG_FUNCTION_ARGS)
{
	int con_num = PG_GETARG_INT32(0);

	if (con_num < 0 || con_num >= NUM_REDIS_CONTEXTS)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("con_num must be between 0 and %d", 
						NUM_REDIS_CONTEXTS-1)));

	if (contexts[con_num] == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("connection number %d is not open", con_num)));

	redisFree(contexts[con_num]);
	contexts[con_num] = NULL;

	PG_RETURN_VOID();
}


PG_FUNCTION_INFO_V1(redis_command);

extern Datum redis_command(PG_FUNCTION_ARGS);

Datum
redis_command(PG_FUNCTION_ARGS)
{
	int con_num = PG_GETARG_INT32(0);
	text *command = PG_GETARG_TEXT_P(1);
	ArrayType *inargs = PG_GETARG_ARRAYTYPE_P(2);

	char *cmd = text_to_cstring(command);

	redisReply *reply = NULL;
	redisContext *ctx;
	char * returnval = NULL;
	Datum      *textargs;
	bool       *argnulls;
	int         nargs;
	char      **args;
	int         i;

	char failmsg[1024];

	snprintf(failmsg, sizeof(failmsg),"command %s failed",cmd);

	deconstruct_array(inargs, TEXTOID, -1, false, 'i',
					  &textargs, &argnulls, &nargs);
	
	args = palloc(nargs * sizeof(char *));

	for (i = 0; i < nargs; i++)
	{
		if (argnulls[i])
			args[i] = NULL;
		else
			args[i] = TextDatumGetCString(textargs[i]);
	}

	if (con_num < 0 || con_num >= NUM_REDIS_CONTEXTS)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("con_num must be between 0 and %d", 
						NUM_REDIS_CONTEXTS-1)));

	if (contexts[con_num] == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("connection number %d is not open", con_num)));

	ctx = contexts[con_num];

	switch(nargs)
	{
		case 0: 
			reply = redisCommand(ctx,cmd); 
			break;
		case 1: 
			reply = redisCommand(ctx,cmd, args[0]); 
			break;
		case 2: 
			reply = redisCommand(ctx,cmd, args[0], args[1]); 
			break;
		case 3: 
			reply = redisCommand(ctx,cmd, args[0],args[1],args[2]); 
			break;
		case 4: 
			reply = redisCommand(ctx,cmd, args[0],args[1],args[2],args[3]); 
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unsupported number of command parameters: %d (can have 0 - 4)", nargs),
					 errdetail("You might need to use redis_command_argv() instead.")));
	}

	check_reply(con_num, ctx, reply,failmsg);

	switch (reply->type)
	{
		case REDIS_REPLY_STATUS:
		case REDIS_REPLY_STRING:
			pg_verifymbstr(reply->str, reply->len, false);
			returnval = pstrdup(reply->str);
			break;
		case REDIS_REPLY_INTEGER:
			returnval = palloc(32);
			snprintf(returnval,32,"%lld",reply->integer);
			break;
		case REDIS_REPLY_NIL:
			returnval = "nil";
		case REDIS_REPLY_ARRAY:
			returnval = get_reply_array(reply);
			break;
		default:
			break;
	}

	PG_RETURN_TEXT_P(CStringGetTextDatum(returnval));

}

PG_FUNCTION_INFO_V1(redis_command_argv);

extern Datum redis_command_argv(PG_FUNCTION_ARGS);

Datum
redis_command_argv(PG_FUNCTION_ARGS)
{
	int con_num = PG_GETARG_INT32(0);
	ArrayType *inargs = PG_GETARG_ARRAYTYPE_P(1);

	char *cmd;

	redisReply *reply = NULL;
	redisContext *ctx;
	char * returnval = NULL;
	Datum      *textargs;
	bool       *argnulls;
	int         nargs;
	char      **args;
	int         i;
	char failmsg[1024];

	deconstruct_array(inargs, TEXTOID, -1, false, 'i',
					  &textargs, &argnulls, &nargs);
	
	args = palloc(nargs * sizeof(char *));

	for (i = 0; i < nargs; i++)
	{
		if (argnulls[i])
			args[i] = ""; /* redisCommandArgv doesn't like NULL args */
		else
			args[i] = TextDatumGetCString(textargs[i]);
	}

	cmd = args[0];

	snprintf(failmsg, sizeof(failmsg),"command %s failed",cmd);

	if (con_num < 0 || con_num >= NUM_REDIS_CONTEXTS)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("con_num must be between 0 and %d", 
						NUM_REDIS_CONTEXTS-1)));

	if (contexts[con_num] == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("connection number %d is not open", con_num)));

	if (nargs <= 0 || strlen(cmd) == 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("command required")));
		

	ctx = contexts[con_num];

	reply = redisCommandArgv(ctx,nargs, (const char **) args, NULL);

	check_reply(con_num, ctx, reply, failmsg);

	switch (reply->type)
	{
		case REDIS_REPLY_STRING:
			pg_verifymbstr(reply->str, reply->len, false);
		case REDIS_REPLY_STATUS:
			/* assume status reply encoding is ok */
			returnval = pstrdup(reply->str);
			break;
		case REDIS_REPLY_INTEGER:
			returnval = palloc(32);
			snprintf(returnval,32,"%lld",reply->integer);
			break;
		case REDIS_REPLY_NIL:
			/* returnval = "nil"; */
			returnval = "";
			break;
		case REDIS_REPLY_ARRAY:
			returnval = get_reply_array(reply);
			break;
		default:
			break;
	}

	PG_RETURN_TEXT_P(CStringGetTextDatum(returnval));

}


/*
 * SQL function redis_push_table
 *
 * push the record with the given keyset and prefix as a hash record
 *
 */

PG_FUNCTION_INFO_V1(redis_push_record);

extern Datum redis_push_record(PG_FUNCTION_ARGS);

Datum
redis_push_record(PG_FUNCTION_ARGS)
{
	int con_num = PG_GETARG_INT32(0);
	Datum row = PG_GETARG_DATUM(1);
	bool push_keys = PG_GETARG_BOOL(2);
	text *keysetname = PG_GETARG_TEXT_P(3);
	text *prefix = PG_GETARG_TEXT_P(4);
	ArrayType *keys = PG_GETARG_ARRAYTYPE_P(5);

    Datum      *keytext;
    bool       *keynulls;
    char      **keystr;
	char      **keyvals;
	char       *keyset = NULL;
    int         nkeys;
	char      **args;
	int         argc;
	redisReply *reply = NULL;
	redisContext *ctx;
	int i,k;
    HeapTupleHeader td;
    Oid         tupType;
    int32       tupTypmod;
    TupleDesc   tupdesc;
    HeapTupleData tmptup,
               *tuple;

	StringInfoData key;

	char *cprefix = text_to_cstring(prefix);

	if (PG_ARGISNULL(0))
	{
		elog(ERROR,"must provide non-null redis connection");
	}
	if (PG_ARGISNULL(1))
	{
		elog(ERROR,"must provide non-null record");
	}
	if (PG_ARGISNULL(2))
	{
		push_keys = false;
	}
	if (! PG_ARGISNULL(3))
	{
		keyset = text_to_cstring(keysetname);
	}
	if (PG_ARGISNULL(4))
	{
		elog(ERROR,"must provide non-null table prefix");
	}
	if (PG_ARGISNULL(5))
	{
		elog(ERROR,"must provide non-null list of key names");
	}

	/* elog(NOTICE,"argnodes: %s", nodeToString(fcinfo->flinfo->fn_expr)); */

	if (con_num < 0 || con_num >= NUM_REDIS_CONTEXTS)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("con_num must be between 0 and %d", 
						NUM_REDIS_CONTEXTS-1)));

	if (contexts[con_num] == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("connection number %d is not open", con_num)));

	ctx = contexts[con_num];

    if (array_contains_nulls(keys))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot call %s with null key elements",
                        "redis_push_table")));

	
    deconstruct_array(keys, TEXTOID, -1, false, 'i',
                      &keytext, &keynulls, &nkeys);

	if (nkeys == 0)
		ereport(
			ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("cannot call %s with no key elements",
					"redis_push_table")));

	keystr = palloc(sizeof(char *) * nkeys);

    for (i = 0; i < nkeys; i++)
    {
        keystr[i] = TextDatumGetCString(keytext[i]);
        if (*keystr[i] == '\0')
            ereport(
                    ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("cannot call %s with empty key elements",
                            "redis_push_table")));
    }

	keyvals = palloc0(sizeof(char *) * nkeys);

    td = DatumGetHeapTupleHeader(row);

    /* Extract rowtype info and find a tupdesc */
    tupType = HeapTupleHeaderGetTypeId(td);
    tupTypmod = HeapTupleHeaderGetTypMod(td);
    tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

    /* Build a temporary HeapTuple control structure */
    tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
    tmptup.t_data = td;
    tuple = &tmptup;

	args = palloc(sizeof(char *) * (tupdesc->natts * 2 + 2));
	argc = 2; /* leave space for command name and key */
	
	for (i = 0; i < tupdesc->natts; i++)
	{
		Datum       val,
			origval;
		bool        isnull;
		char       *attname;
		Oid         typoutput;
		bool        typisvarlena;
		int keycol  = -1;
		char *outputstr = NULL;
		
		if (tupdesc->attrs[i]->attisdropped)
			continue;
		
		attname = NameStr(tupdesc->attrs[i]->attname);
		for (k = 0; k < nkeys; k++)
			if ( strcmp(attname, keystr[k]) == 0)
			{
				keycol = k;
				break;
			}

	    getTypeOutputInfo(tupdesc->attrs[i]->atttypid,
                          &typoutput, &typisvarlena);
		
		origval = heap_getattr(tuple, i + 1, tupdesc, &isnull);
		if (typisvarlena && !isnull)
			val = PointerGetDatum(PG_DETOAST_DATUM(origval));
		else
			val = origval;
	
		if (!isnull)
			outputstr = OidOutputFunctionCall(typoutput, val);

 		/* Clean up detoasted copy, if any */
		if (val != origval)
			pfree(DatumGetPointer(val));

		if (keycol >= 0)
		{
			if (isnull)
				ereport(
                    ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("cannot call %s with null key value (%s)",
                            "redis_push_table", attname)));

			keyvals[keycol] = outputstr;
		}
		if (keycol == 0 || push_keys)
		{
			args[argc++] = attname;
			args[argc++] = isnull ? "nil" : outputstr;
		}
	}

	ReleaseTupleDesc(tupdesc);
	
	initStringInfo(&key);
	appendStringInfoString(&key,cprefix);
	for (k = 0; k < nkeys; k++)
	{
		if (keyvals[k] == NULL)
			ereport(
				ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("key value for %s not found",keystr[k])));
		
		appendStringInfoChar(&key,':');
		appendStringInfoString(&key,keyvals[k]);
	}

	/* clean out any data we have for this key */
	reply = redisCommand(ctx, "DEL %s", key.data);

	check_reply(con_num, ctx, reply, "record delete failure");

	/* add the data */

	args[0] = "HMSET";
	args[1] = key.data;

	reply = redisCommandArgv(ctx, argc, (const char **) args, NULL);

	check_reply(con_num, ctx, reply,"record push failure");

	/* if we're keeping a keyset, add that too */

	if (keyset != NULL)
	{
		reply = redisCommand(ctx, "SADD %s %s", keyset, key.data);

		check_reply(con_num, ctx, reply, "keyset add failure");
	}

	PG_RETURN_NULL();
}


/*
 * SQL function redis_drop_table
 *
 * Drop the "table" designated by the given prefix or keyset
 * i.e. a redis set containing the keys of the table.
 * If the keyset is given, drop that too.
 *
 */

PG_FUNCTION_INFO_V1(redis_drop_table);

extern Datum redis_drop_table(PG_FUNCTION_ARGS);

Datum
redis_drop_table(PG_FUNCTION_ARGS)
{
	int con_num = PG_GETARG_INT32(0);

	redisReply *reply = NULL;
	redisContext *ctx;

	char * keysetname = NULL;
	char cmd[1024];

	if (PG_ARGISNULL(1) == PG_ARGISNULL(2))
	{
		elog(ERROR,"must have exactly one keyset or prefix argument not null");
	}

	if (con_num < 0 || con_num >= NUM_REDIS_CONTEXTS)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("con_num must be between 0 and %d", 
						NUM_REDIS_CONTEXTS-1)));

	if (contexts[con_num] == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("connection number %d is not open", con_num)));

	ctx = contexts[con_num];

	if (PG_ARGISNULL(2))
	{
		/* keyset case */
		keysetname = text_to_cstring(PG_GETARG_TEXT_P(1));
		
		snprintf(cmd, sizeof(cmd),"SMEMBERS %s",keysetname);
	}
	else
	{
		/* prefix case */
		char *prefixstr = text_to_cstring(PG_GETARG_TEXT_P(2));
		
		snprintf(cmd, sizeof(cmd),"KEYS %s*",prefixstr);
	}

	reply = redisCommand(ctx,cmd);
		
	if (reply->type == REDIS_REPLY_ERROR)
	{
		char *reperr = pstrdup(reply->str);
		freeReplyObject(reply);
		/* the context is still valid, we just had a command failure */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), /* ??? */
				 errmsg("command %s failed: %s:",
						cmd,reperr)));
	}
	
	switch (reply->type)
	{
		case REDIS_REPLY_ARRAY:
			delete_members(ctx,reply);
			break;
		default:
			elog(ERROR, "unexpected reply type for %s",cmd);
			break;
	}

	freeReplyObject(reply);

	if (keysetname != NULL)
	{
		/* if there's a keyset we delete it too */
		reply = redisCommand(ctx,"DEL %s",keysetname);

		if (reply->type == REDIS_REPLY_ERROR)
		{
			char *reperr = pstrdup(reply->str);
			freeReplyObject(reply);
			/* the context is still valid, we just had a command failure */
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE), /* ??? */
					 errmsg("command DEL %s failed: %s:",
							keysetname,reperr)));
		}
		
		freeReplyObject(reply);		
	}	
	PG_RETURN_VOID();
}


static void 
delete_members(redisContext *ctx, redisReply *reply)
{

	int i;

	for( i = 0; i < reply->elements; i++)
	{
		redisReply *ir = reply->element[i];
		redisReply *droprepl;
		switch (ir->type)
		{
			case REDIS_REPLY_STRING:
				droprepl = redisCommand(ctx,"DEL %s",ir->str);
				if (droprepl->type == REDIS_REPLY_ERROR)
				{
					char *reperr = pstrdup(reply->str);
					freeReplyObject(droprepl);
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE), /* ??? */
							 errmsg("command DEL %s failed: %s:",
									ir->str, reperr)));
				}
				
				break;
			default:
				elog(ERROR,"unexpected reply type");
				break;
		}
	}
}

/*
 * XXX need proper array quoting and nesting 
 */


static char *
get_reply_array(redisReply *reply)
{
	StringInfo res = makeStringInfo();
	int i;
	bool need_sep = false;

	appendStringInfoChar(res,'{');
	for (i = 0; i < reply->elements; i++)
	{
		redisReply *ir = reply->element[i];
		if (need_sep)
			appendStringInfoChar(res,',');
		need_sep = true;
		if (ir->type == REDIS_REPLY_ARRAY)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE), /* ??? */
					 errmsg("nested array returns not yet supported")));
		switch (ir->type)
		{
			case REDIS_REPLY_STATUS:
			case REDIS_REPLY_STRING:
				pg_verifymbstr(ir->str, ir->len, false);
				appendStringInfoString(res,ir->str);
				break;
			case REDIS_REPLY_INTEGER:
				appendStringInfo(res,"%lld",ir->integer);
				break;
			case REDIS_REPLY_NIL:
				/* appendStringInfoString(res,"nil"); */
				break;
			default:
				break;
		}
	}
	appendStringInfoChar(res,'}');
	
	return res->data;
}


