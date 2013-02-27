#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
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
		if ((reply = redisCommand(ctx,"AUTH %s",cpass)) == NULL)
		{
			char *ctxerr = pstrdup(ctx->errstr);
			redisFree(ctx);
			ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), /* ??? */
                 errmsg("failed to authenticate to redis: %s:",ctxerr)));

		}
		else
		{
			if (reply->type == REDIS_REPLY_ERROR)
			{
				char *reperr = pstrdup(reply->str);
				freeReplyObject(reply);
				redisFree(ctx);
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE), /* ??? */
						 errmsg("failed to authenticate to redis: %s:",reperr)));
			}

			/* prevent memory leak */
			freeReplyObject(reply);
		}
	}

	if (con_db != 0)
	{
		char arg[32];
		
		snprintf(arg,32, "%d", con_db);

		if ((reply = redisCommand(ctx,"SELECT %s",arg)) == NULL)
		{
			char *ctxerr = pstrdup(ctx->errstr);
			redisFree(ctx);
			ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), /* ??? */
                 errmsg("failed to select db %d: %s:",con_db,ctxerr)));

		}
		else
		{
			if (reply->type == REDIS_REPLY_ERROR)
			{
				char *reperr = pstrdup(reply->str);
				freeReplyObject(reply);
				redisFree(ctx);
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE), /* ??? */
						 errmsg("failed to select db %d: %s:",con_db,reperr)));
			}

			/* prevent memory leak */
			freeReplyObject(reply);
		}
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


/*
	elog(NOTICE,"passing %d args",nargs);
	for (i = 0; i < nargs; i++)
		elog(NOTICE,"args[%d]: %s",i,args[i]);
*/
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

	if (reply == NULL)
	{
			char *ctxerr = pstrdup(ctx->errstr);
			redisFree(ctx);
			contexts[con_num] = NULL;
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE), /* ??? */
					 errmsg("command %s failed: %s:",cmd,ctxerr)));

	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		char *reperr = pstrdup(reply->str);
		freeReplyObject(reply);
		/* the context is still valid, we just had a command failure */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), /* ??? */
				 errmsg("command %s failed: %s:",cmd,reperr)));
	}

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

	if (reply == NULL)
	{
			char *ctxerr = pstrdup(ctx->errstr);
			redisFree(ctx);
			contexts[con_num] = NULL;
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE), /* ??? */
					 errmsg("command %s failed: %s:",cmd,ctxerr)));

	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		char *reperr = pstrdup(reply->str);
		freeReplyObject(reply);
		/* the context is still valid, we just had a command failure */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), /* ??? */
				 errmsg("command %s failed: %s:",cmd,reperr)));
	}

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

PG_FUNCTION_INFO_V1(redis_command_table);

extern Datum redis_command_table(PG_FUNCTION_ARGS);

Datum
redis_command_table(PG_FUNCTION_ARGS)
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

	if (reply == NULL)
	{
			char *ctxerr = pstrdup(ctx->errstr);
			redisFree(ctx);
			contexts[con_num] = NULL;
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE), /* ??? */
					 errmsg("command %s failed: %s:",cmd,ctxerr)));

	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		char *reperr = pstrdup(reply->str);
		freeReplyObject(reply);
		/* the context is still valid, we just had a command failure */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), /* ??? */
				 errmsg("command %s failed: %s:",cmd,reperr)));
	}

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


PG_FUNCTION_INFO_V1(redis_push_table);

extern Datum redis_push_table(PG_FUNCTION_ARGS);

Datum
redis_push_table(PG_FUNCTION_ARGS)
{
	int con_num = PG_GETARG_INT32(0);
	Datum row = PG_GETARG_DATUM(1);
	text *prefix = PG_GETARG_TEXT_P(2);
	ArrayType *keys = PG_GETARG_ARRAYTYPE_P(3);
    Datum      *keytext;
    bool       *keynulls;
    char      **keystr;
	char      **keyvals;
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

	elog(NOTICE,"argnodes: %s", nodeToString(fcinfo->flinfo->fn_expr));

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
		else
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

	args[0] = "HMSET";
	args[1] = key.data;

	if ((reply = redisCommandArgv(ctx, argc, (const char **) args, NULL)) 
		== NULL)
	{
		char *ctxerr = pstrdup(ctx->errstr);
		redisFree(ctx);
		contexts[con_num] = NULL;
		ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), /* ??? */
                 errmsg("table push failure: %s",ctxerr)));

	}
	else
	{
		if (reply->type == REDIS_REPLY_ERROR)
		{
			char *reperr = pstrdup(reply->str);
			freeReplyObject(reply);
			redisFree(ctx);
			contexts[con_num] = NULL;
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE), /* ??? */
					 errmsg("table push failure: %s:",reperr)));
		}
		
		/* prevent memory leak */
		freeReplyObject(reply);
	}

	PG_RETURN_NULL();
}
