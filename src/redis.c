#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "utils/array.h"
#include "utils/builtins.h"

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
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("connection number %d is already open", con_num)));

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

