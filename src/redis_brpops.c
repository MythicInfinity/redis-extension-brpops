#include "redismodule.h"
#include <stdlib.h>
#include <stdbool.h>

bool util_reply_with_list(RedisModuleCtx *ctx, RedisModuleString *keyStr) {
    RedisModuleCallReply *reply;
    RedisModuleCallReply *trim_reply;
    RedisModuleCallReply *subreply;

    reply = RedisModule_Call(ctx, "LRANGE", "scc", keyStr, "0", "-1");
    if (reply == NULL) {
        RedisModule_ReplyWithError(ctx, "ERR lrange failed");
        return false;
    }

    trim_reply = RedisModule_Call(ctx, "LTRIM", "scc", keyStr, "1", "0");
    if (trim_reply == NULL || RedisModule_CallReplyType(trim_reply) == REDISMODULE_REPLY_ERROR) {
        RedisModule_ReplyWithError(ctx, "ERR ltrim failed");
        return false;
    }

    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
        RedisModule_ReplyWithError(ctx, "ERR unexpected reply from lrange");
        return false;
    }

    size_t reply_len = RedisModule_CallReplyLength(reply);

    // start the reply
    RedisModule_ReplyWithArray(ctx, reply_len);

    while (reply_len--) {
        subreply = RedisModule_CallReplyArrayElement(reply, reply_len);

        if (RedisModule_CallReplyType(subreply) == REDISMODULE_REPLY_INTEGER) {
            long long subreply_val = RedisModule_CallReplyInteger(subreply);
            RedisModule_ReplyWithLongLong(ctx, subreply_val);
        } else if (RedisModule_CallReplyType(subreply) == REDISMODULE_REPLY_STRING) {
            size_t subreply_len = RedisModule_CallReplyLength(subreply);
            const char *subreply_val = RedisModule_CallReplyStringPtr(subreply, &subreply_len);

            RedisModule_ReplyWithStringBuffer(ctx, subreply_val, subreply_len);
        } else {
            RedisModule_ReplyWithError(ctx, "ERR unrecognized array elem type");
        }
    }

    return true;
}

bool util_reply_with_list_batch(RedisModuleCtx *ctx, RedisModuleString *keyStr, RedisModuleString *countStr) {
    RedisModuleCallReply *reply;
    RedisModuleCallReply *subreply;

    reply = RedisModule_Call(ctx, "RPOP", "ss", keyStr, countStr);

    if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_NULL) {
        RedisModule_ReplyWithNull(ctx);
        return true;
    }

    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
        RedisModule_ReplyWithError(ctx, "ERR unexpected reply from RPOP");
        return false;
    }

    size_t reply_len = RedisModule_CallReplyLength(reply);

    // start the reply
    RedisModule_ReplyWithArray(ctx, reply_len);

    for (long unsigned i = 0; i < reply_len; i++) {
        subreply = RedisModule_CallReplyArrayElement(reply, i);

        if (RedisModule_CallReplyType(subreply) == REDISMODULE_REPLY_INTEGER) {
            long long subreply_val = RedisModule_CallReplyInteger(subreply);
            RedisModule_ReplyWithLongLong(ctx, subreply_val);
        } else if (RedisModule_CallReplyType(subreply) == REDISMODULE_REPLY_STRING) {
            size_t subreply_len = RedisModule_CallReplyLength(subreply);
            const char *subreply_val = RedisModule_CallReplyStringPtr(subreply, &subreply_len);

            RedisModule_ReplyWithStringBuffer(ctx, subreply_val, subreply_len);
        } else {
            RedisModule_ReplyWithError(ctx, "ERR unrecognized array elem type");
        }
    }

    return true;
}

int timeout_func(RedisModuleCtx *ctx, RedisModuleString **argv,
               int argc)
{
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    return RedisModule_ReplyWithNull(ctx);
}

int brpopallWakeUp(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    RedisModule_AutoMemory(ctx);

    RedisModuleString *keyStr = RedisModule_GetBlockedClientReadyKey(ctx);

    // validate key
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyStr, REDISMODULE_READ | REDISMODULE_WRITE);

    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_LIST && RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_OK;
    }

    long list_len = RedisModule_ValueLength(key);
    if (list_len > 0) {
        if (util_reply_with_list(ctx, keyStr)) {
            return REDISMODULE_OK;
        }

        return REDISMODULE_OK;
    }

    return REDISMODULE_ERR;
}

// Command handler for BRPOPALL args: key Optional[timeout]
int brpopallCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 2 || argc > 3) return RedisModule_WrongArity(ctx);

    RedisModule_AutoMemory(ctx);

    long long timeout = 0;

    // Validate timeout in argv[2]
    if (argc == 3) {
        if (RedisModule_StringToLongLong(argv[2],&timeout) != REDISMODULE_OK) {
            return RedisModule_ReplyWithError(ctx,"ERR invalid timeout");
        }

        if (timeout < 0) {
            return RedisModule_ReplyWithError(ctx, "ERR timeout can't be negative");
        }
    }

    // validate key
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);

    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_LIST && RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    long list_len = RedisModule_ValueLength(key);
    if (list_len > 0) {
        if (util_reply_with_list(ctx, argv[1])) {
            return REDISMODULE_OK;
        }
        return REDISMODULE_ERR;
    }

    RedisModuleString *watchedKeysArr[1];
    watchedKeysArr[0] = argv[1];

    RedisModule_BlockClientOnKeys(ctx, brpopallWakeUp, timeout_func, NULL, timeout, watchedKeysArr, 1, NULL);
    return REDISMODULE_OK;
}

int brpopbatchWakeUp(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argc);

    RedisModule_AutoMemory(ctx);

    RedisModuleString *keyStr = RedisModule_GetBlockedClientReadyKey(ctx);

    // validate key
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyStr, REDISMODULE_READ | REDISMODULE_WRITE);

    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_LIST && RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_OK;
    }

    long long count = -1;
    if (argc < 3 || RedisModule_StringToLongLong(argv[2], &count) != REDISMODULE_OK || count < 1) {
        RedisModule_ReplyWithError(ctx, "ERR invalid count");
        return REDISMODULE_OK;
    }

    long list_len = RedisModule_ValueLength(key);
    if (list_len > 0) {
        if (util_reply_with_list_batch(ctx, keyStr, argv[2])) {
            return REDISMODULE_OK;
        }

        return REDISMODULE_OK;
    }

    return REDISMODULE_ERR;
}

// Command handler for BRPOPBATCH args: key count Optional[timeout]
int brpopbatchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3 || argc > 4) return RedisModule_WrongArity(ctx);

    RedisModule_AutoMemory(ctx);

    long long timeout = 0;
    long long count = -1;

    // Validate count in argv[2]
    if (RedisModule_StringToLongLong(argv[2], &count) != REDISMODULE_OK) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid count");
    }

    if (count < 1) {
        return RedisModule_ReplyWithError(ctx, "ERR count can't be less than one.");
    }

    // Optionally Validate timeout in argv[3]
    if (argc == 4) {
        if (RedisModule_StringToLongLong(argv[3], &timeout) != REDISMODULE_OK) {
            return RedisModule_ReplyWithError(ctx,"ERR invalid timeout.");
        }

        if (timeout < 0) {
            return RedisModule_ReplyWithError(ctx, "ERR timeout can't be negative.");
        }
    }

    // validate key
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);

    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_LIST && RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    long list_len = RedisModule_ValueLength(key);
    if (list_len > 0) {
        if (util_reply_with_list_batch(ctx, argv[1], argv[2])) {
            return REDISMODULE_OK;
        }

        return REDISMODULE_ERR;
    }

    RedisModule_Log(ctx, "warning", "blocking client");

    RedisModuleString *watchedKeysArr[1];
    watchedKeysArr[0] = argv[1];

    RedisModule_BlockClientOnKeys(ctx, brpopbatchWakeUp, timeout_func, NULL, timeout, watchedKeysArr, 1, NULL);
    return REDISMODULE_OK;
}


int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "brpopall", 1, REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "brpopall",
        brpopallCommand, "write deny-oom",
        0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "brpopbatch",
        brpopbatchCommand, "write deny-oom",
        0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
