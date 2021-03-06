// The MIT License (MIT)
// Copyright (c) 2014 Yufei (Benny) Chen
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
#ifndef LOOPY_REDIS_H_
#define LOOPY_REDIS_H_

#include <string>
#include "library/sys/ldriver.h"
#include "library/sys/lpromise.h"
#include "drivers/third_party/hiredis/hiredis.h"
#include "drivers/third_party/hiredis/async.h"
#include "drivers/third_party/hiredis/adapters/libevent.h"

namespace loopy {

using std::string;

class LRedisData {
 public:
  LRedisData() {}
  LRedisData(redisReply* reply) {
    type = reply->type;
    switch(reply->type) {
      case REDIS_REPLY_STATUS: 
      case REDIS_REPLY_ERROR:
        str = string(reply->str, reply->len);
        break;
      case REDIS_REPLY_INTEGER: 
        integer = reply->integer;
      case REDIS_REPLY_STRING:
        str = string(reply->str, reply->len);
        break;
      default:
        // FIXME: a lot of cases were not covered
        throw "what the heck happend!";
    }
  }

  // FIXME: change this to an unrestricted union so it doesn't take extra
  // space for no reason
  int16_t integer;
  string str;
  short type;
};

inline void
LRedisCb(redisAsyncContext* redis, void* redis_reply, void* arg) {
  redisReply* reply = static_cast<redisReply*>(redis_reply);
  auto callback = static_cast<std::function<void(void*)>*>(arg);
  // FIXME: holy shit just fix this later
  (*callback)(new LRedisData(reply));
}

// FIXME: add some sort of voodoo proxy calling?
class LRedis : public LDriver {
 public:
  explicit LRedis(
    evthr_t* thread,
    const char* host = "127.0.0.1",
    int port = 6379
  ) : LDriver(thread),
      host_(host),
      port_(port),
      redis_(redisAsyncConnect(host_, port_))
  {
    DBConnect();
  }

  void DBConnect() override {
    // store the connection data into the thread
    redisLibeventAttach(redis_, evbase_);
  };

  void DBDisconnect() override {
    redisAsyncDisconnect(redis_);
  };

  std::string DriverName() const override {
    return "LRedis";
  }
  typedef std::function<void(void*)> CBSignature;
  typedef LRedisData* ReturnType;
  typedef LPromise<LRedis> PromiseType;
  typedef std::shared_ptr<PromiseType> PromisePtr;

  PromisePtr set(const char* key, const char* val) {
    return PromisePtr(new PromiseType(
      CBSignature([this, key, val](void* arg) {
        redisAsyncCommand(redis_, LRedisCb, arg, "SET %s %s", key, val);
      })
    ));
  }

  PromisePtr get(const char* key) {
    return PromisePtr(new PromiseType(
      CBSignature([this, key](void* arg) {
        redisAsyncCommand(redis_, LRedisCb, arg, "GET %s", key);
      })
    ));
  }

  PromisePtr hset(const char* hash, const char* key, const char* val) {
    return PromisePtr(new PromiseType(
      CBSignature([this, hash, key, val](void* arg) {
        redisAsyncCommand(redis_, LRedisCb, arg, "HSET %s %s %s", hash, key, val);
      })
    ));
  }

  PromisePtr hget(const char* hash, const char* key) {
    return PromisePtr(new PromiseType(
      CBSignature([this, hash, key](void* arg) {
        redisAsyncCommand(redis_, LRedisCb, arg, "HGET %s %s", hash, key);
      })
    ));
  }

  PromisePtr incr(const char* key) {
    return PromisePtr(new PromiseType(
      CBSignature([this, key](void* arg) {
        redisAsyncCommand(redis_, LRedisCb, arg, "INCR %s", key);
      })
    ));
  }

  PromisePtr exec(const char* cmd) {
    return PromisePtr(new PromiseType(
      CBSignature([this, cmd](void* arg) {
        redisAsyncCommand(redis_, LRedisCb, arg, cmd);
      })
    ));
  }

 private:
  const char* host_;
  int port_;
  redisAsyncContext* redis_;
};

}  // namespace loopy
#endif  // LOOPY_REDIS_H_
