//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <thread>

#include "blackwidow/blackwidow.h"

using namespace blackwidow;

int main() {
 int cases = 10;
while (cases --){
  blackwidow::BlackwidowOptions options;
  options.options.create_if_missing = true;
  blackwidow::BlackWidow db;
  blackwidow::Status s = db.Open(options, "db");
  if (s.ok()) {
    printf("Open success\n");
  } else {
    printf("Open failed, error: %s\n", s.ToString().c_str());
    return -1;
  }
  // SAdd
  int32_t ret = 0;
  std::vector<std::string> members {"1", "2", "3", "4","5","6","7","a","c","f","t","p","o"};
  s = db.SAdd("SADD_KEY", members, &ret);
 // printf("SAdd return: %s, ret = %d\n", s.ToString().c_str(), ret);

  // SCard
  ret = 0;
  bool f ;
  std::string member;
  for (int i =0 ; i < members.size(); i++){
  s = db.SPop("SADD_KEY", &member );
 // printf("SPop return: %s, ret = %d\n", s.ToString().c_str(), ret);
  }
printf ("%d",cases);
}

  return 0;
}
