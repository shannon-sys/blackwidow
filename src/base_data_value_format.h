//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_BASE_DATA_VALUE_FORMAT_H_
#define SRC_BASE_DATA_VALUE_FORMAT_H_
#include <string>
namespace blackwidow {

//#define HASHES_EXTRA_SUFFIX 'x'

// add a byte suffix
/*inline void AddHashesDataValueExtraSuffix(std::string* value) {
  if (value != NULL) {
    value->append(1, HASHES_EXTRA_SUFFIX);
  }
}
// delete a byte suffix
inline void TrimHashesDataValue(std::string* value) {
  if (value != NULL && value->size() > 0) {
    value->resize(value->size() - 1);
  }
}*/

}
#endif
