/*
Copyright (c) Datos IO, Inc. 2016. All rights reserved. All information contained herein is, and remains the property
of Datos IO, Inc. The intellectual and technical concepts contained herein are proprietary to Datos IO, Inc. and may
be covered by U.S. and Foreign Patents, patents in process, and are protected by trade secret or copyright law.
Dissemination of this information or reproduction of this material is strictly forbidden unless prior written
permission is obtained from Datos IO, Inc.
*/

#ifndef __BSON_UTIL_H__
#define __BSON_UTIL_H__

#include <iostream>
#include <vector>
#include <sstream>

// C includes
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>
#include <limits.h>
#include <dirent.h>
#include <assert.h>
#include <libbson-1.0/bson.h>
#include "common_constants.h"

// Mongo C includes

// Datos local includes

// Datos common includes

//  we should ignore this input_doc
//const int IGNORE_NUM = INT_MAX;

int filter_compare_iterators(bson_iter_t *l, bson_iter_t *r);
int filter_compare_elements(bson_iter_t *l, bson_iter_t *r);
int filter_compare_object(const bson_t *l, bson_t *r);

#endif /* __BSON_UTIL_H__ */

