/*
Copyright (c) Datos IO, Inc. 2016. All rights reserved. All information contained herein is, and remains the property
of Datos IO, Inc. The intellectual and technical concepts contained herein are proprietary to Datos IO, Inc. and may
be covered by U.S. and Foreign Patents, patents in process, and are protected by trade secret or copyright law.
Dissemination of this information or reproduction of this material is strictly forbidden unless prior written
permission is obtained from Datos IO, Inc.
*/

// C++ includes
#include <sstream>
#include <string>
//#include <endian.h>

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

#include "filter_compare_util.h"

using namespace std;

int canonicalize_bson_type(bson_type_t type) {
    switch (type) {
        case BSON_TYPE_MINKEY:
            return -1;
        case BSON_TYPE_MAXKEY:
            return 127;
        case BSON_TYPE_EOD:
        case BSON_TYPE_UNDEFINED:
            return 0;
        case BSON_TYPE_NULL:
            return 5;
        case BSON_TYPE_DOUBLE:
        case BSON_TYPE_INT32:
        case BSON_TYPE_INT64:
        case BSON_TYPE_DECIMAL128:
            return 10;
        case BSON_TYPE_UTF8:
        case BSON_TYPE_SYMBOL:
            return 15;
        case BSON_TYPE_DOCUMENT:
            return 20;
        case BSON_TYPE_ARRAY:
            return 25;
        case BSON_TYPE_BINARY:
            return 30;
        case BSON_TYPE_OID:
            return 35;
        case BSON_TYPE_BOOL:
            return 40;
        case BSON_TYPE_DATE_TIME:
            return 45;
        case BSON_TYPE_TIMESTAMP:
            // return 45; // -- Was 45 in Mongod 2.6
            return 47; // -- Is 47 in 3.0, 3.2. We will just ignore 2.6 for now
        case BSON_TYPE_REGEX:
            return 50;
        case BSON_TYPE_DBPOINTER:
            return 55;
        case BSON_TYPE_CODE:
            return 60;
        case BSON_TYPE_CODEWSCOPE:
            return 65;
        default:
            std::cout << "No canonical mapping found for BSON_TYPE: " << type << std::endl;
            assert(0);
            return -1;
    }
}

bool element_is_special_type(bson_type_t type)
{
    return (type == BSON_TYPE_EOD) || (type == BSON_TYPE_UNDEFINED) || (type == BSON_TYPE_NULL) ||
           (type == BSON_TYPE_MAXKEY) || (type == BSON_TYPE_MINKEY);
}

bool element_is_number_type(bson_type_t type)
{
    return (type == BSON_TYPE_INT32) || (type == BSON_TYPE_INT64) || (type == BSON_TYPE_DOUBLE) ||
           (type == BSON_TYPE_DECIMAL128);
}


int filter_compare_elements(bson_iter_t *l, bson_iter_t *r)
{
    int64_t l_date, r_date;
    int32_t l_int32, r_int32;
    int64_t l_int64, r_int64;
    double l_double, r_double;
    long double l_longdouble, r_longdouble;
    bson_decimal128_t l_dec128, r_dec128;
    uint32_t l_timestamp, l_increment, r_timestamp, r_increment;
    char l_decimal128_string[BSON_DECIMAL128_STRING], r_decimal128_string[BSON_DECIMAL128_STRING];
    char l_options[128], r_options[128];
    const char *l_code, *r_code;
    uint32_t l_len, r_len, l_scope_len, r_scope_len, l_code_len, r_code_len;
    const char  *l_collection, *r_collection, *l_buf, *r_buf;
    const bson_oid_t *l_oid_ptr, *r_oid_ptr;
    bson_iter_t l_child, r_child;
    const uint8_t *l_scope, *r_scope;
    bson_t l_bson, r_bson;
    bson_subtype_t l_subtype, r_subtype;
    int c;
    bool l_valid, r_valid;

    cout << "LIT:" << bson_iter_type(l) << endl;
    cout << "RIT:" << bson_iter_type(r) << endl;

    if (element_is_special_type(bson_iter_type(l)) || element_is_special_type(bson_iter_type(r))) {

        if ((bson_iter_type(l) == bson_iter_type(r)))
            return 0;

        return IGNORE_NUM;
    }

    switch (bson_iter_type(l)) {

        case BSON_TYPE_BOOL:

            if (bson_iter_type(r) != BSON_TYPE_BOOL)
                return IGNORE_NUM;

            return bson_iter_bool(l) - bson_iter_bool(r);

        case BSON_TYPE_TIMESTAMP:

            if (bson_iter_type(r) != BSON_TYPE_TIMESTAMP)\
                return IGNORE_NUM;

            bson_iter_timestamp(l, &l_timestamp, &l_increment);
            bson_iter_timestamp(r, &r_timestamp, &r_increment);

            if (l_timestamp < r_timestamp)
                return -1;
            else if (l_timestamp == r_timestamp) {
                if (l_increment < r_increment)
                    return -1;
                else if (l_increment == r_increment)
                    return 0;
                else return 1;
            }
            else return 1;

        case BSON_TYPE_DATE_TIME:

            if (bson_iter_type(r) != BSON_TYPE_DATE_TIME)
                return IGNORE_NUM;

            l_date = bson_iter_date_time(l);
            r_date = bson_iter_date_time(r);

            if (l_date < r_date)
                return -1;
            return l_date == r_date ? 0 : 1;

        case BSON_TYPE_INT32:

            if (!element_is_number_type(bson_iter_type(r)))
                return IGNORE_NUM;

            l_int32 = bson_iter_int32(l);

            switch (bson_iter_type(r)) {
                case BSON_TYPE_INT32:

                    r_int32 = bson_iter_int32(r);
                    return (l_int32 > r_int32) - (l_int32 < r_int32);

                case BSON_TYPE_INT64:

                    r_int64 = bson_iter_int64(r);
                    return (l_int32 > r_int64) - (l_int32 < r_int64);

                case BSON_TYPE_DOUBLE:

                    r_double = bson_iter_double(r);
                    return (l_int32 > r_double) - (l_int32 < r_double);

                case BSON_TYPE_DECIMAL128:

                    bson_iter_decimal128(r, &r_dec128); //check already made, so will return true
                    bson_decimal128_to_string(&r_dec128, r_decimal128_string);//void function
                    r_longdouble = strtold(r_decimal128_string, NULL);
                    return (l_int32 > r_longdouble) - (l_int32 < r_longdouble);
            }

        case BSON_TYPE_INT64: {

            if (!element_is_number_type(bson_iter_type(r)))
                return IGNORE_NUM;

            l_int64 = bson_iter_int64(l);

            switch (bson_iter_type(r)) {
                case BSON_TYPE_INT32:

                    r_int32 = bson_iter_int32(r);
                    return (l_int64 > r_int32) - (l_int64 < r_int32);

                case BSON_TYPE_INT64:

                    r_int64 = bson_iter_int64(r);
                    return (l_int64 > r_int64) - (l_int64 < r_int64);

                case BSON_TYPE_DOUBLE:

                    r_double = bson_iter_double(r);
                    return (l_int64 > r_double) - (l_int64 < r_double);

                case BSON_TYPE_DECIMAL128:

                    bson_iter_decimal128(r, &r_dec128); //check already made, so will return true
                    bson_decimal128_to_string(&r_dec128, r_decimal128_string);//void function
                    r_longdouble = strtold(r_decimal128_string, NULL);

                    return (l_int64 > r_longdouble) - (l_int64 < r_longdouble);
            }

        }

        case BSON_TYPE_DOUBLE:

            if (!element_is_number_type(bson_iter_type(r)))
                return IGNORE_NUM;

            l_double = bson_iter_double(l);

            switch (bson_iter_type(r)) {
                case BSON_TYPE_INT32:

                    r_int32 = bson_iter_int32(r);
                    return (l_double > r_int32) - (l_double < r_int32);

                case BSON_TYPE_INT64:

                    r_int64 = bson_iter_int64(r);
                    return (l_double > r_int64) - (l_double < r_int64);

                case BSON_TYPE_DOUBLE:

                    r_double = bson_iter_double(r);
                    return (l_double > r_double) - (l_double < r_double);

                case BSON_TYPE_DECIMAL128:

                    bson_iter_decimal128(r, &r_dec128);//check already made, so will return true
                    bson_decimal128_to_string(&r_dec128, r_decimal128_string);//void function
                    r_longdouble = strtold(r_decimal128_string, NULL);

                    return (l_double > r_longdouble) - (l_double < r_longdouble);
            }

        case BSON_TYPE_DECIMAL128:

            if (!element_is_number_type(bson_iter_type(r)))
                return IGNORE_NUM;

            bson_iter_decimal128(l, &l_dec128);
            bson_decimal128_to_string(&l_dec128, l_decimal128_string);
            l_longdouble = strtold(l_decimal128_string, NULL);

            switch (bson_iter_type(r)) {
                case BSON_TYPE_INT32:

                    r_int32 = bson_iter_int32(r);
                    return (l_longdouble > r_int32) - (l_longdouble < r_int32);

                case BSON_TYPE_INT64:

                    r_int64 = bson_iter_int64(r);
                    return (l_longdouble > r_int64) - (l_longdouble < r_int64);

                case BSON_TYPE_DOUBLE:

                    r_double = bson_iter_double(r);
                    return (l_longdouble > r_double) - (l_longdouble < r_double);

                case BSON_TYPE_DECIMAL128:

                    bson_iter_decimal128(r, &r_dec128); //check already made, so will return true
                    bson_decimal128_to_string(&r_dec128, r_decimal128_string);//void function
                    r_longdouble = strtold(r_decimal128_string, NULL);

                    return (l_longdouble > r_longdouble) - (l_longdouble < r_longdouble);
            }

        case BSON_TYPE_OID:

            if (bson_iter_type(r) != BSON_TYPE_OID)
                return IGNORE_NUM;

            return memcmp(bson_iter_oid(l), bson_iter_oid(r), 12);

        case BSON_TYPE_CODE:

            if (bson_iter_type(r) != BSON_TYPE_CODE)
                return IGNORE_NUM;

            return strcmp(bson_iter_code(l, NULL), bson_iter_code(r, NULL));

        case BSON_TYPE_SYMBOL:

            if (bson_iter_type(r) != BSON_TYPE_SYMBOL)
                return IGNORE_NUM;

            return strcmp(bson_iter_symbol(l, NULL), bson_iter_symbol(r, NULL));

        case BSON_TYPE_UTF8:

            if (bson_iter_type(r) != BSON_TYPE_UTF8)
                return IGNORE_NUM;

            std::cout << strcmp(bson_iter_utf8(l, NULL), bson_iter_utf8(r, NULL)) << std::endl;
            return strcmp(bson_iter_utf8(l, NULL), bson_iter_utf8(r, NULL));


        case BSON_TYPE_REGEX:

            if (bson_iter_type(r) != BSON_TYPE_REGEX)
                return IGNORE_NUM;

            c = strcmp(bson_iter_regex(l, (const char **)&l_options), bson_iter_regex(r, (const char **)&r_options));
            if (c)
                return c;

            return strcmp(l_options, r_options);

        case BSON_TYPE_DBPOINTER:

            if (bson_iter_type(r) != BSON_TYPE_DBPOINTER)
                return IGNORE_NUM;

            bson_iter_dbpointer(l, &l_len, &l_collection, &l_oid_ptr); //void function
            bson_iter_dbpointer(r, &r_len, &r_collection, &r_oid_ptr); //void function

            if (l_len != r_len)
                return (l_len > r_len) - (l_len < r_len);

            return strcmp(l_collection, r_collection);

        case BSON_TYPE_DOCUMENT:

            if (bson_iter_type(r) != BSON_TYPE_DOCUMENT)
                return IGNORE_NUM;

            l_valid = bson_iter_recurse(l, &l_child);
            r_valid = bson_iter_recurse(r, &r_child);

            if (!l_valid)
                return r_valid ? -1 : 0;
            if (!r_valid)
                return 1;

            return filter_compare_iterators(&l_child, &r_child);

        case BSON_TYPE_ARRAY:

            if (bson_iter_type(r) != BSON_TYPE_ARRAY)
                return IGNORE_NUM;

            l_valid = bson_iter_recurse(l, &l_child);
            r_valid = bson_iter_recurse(r, &r_child);

            if (!l_valid)
                return r_valid ? -1 : 0;
            if (!r_valid)
                return 1;

            return filter_compare_iterators(&l_child, &r_child);

        case BSON_TYPE_BINARY:

            if (bson_iter_type(r) != BSON_TYPE_BINARY)
                return IGNORE_NUM;

            bson_iter_binary(l, &l_subtype, &l_len, (const uint8_t **)&l_buf); //void function
            bson_iter_binary(r, &r_subtype, &r_len, (const uint8_t **)&r_buf); //void function

            if (l_len != r_len)
                return (l_len > r_len) - (l_len < r_len);

            return memcmp(l_buf, r_buf, l_len);

        case BSON_TYPE_CODEWSCOPE:

            if (bson_iter_type(r) != BSON_TYPE_CODEWSCOPE)
                return IGNORE_NUM;

            l_code = bson_iter_codewscope(l, &l_code_len, &l_scope_len, &l_scope);
            r_code = bson_iter_codewscope(r, &r_code_len, &r_scope_len, &r_scope);

            c = strcmp(l_code, r_code);
            if (c)
                return c;

            l_valid = bson_init_static(&l_bson, l_scope, l_scope_len);
            if (l_valid)
                l_valid = bson_iter_init(&l_child, &l_bson);

            r_valid = bson_init_static(&r_bson, r_scope, r_scope_len);
            if (r_valid)
                r_valid = bson_iter_init(&r_child, &r_bson);

            if (!l_valid)
                return r_valid ? -1 : 0;
            if (!r_valid)
                return 1;

            return filter_compare_iterators(&l_child, &r_child);
    }
}

int filter_compare_iterators(bson_iter_t *l, bson_iter_t *r)
{
    bool l_valid, r_valid;

    while (true) {
        r_valid = bson_iter_next(r);
        l_valid = false;
        if (r_valid) {
            cout << "Key:" << bson_iter_key(r) << endl;
            l_valid = bson_iter_find(l, bson_iter_key(r));

        }

        if (!l_valid)
            return IGNORE_NUM;
        if (!r_valid)
            return IGNORE_NUM;

        return filter_compare_elements(l, r);
    }
}

int filter_compare_object(const bson_t *l, bson_t *r)
{
    bson_iter_t l_iter, r_iter;
    int cmp_rst;

    /* initialize */
    bson_iter_init(&l_iter, l);
    bson_iter_init(&r_iter, r);

    cmp_rst = filter_compare_iterators(&l_iter, &r_iter);

    if (cmp_rst == IGNORE_NUM) {
        return IGNORE_NUM;
    }
    else if (cmp_rst > 0) {
        return 1;
    } else {
        return cmp_rst < 0 ? -1 : 0;
    }
}


