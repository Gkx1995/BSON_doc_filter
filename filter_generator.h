//
// Created by kai on 8/3/18.
//

#ifndef _FILTER_GENERATOR_UTILS_H
#define _FILTER_GENERATOR_UTILS_H

// C include
#include <iostream>
#include <fstream>
#include <sstream>
#include "stack"
// third party include
#include <tao/pegtl.hpp>
#include <libbson-1.0/bson.h>
#include "filter_compare_util.h"

class Filter{
public:
    // Constructor
    explicit Filter(std::string& query);
    // Destructor
    ~Filter();

    // entry point to filter input_doc
    bool should_insert(const bson_t* input_doc);
    const bson_t* get_input_doc_if_satisfied_filter(const bson_t* input_doc);

    //////////////////////////////////////////////////////////
    // methods for testing
    //////////////////////////////////////////////////////////
    void print_map();
    void print_filters();
    bson_t* generate_input_doc();


private:

    bson_t* generate_filter(std::string& field, std::string& term, std::string& dataType);
    void generate_filters();
    bool filter_satisfied (int flag, std::string& _operator);
    void perform_pegtl_parser(std::string& query);
    void generate_data_type_map();
    bson_t* generate_unnested_filter(std::string& field, std::string& term, std::string& dataType);
    bool satisfy_query(bool restrictions_satisfied_arr[]);

protected:
    std::map<std::string, std::vector<std::string>> arg_map;
    std::map<std::string, unsigned long> data_type_map;
    std::vector<_bson_t*> filters;
};

#endif //_FILTER_GENERATOR_UTILS_H
