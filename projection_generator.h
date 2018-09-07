//
// Created by kai on 8/28/18.
//

#ifndef UNTITLED_PROJECTION_GENERATOR_H
#define UNTITLED_PROJECTION_GENERATOR_H

// C include
#include <iostream>
#include <fstream>
#include <sstream>
// third party include
#include <libbson-1.0/bson.h>
#include "filter_compare_util.h"

class Projector {
public:
    // Constructor
    explicit Projector(std::vector<std::string> &selected_fields_list, const std::string& shard_key);
    // Destructor
    ~Projector();

    // entry point
    bson_t* get_input_doc_if_satisfied_filter(const bson_t* input_doc);

private:

    bson_t* append_document(bson_t* bson_doc, std::string& field);
    bson_t* append_array(bson_t* bson_doc, std::string& field);
    void generate_basic_element_doc(bson_t* returned_doc, bson_iter_t* iter);
    bool find_and_append_unique_id(bson_t* returned_doc, const bson_t* input_doc);
    int addtional_appended_count;

protected:
    std::vector<std::string> selected_fields_list;
};

#endif //UNTITLED_PROJECTION_GENERATOR_H
