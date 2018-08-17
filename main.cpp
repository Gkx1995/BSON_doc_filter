#include "filter_generator.h"

int main() {

    //query sth matched with input doc
    std::string query = "WHERE int32 document.a.b.c = 1";

    // select all, should return 1
//    std::string query = "where *";

    // query sth not in input_doc, should return 0
//    std::string query = "where string string <= 150";
//    std::string query = "WHERE bool string = 1";
//    std::string query = "WHERE string string = str";

            auto* filter = new Filter(query);

    bson_t* input_doc = filter->generate_input_doc();

    filter->print_map();
    filter->print_filters();
    bool should_input = filter->should_insert(input_doc);



    std::cout << "query is : \n" << query << std::endl;

    std::cout << "input doc is:\n" << bson_as_json(input_doc, NULL) << std::endl;

    std::cout << "input doc should be insert: " << should_input << std::endl;

//    delete(filter);
    delete(input_doc);

    return 0;
}