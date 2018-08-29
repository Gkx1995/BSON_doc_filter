#include "filter_generator.h"

bson_t* generate_fixed_input_doc() {
    bson_decimal128_t decimal128;
    bson_t* input_doc;
    bson_t* a = bson_new();
    bson_t* b = bson_new();
    bson_t* c = bson_new();

    input_doc = BCON_NEW("foo", "{", "bar", "[", "{", "baz_0", BCON_INT32 (0), "}", "{", "baz_1", BCON_INT32 (1), "}", "]", "}");
//    BSON_APPEND_BOOL(input_doc, "bool", true);
//    BSON_APPEND_UTF8(input_doc, "utf8", "99");
//    BSON_APPEND_DOUBLE(input_doc, "double", 10.50);
//    BSON_APPEND_INT32(input_doc, "int32", 200);
//    BSON_APPEND_INT64(input_doc, "int64", 300);
//    BSON_APPEND_DATE_TIME(input_doc, "date_time", 400);
//    BSON_APPEND_SYMBOL(input_doc, "symbol", "***");
//    BSON_APPEND_MAXKEY(input_doc, "maxkey");
//    BSON_APPEND_MINKEY(input_doc, "minkey");
//    BSON_APPEND_NULL(input_doc, "null");
//    BSON_APPEND_INT32(input_doc, "in t 32", 1000);
//    BSON_APPEND_BOOL(input_doc, "b o o l", false);
//
//    BSON_APPEND_UTF8(input_doc, "utf 8", "utf 8");

    // generate nested element {document:{a: {b: {c: 1}}}}
    BSON_APPEND_INT32(c, "c", 1);
    BSON_APPEND_INT32(c, "nested", 5);
    BSON_APPEND_DOCUMENT(b, "b", c);
    BSON_APPEND_DOCUMENT(a, "a", b);
    BSON_APPEND_DOCUMENT(input_doc, "document", a);



    bson_decimal128_from_string("500", &decimal128);
    BSON_APPEND_DECIMAL128(input_doc, "decimal128", &decimal128);

    std::cout << "input_doc is: " << bson_as_json(input_doc, NULL) << std::endl;

    return input_doc;
}

int main() {

//    generate_fixed_input_doc();

//    query sth matched with input doc
    std::string query = "select document.a.b.c,document.a.b.d,foo.bar.0.baz_0,int32,foo.bar.1.baz_1 where ((int32 int32 *) or (double double = 10.50)) and int64 int64 >= 300";

    // select all, should return 1
//    std::string query = "where *";

    // query sth not in input_doc, should return 0
//    std::string query = "where string string <= 150";
//    std::string query = "WHERE bool string = 1";
//    std::string query = "WHERE string string = str";

            auto* filter = new Filter(query);

    bson_t* input_doc = filter->generate_input_doc();

//    filter->print_map();
//    filter->print_filters();
//    bool should_input = filter->should_insert(input_doc);
    const bson_t* projection = filter->get_input_doc_if_satisfied_filter(input_doc);



    std::cout << "query is : \n" << query << std::endl;

    std::cout << "input doc is:\n" << bson_as_json(input_doc, NULL) << std::endl;

//    std::cout << "input doc should be insert: " << should_input << std::endl;
    if (projection) {
        std::cout << "projection of input_doc: " << bson_as_json(projection, NULL) << std::endl;
    } else {
        std::cout << "do not exist such projection" << std::endl;
    }

//    delete(filter);
    delete(input_doc);

    return 0;
}