//
// Created by kai on 8/6/18.
//

#define CATCH_CONFIG_MAIN
#include "filter_generator.h"
#include "catch2/catch.hpp"

bson_t* generate_fixed_input_doc() {
    bson_decimal128_t decimal128;
    bson_t* input_doc;
    bson_t* a = bson_new();
    bson_t* b = bson_new();
    bson_t* c = bson_new();

    input_doc = BCON_NEW("foo", "{", "bar", "[", "{", "baz_0", BCON_INT32 (0), "}", "{", "baz_1", BCON_INT32 (1), "}", "]", "}");
    BSON_APPEND_BOOL(input_doc, "bool", true);
    BSON_APPEND_UTF8(input_doc, "utf8", "99");
    BSON_APPEND_DOUBLE(input_doc, "double", 10.50);
    BSON_APPEND_INT32(input_doc, "int32", 200);
    BSON_APPEND_INT64(input_doc, "int64", 300);
    BSON_APPEND_DATE_TIME(input_doc, "date_time", 400);
    BSON_APPEND_SYMBOL(input_doc, "symbol", "***");
    BSON_APPEND_MAXKEY(input_doc, "maxkey");
    BSON_APPEND_MINKEY(input_doc, "minkey");
    BSON_APPEND_NULL(input_doc, "null");
    BSON_APPEND_INT32(input_doc, "in t 32", 1000);
    BSON_APPEND_BOOL(input_doc, "b o o l", false);

    BSON_APPEND_UTF8(input_doc, "utf 8", "utf 8");

    // generate nested element {document:{a: {b: {c: 1}}}}
    BSON_APPEND_INT32(c, "c", 1);
    BSON_APPEND_DOCUMENT(b, "b", c);
    BSON_APPEND_DOCUMENT(a, "a", b);
    BSON_APPEND_DOCUMENT(input_doc, "document", a);



    bson_decimal128_from_string("500", &decimal128);
    BSON_APPEND_DECIMAL128(input_doc, "decimal128", &decimal128);
//    std::cout << "input_doc is: " << bson_as_json(input_doc, NULL) << std::endl;

    return input_doc;
}

bool should_insert(bson_t* input_doc, std::string &query) {
    auto* filter = new Filter(query);
    bool should_input = filter->should_insert(input_doc);
    return should_input;
}

const bson_t* get_input_doc_if_satisfied_filter(bson_t* input_doc, std::string &query) {
    auto* filter = new Filter(query);
    return filter->get_input_doc_if_satisfied_filter(input_doc);
}

bool is_identical(const bson_t* l, const bson_t* r) {
    return bson_compare(l, r) == 0;
}

TEST_CASE("Test projections: select *", "[get_input_doc_if_satisfied_filter]") {
    bson_t *input_doc = generate_fixed_input_doc();

    std::string q1 = "select *";
    const bson_t* output_doc_1 = get_input_doc_if_satisfied_filter(input_doc, q1);
    const bson_t* valid_doc_1 = generate_fixed_input_doc();
    CHECK(is_identical(output_doc_1, valid_doc_1) == true);

    if (input_doc == output_doc_1)
        delete (input_doc);
    else {
        delete (output_doc_1);
        delete (input_doc);
    }
    if (valid_doc_1)
        delete (valid_doc_1);
}

TEST_CASE("Test projections: select int32", "[get_input_doc_if_satisfied_filter]") {
    bson_t *input_doc = generate_fixed_input_doc();

    std::string q1 = "select int32";
    const bson_t* output_doc_1 = get_input_doc_if_satisfied_filter(input_doc, q1);
    bson_t* valid_doc_1 = bson_new();
    BSON_APPEND_INT32(valid_doc_1, "int32", 200);

    CHECK(is_identical(output_doc_1, valid_doc_1) == true);

    if (input_doc == output_doc_1)
        delete (input_doc);
    else {
        delete (output_doc_1);
        delete (input_doc);
    }
    if (valid_doc_1)
        delete (valid_doc_1);
}

TEST_CASE("Test projections: select document.a.b.c", "[get_input_doc_if_satisfied_filter]") {
    bson_t *input_doc = generate_fixed_input_doc();

    std::string q1 = "select document.a.b.c";
    const bson_t* output_doc_1 = get_input_doc_if_satisfied_filter(input_doc, q1);
    bson_t* valid_doc_1 = bson_new();
    bson_t* a = bson_new();
    bson_t* b = bson_new();
    bson_t* c = bson_new();
    BSON_APPEND_INT32(c, "c", 1);
    BSON_APPEND_DOCUMENT(b, "b", c);
    BSON_APPEND_DOCUMENT(a, "a", b);
    BSON_APPEND_DOCUMENT(valid_doc_1, "document", a);

    CHECK(is_identical(output_doc_1, valid_doc_1) == true);

    if (input_doc == output_doc_1)
        delete (input_doc);
    else {
        delete (output_doc_1);
        delete (input_doc);
    }
    if (valid_doc_1)
        delete (valid_doc_1);
}

TEST_CASE("Test projections: select wrong_doc.a.b.c", "[get_input_doc_if_satisfied_filter]") {
    bson_t *input_doc = generate_fixed_input_doc();

    std::string q1 = "select wrong_doc.a.b.c";
    // should return nullptr
    const bson_t* output_doc_1 = get_input_doc_if_satisfied_filter(input_doc, q1);


    CHECK(!output_doc_1);

    if (input_doc == output_doc_1)
        delete (input_doc);
    else {
        delete (output_doc_1);
        delete (input_doc);
    }
}

//TEST_CASE("Test projections: select document.a.b.c,foo.bar.0.baz_0,int32", "[get_input_doc_if_satisfied_filter]") {
//    bson_t *input_doc = generate_fixed_input_doc();
//
//    std::string q1 = "select document.a.b.c";
//    const bson_t* output_doc_1 = get_input_doc_if_satisfied_filter(input_doc, q1);
//    bson_t* valid_doc_1 = bson_new();
//    bson_t* a = bson_new();
//    bson_t* b = bson_new();
//    bson_t* c = bson_new();
//    BSON_APPEND_INT32(c, "c", 1);
//    BSON_APPEND_DOCUMENT(b, "b", c);
//    BSON_APPEND_DOCUMENT(a, "a", b);
//    BSON_APPEND_DOCUMENT(valid_doc_1, "document", a);
//
//    BSON_APPEND_INT32(valid_doc_1, "int32", 200);
//
//    CHECK(is_identical(output_doc_1, valid_doc_1) == true);
//
//    if (input_doc == output_doc_1)
//        delete (input_doc);
//    else {
//        delete (output_doc_1);
//        delete (input_doc);
//    }
//    if (valid_doc_1)
//        delete (valid_doc_1);
//}

TEST_CASE("Test dot donation style projections: select foo.bar.baz_0,document.a.b.c, int32", "[get_input_doc_if_satisfied_filter]") {
    bson_t *input_doc = generate_fixed_input_doc();

    std::string q1 = "select document.a.b.c";
    const bson_t* output_doc_1 = get_input_doc_if_satisfied_filter(input_doc, q1);
    bson_t* valid_doc_1 = BCON_NEW("foo", "{", "bar", "[", "{", "baz_0", BCON_INT32 (0), "}", "]", "}");
    bson_t* a = bson_new();
    bson_t* b = bson_new();
    bson_t* c = bson_new();
    BSON_APPEND_INT32(c, "c", 1);
    BSON_APPEND_DOCUMENT(b, "b", c);
    BSON_APPEND_DOCUMENT(a, "a", b);
    BSON_APPEND_DOCUMENT(valid_doc_1, "document", a);
    BSON_APPEND_INT32(valid_doc_1, "int32", 200);

    CHECK(is_identical(output_doc_1, valid_doc_1) == true);

    if (input_doc == output_doc_1)
        delete (input_doc);
    else {
        delete (output_doc_1);
        delete (input_doc);
    }
    if (valid_doc_1)
        delete (valid_doc_1);
}

TEST_CASE("Test projections: select document.a.b.c,int32 where maxkey maxkey *", "[get_input_doc_if_satisfied_filter]") {
    bson_t *input_doc = generate_fixed_input_doc();

    std::string q1 = "select document.a.b.c,int32 where maxkey maxkey *";
    const bson_t* output_doc_1 = get_input_doc_if_satisfied_filter(input_doc, q1);
    bson_t* valid_doc_1 = bson_new();
    bson_t* a = bson_new();
    bson_t* b = bson_new();
    bson_t* c = bson_new();
    BSON_APPEND_INT32(c, "c", 1);
    BSON_APPEND_DOCUMENT(b, "b", c);
    BSON_APPEND_DOCUMENT(a, "a", b);
    BSON_APPEND_DOCUMENT(valid_doc_1, "document", a);
    BSON_APPEND_INT32(valid_doc_1, "int32", 200);

    CHECK(is_identical(output_doc_1, valid_doc_1) == true);

    if (input_doc == output_doc_1)
        delete (input_doc);
    else {
        delete (output_doc_1);
        delete (input_doc);
    }
    if (valid_doc_1)
        delete (valid_doc_1);
}

TEST_CASE("Test projections: select document.a.b.c,int32 where maxkey maxkey !", "[get_input_doc_if_satisfied_filter]") {
    bson_t *input_doc = generate_fixed_input_doc();

    std::string q1 = "select document.a.b.c,int32 where maxkey maxkey !";
    // should return nullptr
    const bson_t* output_doc_1 = get_input_doc_if_satisfied_filter(input_doc, q1);


    CHECK(!output_doc_1);

    if (input_doc == output_doc_1)
        delete (input_doc);
    else {
        delete (output_doc_1);
        delete (input_doc);
    }
}

TEST_CASE("Test 6 numeric operators", "[should_insert]") {
    bson_t *input_doc = generate_fixed_input_doc();

    std::string q1 = "select * where int32 int32 = 200";
    CHECK(should_insert(input_doc, q1) == true);

    std::string q2 = "select * where int32 int32 > 200";
    CHECK(should_insert(input_doc, q2) == false);

    std::string q3 = "select * where int32 int32 < 200";
    CHECK(should_insert(input_doc, q3) == false);

    std::string q5 = "select * where int32 int32 != 200";
    CHECK(should_insert(input_doc, q5) == false);

    std::string q6 = "select * where int32 int32 >= 200";
    CHECK(should_insert(input_doc, q6) == true);

    std::string q7 = "select * where int32 int32 <= 200";
    CHECK(should_insert(input_doc, q7) == true);

    delete(input_doc);
}

TEST_CASE("Multiple input doc for one filter instance", "[should_insert]") {

    std::cout << "Multiple input doc for one filter instance" << std::endl;
    bson_t *input_doc1 = generate_fixed_input_doc();
    bson_t *input_doc2 = generate_fixed_input_doc();
    bson_t *input_doc3 = generate_fixed_input_doc();
    bson_t *input_doc4 = generate_fixed_input_doc();
    bson_t *input_doc5 = generate_fixed_input_doc();
    std::string q1 = "select * where int32 int32 = 200";
    auto* filter = new Filter(q1);

    CHECK(filter->should_insert(input_doc1));

    CHECK(filter->should_insert(input_doc2));

    CHECK(filter->should_insert(input_doc3));

    CHECK(filter->should_insert(input_doc4));

    CHECK(filter->should_insert(input_doc5));

    delete(input_doc1);
    delete(input_doc2);
    delete(input_doc3);
    delete(input_doc4);
    delete(input_doc5);

}

TEST_CASE( "input_doc have 11 data types", "[should_insert]" ) {

    bson_t *input_doc = generate_fixed_input_doc();

    SECTION( "filter data type is utf8, should only check utf8 element in input_doc" ) {

        std::string q1 = "select * where utf8 utf8 = 99";
        CHECK(should_insert(input_doc, q1) == true);

        std::string q2 = "select * where utf8 utf8 < 98";
        CHECK(should_insert(input_doc, q2) == false);

        std::string q3 = "select * where utf8 utf8 > 97";
        CHECK(should_insert(input_doc, q3) == true);

        std::string q5 = "select * where utf8 int32 = 99";
        CHECK(should_insert(input_doc, q5) == false);
    }
    SECTION( "filter data type is symbol, should only check symbol element in input_doc" ) {

        std::string q1 = "select * where symbol symbol = ***";
        CHECK(should_insert(input_doc, q1) == true);

        std::string q2 = "select * where symbol symbol = **";
        CHECK(should_insert(input_doc, q2) == false);

        std::string q3 = "select * where symbol symbol != **";
        CHECK(should_insert(input_doc, q3) == true);
    }
    SECTION( "filter data type is int32 type, should only check numeric data types in input_doc" ) {

        std::string q4 = "select * where int32 utf8 = 99";
        CHECK(should_insert(input_doc, q4) == false);

        std::string q1 = "select * where int32 double < 11";
        CHECK(should_insert(input_doc, q1) == true);

        std::string q2 = "select * where int32 decimal128 = 500";
        CHECK(should_insert(input_doc, q2) == true);

        std::string q3 = "select * where int32 int64 = 300";
        CHECK(should_insert(input_doc, q3) == true);

        std::string q5 = "select * where int32 date_time = 400";
        CHECK(should_insert(input_doc, q5) == false);

        std::string q6 = "select * where int32 int32 = 200";
        CHECK(should_insert(input_doc, q6) == true);
    }
    SECTION( "filter data type is int64, should only check numeric data types in input_doc" ) {

        std::string q4 = "select * where int64 utf8 = 99";
        CHECK(should_insert(input_doc, q4) == false);

        std::string q1 = "select * where int64 double < 11";
        CHECK(should_insert(input_doc, q1) == true);

        std::string q2 = "select * where int64 decimal128 = 500";
        CHECK(should_insert(input_doc, q2) == true);

        std::string q3 = "select * where int64 int64 = 300";
        CHECK(should_insert(input_doc, q3) == true);

        std::string q5 = "select * where int64 date_time = 400";
        CHECK(should_insert(input_doc, q5) == false);

        std::string q6 = "select * where int64 int32 = 200";
        CHECK(should_insert(input_doc, q6) == true);
    }

    SECTION( "filter data type is double, should only check numeric data types in input_doc" ) {

        std::string q4 = "select * where double utf8 = 99";
        CHECK(should_insert(input_doc, q4) == false);

        std::string q1 = "select * where double double < 11";
        CHECK(should_insert(input_doc, q1) == true);

        std::string q2 = "select * where double decimal128 = 500";
        CHECK(should_insert(input_doc, q2) == true);

        std::string q3 = "select * where double int64 = 300";
        CHECK(should_insert(input_doc, q3) == true);

        std::string q5 = "select * where double date_time = 400";
        CHECK(should_insert(input_doc, q5) == false);

        std::string q6 = "select * where double int32 = 200";
        CHECK(should_insert(input_doc, q6) == true);
    }

    SECTION( "filter data type is decimal128, should only check numeric data types in input_doc" ) {

        std::string q4 = "select * where decimal128 utf8 = 99";
        CHECK(should_insert(input_doc, q4) == false);

        std::string q1 = "select * where decimal128 double < 11";
        CHECK(should_insert(input_doc, q1) == true);

        std::string q2 = "select * where decimal128 decimal128 = 500";
        CHECK(should_insert(input_doc, q2) == true);

        std::string q3 = "select * where decimal128 int64 = 300";
        CHECK(should_insert(input_doc, q3) == true);

        std::string q5 = "select * where decimal128 date_time = 400";
        CHECK(should_insert(input_doc, q5) == false);

        std::string q6 = "select * where decimal128 int32 = 200";
        CHECK(should_insert(input_doc, q6) == true);
    }

    SECTION( "filter data type is date_time, should only check date time in input_doc" ) {

        std::string q4 = "select * where date_time date_time = 400";
        CHECK(should_insert(input_doc, q4) == true);

        std::string q1 = "select * where date_time date_time <= 400";
        CHECK(should_insert(input_doc, q1) == true);

        std::string q2 = "select * where date_time date_time >= 400";
        CHECK(should_insert(input_doc, q2) == true);

        std::string q3 = "select * where date_time date_time < 400";
        CHECK(should_insert(input_doc, q3) == false);

        std::string q5 = "select * where date_time date_time > 400";
        CHECK(should_insert(input_doc, q5) == false);

        std::string q6 = "select * where date_time date_time != 400";
        CHECK(should_insert(input_doc, q6) == false);
    }

    SECTION( "filter data type is null, should only check null type in input_doc and ignore operators and term" ) {

        std::string q1 = "select * where null null !";
        CHECK(should_insert(input_doc, q1) == false);

        std::string q2 = "select * where null null *";
        CHECK(should_insert(input_doc, q2) == true);

        std::string q3 = "select * where decimal128 null = 300";
        CHECK(should_insert(input_doc, q3) == false);
    }

    SECTION( "filter data type is minkey, should only check minkey type in input_doc and ignore operators and term" ) {

        std::string q1 = "select * where minkey minkey !";
        CHECK(should_insert(input_doc, q1) == false);

        std::string q2 = "select * where minkey minkey *";
        CHECK(should_insert(input_doc, q2) == true);

        std::string q3 = "select * where decimal128 minkey = 300";
        CHECK(should_insert(input_doc, q3) == false);
    }

    SECTION( "filter data type is maxkey, should only check maxkey type in input_doc and ignore operators and term" ) {

        std::string q1 = "select * where maxkey maxkey !";
        CHECK(should_insert(input_doc, q1) == false);

        std::string q2 = "select * where maxkey maxkey *";
        CHECK(should_insert(input_doc, q2) == true);

        std::string q3 = "select * where decimal128 maxkey = 300";
        CHECK(should_insert(input_doc, q3) == false);
    }
    SECTION("do not have filter, just insert") {
        std::string q1 = "select *";
        CHECK(should_insert(input_doc, q1) == true);
    }
    SECTION("test quoted query") {
        std::string q1 = "select * where bool 'b o o l' = 0 and int32 'in t 32' = 1000 and bool bool = 1 and utf8 'utf 8' = 'utf 8'";
        CHECK(should_insert(input_doc, q1) == true);
    }
    SECTION("test quoted query") {
        std::string q1 = "select * where bool 'b o o l' = 0 and int32 'in t 32' = 1000 and bool bool = 1 and utf8 'utf 8' = 'utf 8'";
        CHECK(should_insert(input_doc, q1) == true);
    }SECTION("Test existing or not") {
        std::string q1 = "select * where bool 'b o o l' *";
        CHECK(should_insert(input_doc, q1) == true);

        std::string q2 = "select * where int32 'in t 32' !";
        CHECK(should_insert(input_doc, q2) == false);

        std::string q3 = "select * where code 'in t 32' !";
        CHECK(should_insert(input_doc, q3) == true);
    }

    SECTION("Test bool relation type") {
        std::string q1 = "select * where int32 int32 < 100000 and int32 int32 > -100";
        CHECK(should_insert(input_doc, q1) == true);

        std::string q2 = "select * where int32 eid !";
        CHECK(should_insert(input_doc, q2) == true);

        std::string q3 = "select * where int32 eid ! and int32 int32 >= 24";
        CHECK(should_insert(input_doc, q3) == true);
    }

    SECTION("Test dot notation query") {
        std::string q1 = "select * where int32 document.a.b.c = 1";
        CHECK(should_insert(input_doc, q1) == true);
        std::string q2 = "select * where int32 document.a.b.c > 0";
        CHECK(should_insert(input_doc, q2) == true);
        std::string q3 = "select * where int32 document.a.b.c < 5";
        CHECK(should_insert(input_doc, q3) == true);
        std::string q4 = "select * where int32 document.a.b.c *";
        CHECK(should_insert(input_doc, q4) == true);
        std::string q5 = "select * where int32 document.a.b.c !";
        CHECK(should_insert(input_doc, q5) == false);
        std::string q6 = "select * where int32 foo.bar.1.baz_1 = 1";
        CHECK(should_insert(input_doc, q6) == true);
        std::string q7 = "select * where array foo.bar *";
        CHECK(should_insert(input_doc, q7) == true);
    }

    delete(input_doc);
}

TEST_CASE("Test projections: select document.a.b.c,foo.bar.0.baz_0,int32", "[get_input_doc_if_satisfied_filter]") {
    bson_t *input_doc = generate_fixed_input_doc();

    std::string q1 = "select document.a.b.c,foo.bar,int32";
    const bson_t* output_doc_1 = get_input_doc_if_satisfied_filter(input_doc, q1);
    bson_t* valid_doc_1 = bson_new();
    bson_t* a = bson_new();
    bson_t* b = bson_new();
    bson_t* c = bson_new();
    BSON_APPEND_INT32(c, "c", 1);
    BSON_APPEND_DOCUMENT(b, "b", c);
    BSON_APPEND_DOCUMENT(a, "a", b);
    BSON_APPEND_DOCUMENT(valid_doc_1, "document", a);

    BSON_APPEND_INT32(valid_doc_1, "int32", 200);

    CHECK(is_identical(valid_doc_1, output_doc_1) == true);

    std::cout << "projection of input_doc: " << bson_as_json(output_doc_1, NULL) << std::endl;
    std::cout << "projection of valid doc: " << bson_as_json(valid_doc_1, NULL) << std::endl;

    if (input_doc == output_doc_1)
        delete (input_doc);
    else {
        delete (output_doc_1);
        delete (input_doc);
    }
    if (valid_doc_1)
        delete (valid_doc_1);
}


