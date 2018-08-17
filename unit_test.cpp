//
// Created by kai on 8/6/18.
//

#define CATCH_CONFIG_MAIN
#include "filter_generator.h"
#include "catch2/catch.hpp"


bson_t* generate_input_doc(unsigned long _data_type, std::string &field, std::string &term) {

    bson_t *b;
    const bson_t *sub_doc;
    const bson_t *array;
    bson_subtype_t binary_subtype;
    const uint8_t *binary;
    uint32_t binary_length;
    bson_oid_t *oid_oid;
    bool bool_flag;
    int64_t datetime_val;
    const char *reg_options, *regex;
    const char *collection;
    bson_oid_t *dbp_oid;
    const char *code_javascript;
    const char *cws_javascript;
    const bson_t *cws_scope;
    uint32_t timestamp;
    uint32_t increment;
    bson_decimal128_t decimal128;

    bson_t* input_doc = bson_new();

    if (_data_type == BSON_TYPE_EOD) {
        throw "Error: Filter not generated, data type should not be BSON_TYPE_EOD!";
    }
    else if (_data_type == BSON_TYPE_DOUBLE) {
        BSON_APPEND_DOUBLE(b, field.c_str(), std::stod(term));
    }
    else if (_data_type == BSON_TYPE_UTF8) {
        BSON_APPEND_UTF8(b, field.c_str(), term.c_str());
    }
    else if (_data_type == BSON_TYPE_DOCUMENT) {
        // TODO: need to specifically define this case
        sub_doc = nullptr;
        BSON_APPEND_DOCUMENT(b, field.c_str(), sub_doc);
    }
    else if (_data_type == BSON_TYPE_ARRAY) {
        // TODO: need to specifically define this case
        array = nullptr;
        BSON_APPEND_ARRAY(b, field.c_str(), array);
    }
    else if (_data_type == BSON_TYPE_BINARY) {
        // TODO: need to specifically define this case
        binary_subtype;
        binary = nullptr;
        binary_length;
        BSON_APPEND_BINARY(b, field.c_str(), binary_subtype, binary, binary_length);
    }
    else if (_data_type == BSON_TYPE_UNDEFINED) {
        throw "Error: Filter not generated, data type should not be BSON_TYPE_UNDEFINED!";
    }
    else if (_data_type == BSON_TYPE_OID) {
        // TODO: need to specifically define this case
        oid_oid = nullptr;
        BSON_APPEND_OID(b, field.c_str(), oid_oid);
    }
    else if (_data_type == BSON_TYPE_BOOL) {
        // "0"for false, and "1" or "true"
        bool_flag = term == "1";
        BSON_APPEND_BOOL(b, field.c_str(), bool_flag);
    }
    else if (_data_type == BSON_TYPE_DATE_TIME) {
        // value is assumed to be in UTC format of milliseconds since the UNIX epoch. value MAY be negative
        datetime_val = std::strtoll(term.c_str(), nullptr, 10);
        BSON_APPEND_DATE_TIME(b, field.c_str(), datetime_val);
    }
    else if (_data_type == BSON_TYPE_NULL) {
        //TODO: there is no term in this case, need to handle this
        BSON_APPEND_NULL(b, field.c_str());
    }
    else if (_data_type == BSON_TYPE_REGEX) {
        // TODO: need to include [const char *options] as a param
        reg_options = nullptr;
        regex = term.c_str();
        BSON_APPEND_REGEX(b, field.c_str(), regex, reg_options);
    }
    else if (_data_type == BSON_TYPE_DBPOINTER) {
        //TODO: need to specifically define OID
        // Warning: The dbpointer field type is DEPRECATED and should only be used when interacting with legacy systems.
        collection = term.c_str();
        dbp_oid = nullptr;
        BSON_APPEND_DBPOINTER(b, field.c_str(), collection, dbp_oid);
    }
    else if (_data_type == BSON_TYPE_CODE) {
        //javascript: A UTF-8 encoded string containing the javascript.
        code_javascript = term.c_str();
        BSON_APPEND_CODE(b, field.c_str(), code_javascript);
    }
    else if (_data_type == BSON_TYPE_SYMBOL) {
        // Appends a new field to bson of type BSON_TYPE_SYMBOL.
        // This BSON type is deprecated and should not be used in new code.
        BSON_APPEND_SYMBOL(b, field.c_str(), term.c_str());
    }
    else if (_data_type == BSON_TYPE_CODEWSCOPE) {
        //TODO: need to specifically define [const bson_t *scope]
        // scope: Optional bson_t containing the scope for javascript.
        cws_javascript = term.c_str();
        cws_scope = nullptr;
        BSON_APPEND_CODE_WITH_SCOPE(b, field.c_str(), cws_javascript, cws_scope);
    }
    else if (_data_type == BSON_TYPE_INT32) {
        BSON_APPEND_INT32(b, field.c_str(), std::stoi(term));
    }
    else if (_data_type == BSON_TYPE_INT64) {
        BSON_APPEND_INT64(b, field.c_str(), std::stoi(term));
    }
    else if (_data_type == BSON_TYPE_TIMESTAMP) {
        //TODO: need to specifically define [unit32_t timestamp] and [unit32_t increment]
        // This function is not similar in functionality to bson_append_date_time().
        // Timestamp elements are different in that they include only second precision and an increment field.
        // They are primarily used for intra-MongoDB server communication.
        timestamp = strtoul(term.c_str(), NULL, 10);
        increment;
        BSON_APPEND_TIMESTAMP(b, field.c_str(), timestamp, increment);
    }
    else if (_data_type == BSON_TYPE_DECIMAL128) {
        bson_decimal128_from_string(term.c_str(), &decimal128);
        BSON_APPEND_DECIMAL128(b, field.c_str(), &decimal128);
    }
    else if (_data_type == BSON_TYPE_MAXKEY) {
        //TODO: there is no term in this case, need to handle this
        BSON_APPEND_MAXKEY(b, field.c_str());
    }
    else if (_data_type == BSON_TYPE_MINKEY) {
        BSON_APPEND_MINKEY(b, field.c_str());
    }

    return b;
}

bson_t* generate_fixed_input_doc() {
    bson_decimal128_t decimal128;
    bson_t* input_doc = bson_new();
    bson_t* a = bson_new();
    bson_t* b = bson_new();
    bson_t* c = bson_new();

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

TEST_CASE("Test 6 numeric operators", "[should_insert]") {
    bson_t *input_doc = generate_fixed_input_doc();

    std::string q1 = "where int32 int32 = 200";
    CHECK(should_insert(input_doc, q1) == true);

    std::string q2 = "where int32 int32 > 200";
    CHECK(should_insert(input_doc, q2) == false);

    std::string q3 = "where int32 int32 < 200";
    CHECK(should_insert(input_doc, q3) == false);

    std::string q5 = "where int32 int32 != 200";
    CHECK(should_insert(input_doc, q5) == false);

    std::string q6 = "where int32 int32 >= 200";
    CHECK(should_insert(input_doc, q6) == true);

    std::string q7 = "where int32 int32 <= 200";
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
    std::string q1 = "where int32 int32 = 200";
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

        std::string q1 = "where utf8 utf8 = 99";
        CHECK(should_insert(input_doc, q1) == true);

        std::string q2 = "where utf8 utf8 < 98";
        CHECK(should_insert(input_doc, q2) == false);

        std::string q3 = "where utf8 utf8 > 97";
        CHECK(should_insert(input_doc, q3) == true);

        std::string q5 = "where utf8 int32 = 99";
        CHECK(should_insert(input_doc, q5) == false);
    }
    SECTION( "filter data type is symbol, should only check symbol element in input_doc" ) {

        std::string q1 = "where symbol symbol = ***";
        CHECK(should_insert(input_doc, q1) == true);

        std::string q2 = "where symbol symbol = **";
        CHECK(should_insert(input_doc, q2) == false);

        std::string q3 = "where symbol symbol != **";
        CHECK(should_insert(input_doc, q3) == true);
    }
    SECTION( "filter data type is int32 type, should only check numeric data types in input_doc" ) {

        std::string q4 = "where int32 utf8 = 99";
        CHECK(should_insert(input_doc, q4) == false);

        std::string q1 = "where int32 double < 11";
        CHECK(should_insert(input_doc, q1) == true);

        std::string q2 = "where int32 decimal128 = 500";
        CHECK(should_insert(input_doc, q2) == true);

        std::string q3 = "where int32 int64 = 300";
        CHECK(should_insert(input_doc, q3) == true);

        std::string q5 = "where int32 date_time = 400";
        CHECK(should_insert(input_doc, q5) == false);

        std::string q6 = "where int32 int32 = 200";
        CHECK(should_insert(input_doc, q6) == true);
    }
    SECTION( "filter data type is int64, should only check numeric data types in input_doc" ) {

        std::string q4 = "where int64 utf8 = 99";
        CHECK(should_insert(input_doc, q4) == false);

        std::string q1 = "where int64 double < 11";
        CHECK(should_insert(input_doc, q1) == true);

        std::string q2 = "where int64 decimal128 = 500";
        CHECK(should_insert(input_doc, q2) == true);

        std::string q3 = "where int64 int64 = 300";
        CHECK(should_insert(input_doc, q3) == true);

        std::string q5 = "where int64 date_time = 400";
        CHECK(should_insert(input_doc, q5) == false);

        std::string q6 = "where int64 int32 = 200";
        CHECK(should_insert(input_doc, q6) == true);
    }

    SECTION( "filter data type is double, should only check numeric data types in input_doc" ) {

        std::string q4 = "where double utf8 = 99";
        CHECK(should_insert(input_doc, q4) == false);

        std::string q1 = "where double double < 11";
        CHECK(should_insert(input_doc, q1) == true);

        std::string q2 = "where double decimal128 = 500";
        CHECK(should_insert(input_doc, q2) == true);

        std::string q3 = "where double int64 = 300";
        CHECK(should_insert(input_doc, q3) == true);

        std::string q5 = "where double date_time = 400";
        CHECK(should_insert(input_doc, q5) == false);

        std::string q6 = "where double int32 = 200";
        CHECK(should_insert(input_doc, q6) == true);
    }

    SECTION( "filter data type is decimal128, should only check numeric data types in input_doc" ) {

        std::string q4 = "where decimal128 utf8 = 99";
        CHECK(should_insert(input_doc, q4) == false);

        std::string q1 = "where decimal128 double < 11";
        CHECK(should_insert(input_doc, q1) == true);

        std::string q2 = "where decimal128 decimal128 = 500";
        CHECK(should_insert(input_doc, q2) == true);

        std::string q3 = "where decimal128 int64 = 300";
        CHECK(should_insert(input_doc, q3) == true);

        std::string q5 = "where decimal128 date_time = 400";
        CHECK(should_insert(input_doc, q5) == false);

        std::string q6 = "where decimal128 int32 = 200";
        CHECK(should_insert(input_doc, q6) == true);
    }

    SECTION( "filter data type is date_time, should only check date time in input_doc" ) {

        std::string q4 = "where date_time date_time = 400";
        CHECK(should_insert(input_doc, q4) == true);

        std::string q1 = "where date_time date_time <= 400";
        CHECK(should_insert(input_doc, q1) == true);

        std::string q2 = "where date_time date_time >= 400";
        CHECK(should_insert(input_doc, q2) == true);

        std::string q3 = "where date_time date_time < 400";
        CHECK(should_insert(input_doc, q3) == false);

        std::string q5 = "where date_time date_time > 400";
        CHECK(should_insert(input_doc, q5) == false);

        std::string q6 = "where date_time date_time != 400";
        CHECK(should_insert(input_doc, q6) == false);
    }

    SECTION( "filter data type is null, should only check null type in input_doc and ignore operators and term" ) {

        std::string q1 = "where null null !";
        CHECK(should_insert(input_doc, q1) == false);

        std::string q2 = "where null null *";
        CHECK(should_insert(input_doc, q2) == true);

        std::string q3 = "where decimal128 null = 300";
        CHECK(should_insert(input_doc, q3) == false);
    }

    SECTION( "filter data type is minkey, should only check minkey type in input_doc and ignore operators and term" ) {

        std::string q1 = "where minkey minkey !";
        CHECK(should_insert(input_doc, q1) == false);

        std::string q2 = "where minkey minkey = *";
        CHECK(should_insert(input_doc, q2) == true);

        std::string q3 = "where decimal128 minkey = 300";
        CHECK(should_insert(input_doc, q3) == false);
    }

    SECTION( "filter data type is maxkey, should only check maxkey type in input_doc and ignore operators and term" ) {

        std::string q1 = "where maxkey maxkey !";
        CHECK(should_insert(input_doc, q1) == false);

        std::string q2 = "where maxkey maxkey *";
        CHECK(should_insert(input_doc, q2) == true);

        std::string q3 = "where decimal128 maxkey = 300";
        CHECK(should_insert(input_doc, q3) == false);
    }
    SECTION("do not have filter, just insert") {
        std::string q1 = "where *";
        CHECK(should_insert(input_doc, q1) == true);
    }
    SECTION("test quoted query") {
        std::string q1 = "where bool 'b o o l' = 0 and int32 'in t 32' = 1000 and bool bool = 1 and utf8 'utf 8' = 'utf 8'";
        CHECK(should_insert(input_doc, q1) == true);
    }
    SECTION("test quoted query") {
        std::string q1 = "where bool 'b o o l' = 0 and int32 'in t 32' = 1000 and bool bool = 1 and utf8 'utf 8' = 'utf 8'";
        CHECK(should_insert(input_doc, q1) == true);
    }SECTION("Test existing or not") {
        std::string q1 = "where bool 'b o o l' *";
        CHECK(should_insert(input_doc, q1) == true);

        std::string q2 = "where int32 'in t 32' !";
        CHECK(should_insert(input_doc, q2) == false);

        std::string q3 = "where code 'in t 32' !";
        CHECK(should_insert(input_doc, q3) == true);
    }

    SECTION("Test bool relation type") {
        std::string q1 = "where int32 int32 < 100000 and int32 int32 > -100";
        CHECK(should_insert(input_doc, q1) == true);

        std::string q2 = "where int32 eid !";
        CHECK(should_insert(input_doc, q2) == true);

        std::string q3 = "where int32 eid ! and int32 int32 >= 24";
        CHECK(should_insert(input_doc, q3) == true);
    }

    SECTION("Test nested query") {
        std::string q1 = "where int32 document.a.b.c = 1";
        CHECK(should_insert(input_doc, q1) == true);
        std::string q2 = "where int32 document.a.b.c > 0";
        CHECK(should_insert(input_doc, q2) == true);
        std::string q3 = "where int32 document.a.b.c < 5";
        CHECK(should_insert(input_doc, q3) == true);
    }


    delete(input_doc);
}


