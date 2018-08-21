//
// Created by kai on 8/3/18.
//

#include <tao/pegtl.hpp>
#include "filter_generator.h"

//////////////////////////////////////////////////////////
// PEGTL rules and actions
//////////////////////////////////////////////////////////

namespace tao {
    namespace pegtl {
        namespace test {

            const std::string PLACE_HOLDER = "0";
            //////////////////////////////////////////////////////////
            // PEGTL rules
            //////////////////////////////////////////////////////////




            struct _select: sor<TAO_PEGTL_STRING("select"), TAO_PEGTL_STRING("SELECT")>{};
            struct _where : sor<string<'W', 'H', 'E', 'R', 'E'>, string<'w', 'h', 'e', 'r', 'e'>> {};
            struct _and: sor<string<'A', 'N', 'D'>, string<'a', 'n', 'd'>>{};
            struct _or: sor<string<'O', 'R'>, string<'o', 'r'>>{};

            struct _all: string<'*'>{};
            struct boolType: sor<_and, _or>{};

            //////////////////////////////////////////////////////////
            // define data type rules
            //////////////////////////////////////////////////////////

            struct _EOD: TAO_PEGTL_STRING("eod"){};
            struct _DOUBLE: TAO_PEGTL_STRING("double"){};
            struct _UTF8: TAO_PEGTL_STRING("utf8"){};
            struct _DOCUMENT: TAO_PEGTL_STRING("document"){};
            struct _ARRAY: TAO_PEGTL_STRING("array"){};
            struct _BINARY: TAO_PEGTL_STRING("binary"){};
            struct _UNDEFINED: TAO_PEGTL_STRING("undefined"){};
            struct _OID: TAO_PEGTL_STRING("oid"){};
            struct _BOOL: TAO_PEGTL_STRING("bool"){};
            struct _DATE_TIME: TAO_PEGTL_STRING("date_time"){};
            struct _NULL: TAO_PEGTL_STRING("null"){};
            struct _REGEX: TAO_PEGTL_STRING("regex"){};
            struct _DBPOINTER: TAO_PEGTL_STRING("dbpointer"){};
            struct _CODE: TAO_PEGTL_STRING("code"){};
            struct _SYMBOL: TAO_PEGTL_STRING("symbol"){};
            struct _CODEWSCOPE: TAO_PEGTL_STRING("codewscope"){};
            struct _INT32: TAO_PEGTL_STRING("int32"){};
            struct _TIMESTAMP: TAO_PEGTL_STRING("timestamp"){};
            struct _INT64: TAO_PEGTL_STRING("int64"){};
            struct _DECIMAL128: TAO_PEGTL_STRING("decimal128"){};
            struct _MAXKEY: TAO_PEGTL_STRING("maxkey"){};
            struct _MINKEY: TAO_PEGTL_STRING("minkey"){};

            struct dataType: sor<_EOD, _DOUBLE, _UTF8, _DOCUMENT, _ARRAY, _BINARY, _UNDEFINED,
                    _OID, _BOOL, _DATE_TIME, _NULL, _REGEX, _DBPOINTER, _CODE, _SYMBOL, _CODEWSCOPE,
                    _INT32, _TIMESTAMP, _INT64, _DECIMAL128, _MAXKEY, _MINKEY>{};

            // Matches and consumes any single true ASCII character in [32, 59] and [63, 126]
            // terms or fields without white space could either be quoted by single quote or not;
            // terms or fields with white space should be quoted by single quote, which means we could not
            // accept terms or fields with single quotes inside them
            struct not_quoted_field: plus<not_one<32>> {};
            struct nested_field: plus<not_one<39>>{};
            struct single_quoted_field: seq<one<39>, nested_field,  one<39>> {};
            struct field: sor<single_quoted_field, not_quoted_field>{};

            struct not_exist: string<'!'>{};
            struct _exist: string<'*'>{};
            struct exist_or_not: sor<not_exist, _exist>{};
            struct relationType: sor<string<'<', '='>, string<'>', '='>, string<'!', '='>, string<'>'>,
                    string<'<'>, string<'='>> {};

            struct not_quoted_term: plus<not_one<32>> {};
            struct nested_term: plus<not_one<39>> {};
            struct single_quoted_term: seq<one<39>, nested_term,  one<39>> {};
            struct term: sor<single_quoted_term, not_quoted_term>{};
            struct select_field: plus<not_one<' ', ','>> {};
            struct restriction: seq<dataType, blank, field, blank, sor< seq<relationType, blank, term>, exist_or_not>> {};
            struct select_clause: seq<_select, blank, select_field, star<seq<one<44>, select_field>>>{};
            struct grammar: must<select_clause, blank, _where, blank, sor<_all, seq<restriction, star<blank, boolType, blank, restriction>>>, eof>{};


            //////////////////////////////////////////////////////////
            // PEGTL actions
            //////////////////////////////////////////////////////////

            // Class template for user-defined actions that does nothing by default
            template <typename Rule>
            struct action: nothing<Rule> {};

            // Specialisation of the user-defined action to do something when the 'name' rule succeeds;
            // is called with the portion of the input that matched the rule.
            template<> struct action< select_field > {
                template <typename Input>
                static void apply(const Input& in, std::map<std::string, std::vector<std::string>>& arg_map) {
                    arg_map["selected"].push_back(in.string());

                    std::cout << "selected matched: " << in.string() << std::endl;
                }
            };
            template<> struct action< nested_field > {
                template <typename Input>
                static void apply(const Input& in, std::map<std::string, std::vector<std::string>>& arg_map) {
                    arg_map["field"].push_back(in.string());

                    std::cout << "field matched: " << in.string() << std::endl;
                }
            };

            template<> struct action< not_quoted_field > {
                template <typename Input>
                static void apply(const Input& in, std::map<std::string, std::vector<std::string>>& arg_map) {
                    arg_map["field"].push_back(in.string());

                    std::cout << "field matched: " << in.string() << std::endl;
                }
            };

            template<> struct action< boolType > {
                template <typename Input>
                static void apply(const Input& in, std::map<std::string, std::vector<std::string>>& arg_map) {
                    arg_map["boolType"].push_back(in.string());

                    std::cout << "boolType matched: " << in.string() << std::endl;
                }
            };

            template<> struct action< nested_term > {
                template <typename Input>
                static void apply(const Input& in, std::map<std::string, std::vector<std::string>>& arg_map) {
                    arg_map["term"].push_back(in.string());

                    std::cout << "term matched: " << in.string() << std::endl;
                }
            };

            template<> struct action< not_quoted_term > {
                template <typename Input>
                static void apply(const Input& in, std::map<std::string, std::vector<std::string>>& arg_map) {
                    arg_map["term"].push_back(in.string());

                    std::cout << "term matched: " << in.string() << std::endl;
                }
            };

            template<> struct action< relationType > {
                template <typename Input>
                static void apply(const Input& in, std::map<std::string, std::vector<std::string>>& arg_map) {
                    arg_map["relationType"].push_back(in.string());

                    std::cout << "relationType matched: " << in.string() << std::endl;
                }
            };

            template<> struct action< exist_or_not > {
                template <typename Input>
                static void apply(const Input& in, std::map<std::string, std::vector<std::string>>& arg_map) {
                    arg_map["relationType"].push_back(in.string());
                    arg_map["term"].push_back(PLACE_HOLDER);

                    std::cout << "relationType matched: " << in.string() << std::endl;
                }
            };

            template<> struct action< dataType > {
                template <typename Input>
                static void apply(const Input& in, std::map<std::string, std::vector<std::string>>& arg_map) {
                    arg_map["dataType"].push_back(in.string());

                    std::cout << "dataType matched: " << in.string() << std::endl;
                }
            };

        }
    }
}

//////////////////////////////////////////////////////////
// Filter methods
//////////////////////////////////////////////////////////

// Constructor
Filter::Filter(std::string& query) {

    perform_pegtl_parser(query);
}

// Destructor
Filter::~Filter() {
    long filters_size;

    filters_size = filters.size();

    for (auto i = 0; i < filters_size; i++) {
        bson_destroy(filters.at(i));
    }
    delete(&arg_map);
}

const bson_t* Filter::get_input_doc_if_satisfied_filter (const bson_t* input_doc) {
    bson_t* returned_doc;
    long selected_num;
    std::vector<std::string> selected_list;

    if (!should_insert(input_doc))
        return nullptr;

    selected_list = arg_map["selected"];
    selected_num = selected_list.size();
    std::cout << "selected_num = " << selected_num << std::endl;

    if (selected_num == 0 || selected_list.at(0) == "*")
        return input_doc;

    returned_doc = bson_new();
    for (long i = 0; i < selected_num; ++i) {
        std::istringstream iss(selected_list.at(i));
        std::vector<std::string> tokens;
        std::string token;
        bson_iter_t iter;
        bson_t* element_doc;

        while (std::getline(iss, token, '.')) {
            if (!token.empty())
                tokens.push_back(token);
        }

        bson_iter_init(&iter, input_doc);
        for (int j = 0; j < tokens.size(); j++) {
            bson_iter_find(&iter, tokens.at(j).c_str());
        }

        if (tokens.size() == 1) {
            generate_basic_element_doc(returned_doc, &iter);
        } else {
            element_doc = bson_new();
            generate_basic_element_doc(element_doc, &iter);
            for (long j = tokens.size() - 2; j > 0; j--) {
                element_doc = append_document(element_doc, tokens.at(j));
            }
            BSON_APPEND_DOCUMENT(returned_doc, tokens.at(0).c_str(), element_doc);
        }
    }

    return returned_doc;
}

void Filter::generate_basic_element_doc(bson_t* b, bson_iter_t* iter) {
    const char* key;
    bson_type_t type;
    const bson_value_t* value;

    key = bson_iter_key(iter);
    type = bson_iter_type(iter);
    value = bson_iter_value(iter);

    switch (type) {
        case BSON_TYPE_EOD:
            return;

        case BSON_TYPE_DOUBLE:
            BSON_APPEND_DOUBLE(b, key, value->value.v_double);
            break;

        case BSON_TYPE_UTF8:
            BSON_APPEND_UTF8(b, key, value->value.v_utf8.str);
            break;

        case BSON_TYPE_ARRAY: {
            uint32_t* array_len = 0;
            const uint8_t** array = NULL;
            bson_iter_array(iter, array_len, array);
            BSON_APPEND_ARRAY(b, key, bson_new_from_data(*array, *array_len));
            break;
        }
        case BSON_TYPE_BINARY:
            BSON_APPEND_BINARY(b, key, value->value.v_binary.subtype, value->value.v_binary.data, value->value.v_binary.data_len);
            break;

        case BSON_TYPE_UNDEFINED:
            return;

        case BSON_TYPE_OID:
            BSON_APPEND_OID(b, key, &value->value.v_oid);
            break;

        case BSON_TYPE_BOOL:
            BSON_APPEND_BOOL(b, key, value->value.v_bool);
            break;

        case BSON_TYPE_DATE_TIME:
            BSON_APPEND_DATE_TIME(b, key, value->value.v_datetime);
            break;

        case BSON_TYPE_NULL:
            BSON_APPEND_NULL(b, key);
            break;

        case BSON_TYPE_REGEX:
            BSON_APPEND_REGEX(b, key, value->value.v_regex.regex, value->value.v_regex.options);
            break;

        case BSON_TYPE_DBPOINTER:
            BSON_APPEND_DBPOINTER(b, key, value->value.v_dbpointer.collection, &value->value.v_dbpointer.oid);
            break;

        case BSON_TYPE_CODE:
            BSON_APPEND_CODE(b, key, value->value.v_code.code);
            break;

        case BSON_TYPE_SYMBOL:
            BSON_APPEND_SYMBOL(b, key, value->value.v_symbol.symbol);
            break;

        case BSON_TYPE_CODEWSCOPE:
            BSON_APPEND_CODE_WITH_SCOPE(b, key, value->value.v_codewscope.code,
                    bson_new_from_data(value->value.v_codewscope.scope_data, value->value.v_codewscope.scope_len));
            break;

        case BSON_TYPE_INT32:
            BSON_APPEND_INT32(b, key, value->value.v_int32);
            break;

        case BSON_TYPE_INT64:
            BSON_APPEND_INT64(b, key, value->value.v_int64);
            break;

        case BSON_TYPE_TIMESTAMP:
            BSON_APPEND_TIMESTAMP(b, key, value->value.v_timestamp.timestamp, value->value.v_timestamp.increment);
            break;

        case BSON_TYPE_DECIMAL128: {
            bson_decimal128_t* dec = NULL;
            bson_iter_decimal128(iter, dec);
            BSON_APPEND_DECIMAL128(b, key, dec);
            break;

        }
        case BSON_TYPE_MAXKEY:
            BSON_APPEND_MAXKEY(b, key);
            break;

        case BSON_TYPE_MINKEY:
            BSON_APPEND_MINKEY(b, key);
            break;
    }

    std::cout << "basic element doc generated: " << bson_as_json(b, NULL) << std::endl;
}

bool Filter::should_insert(const bson_t* input_doc) {

    long filters_size = filters.size();
    bool should_insert;

    // no filters, all satisfied
    if (filters_size == 0)
        return true;

    auto* filter_satisfied_arr = new bool[filters_size];

    for (long i = 0; i < filters_size; i++) {
        std::cout << "Input doc: " << bson_as_json(input_doc, NULL) << std::endl;
        int flag = filter_compare_object(input_doc, filters.at(i));
        std::cout << "filter: " << bson_as_json(filters.at(i), NULL) << ", flag: " << flag << std::endl;

//        if (flag == IGNORE_NUM) {
//            if (arg_map["relationType"].at(i) == "!")
//                filter_satisfied_arr[i] = true;
//            else
//                filter_satisfied_arr[i] = false;
//        } else {
//            filter_satisfied_arr[i] = filter_satisfied(flag, arg_map["relationType"].at(i));
//        }
        filter_satisfied_arr[i] = filter_satisfied(flag, arg_map["relationType"].at(i));
        std::cout << "filter: " << bson_as_json(filters.at(i), NULL) << " satisfied : " << filter_satisfied_arr[i] << std::endl;
    }

    should_insert = filter_satisfied_arr[0];

    long bool_relations_size = arg_map["boolType"].size();

    // only one filter
    if (bool_relations_size == 0) {
        return should_insert;
    }

    if (filters_size > 0 && bool_relations_size == filters_size - 1) {

        for (long i = 0; i < bool_relations_size; i++) {

            if (arg_map["boolType"].at(i) == "and") {
                should_insert = should_insert && filter_satisfied_arr[i + 1];
            }
            else if (arg_map["boolType"].at(i) == "or") {
                should_insert = should_insert || filter_satisfied_arr[i + 1];
            }
        }
    } else {
        //TODO: throw exception
    }

    delete[] filter_satisfied_arr;

    return should_insert;
}

void Filter::perform_pegtl_parser(std::string& query) {

    // generate memory_input
    tao::pegtl::memory_input<> in(query, "");

    // apply input and generate arg_map
    tao::pegtl::parse<tao::pegtl::test::grammar, tao::pegtl::test::action> (in, arg_map);

    generate_data_type_map();

    try {
        generate_filters();
    } catch (const char *msg) {
        std::cerr << msg << std::endl;
    }
};

void Filter:: generate_data_type_map() {
    // generate data type map
    data_type_map["eod"] = BSON_TYPE_EOD;
    data_type_map["double"] = BSON_TYPE_DOUBLE;
    data_type_map["utf8"] = BSON_TYPE_UTF8;
    data_type_map["document"] = BSON_TYPE_DOCUMENT;
    data_type_map["array"] = BSON_TYPE_ARRAY;
    data_type_map["binary"] = BSON_TYPE_BINARY;
    data_type_map["undefined"] = BSON_TYPE_UNDEFINED;
    data_type_map["oid"] = BSON_TYPE_OID;
    data_type_map["bool"] = BSON_TYPE_BOOL;
    data_type_map["date_time"] = BSON_TYPE_DATE_TIME;
    data_type_map["null"] = BSON_TYPE_NULL;
    data_type_map["regex"] = BSON_TYPE_REGEX;
    data_type_map["dbpointer"] = BSON_TYPE_DBPOINTER;
    data_type_map["code"] = BSON_TYPE_CODE;
    data_type_map["symbol"] = BSON_TYPE_SYMBOL;
    data_type_map["codewscope"] = BSON_TYPE_CODEWSCOPE;
    data_type_map["int32"] = BSON_TYPE_INT32;
    data_type_map["timestamp"] = BSON_TYPE_TIMESTAMP;
    data_type_map["int64"] = BSON_TYPE_INT64;
    data_type_map["decimal128"] = BSON_TYPE_DECIMAL128;
    data_type_map["maxkey"] = BSON_TYPE_MAXKEY;
    data_type_map["minkey"] = BSON_TYPE_MINKEY;
}

void Filter::generate_filters() {

    // no filters, just insert it
    if (arg_map.empty())
        return;

    if (arg_map.find("field") != arg_map.end() &&
        arg_map.find("term") != arg_map.end() &&
        arg_map.find("dataType") != arg_map.end()) {

        std::vector<std::string> field_list = arg_map["field"];
        std::vector<std::string> term_list = arg_map["term"];
        std::vector<std::string> data_type_list = arg_map["dataType"];

        if (field_list.size() == term_list.size() && data_type_list.size() == term_list.size()) {

            long size = term_list.size();
            for (long i = 0; i < size; i++) {

                try {
                    filters.push_back(generate_filter(field_list.at(i), term_list.at(i), data_type_list.at(i)));
                } catch (const char *msg) {
                    std::cerr << msg << std::endl;
                }
            }
        } else {
            //TODO: throw exceptions
            throw "Error: Query parsed wrong! Fields amount is not equal to term amount!";
        }
    } else {
        //TODO: throw not found exceptions
        throw "Error: Query parsed wrong! Missing field or term or dataType!";
    }
}


bson_t* Filter::generate_filter(std::string& field, std::string& term, std::string& dataType) {
    std::istringstream iss(field);
    std::vector<std::string> tokens;
    std::string token;
    long size;
    bson_t* filter;

    while (std::getline(iss, token, '.')) {
        if (!token.empty()) {
            tokens.push_back(token);
        }
    }
    size = tokens.size();
    filter = generate_unnested_filter(tokens.at(size - 1), term, dataType);

    for (long i = size - 2; i >= 0; --i) {
        filter = append_document(filter, tokens.at(i));
//        std::cout << "index: " << i << std::endl;
    }

    return filter;
}

bson_t* Filter::append_document(bson_t* bson_doc, std::string& field) {
    bson_t* return_doc;
    return_doc = bson_new();
    BSON_APPEND_DOCUMENT(return_doc, field.c_str(), bson_doc);
    std::cout << "nested doc appended: " << bson_as_json(return_doc, NULL) << std::endl;
    return return_doc;
}

// do not support nested query:
// BSON_TYPE_DOCUMENT, BSON_TYPE_ARRAY, BSON_TYPE_REGEX, BSON_TYPE_CODEWSCOPE, BSON_TYPE_CODE

// also do not support: BSON_TYPE_OID, BSON_TYPE_DBPOINTER, BSON_TYPE_BINARY, BSON_TYPE_EOD, BSON_TYPE_UNDEFINED,  BSON_TYPE_TIMESTAMP

// support: BSON_TYPE_DOUBLE, BSON_TYPE_UTF8, BSON_TYPE_BOOL, BSON_TYPE_DATE_TIME, BSON_TYPE_NULL, BSON_TYPE_SYMBOL
// BSON_TYPE_INT32, BSON_TYPE_TIMESTAMP, BSON_TYPE_DECIMAL128, BSON_TYPE_MAXKEY, BSON_TYPE_MINKEY
bson_t* Filter::generate_unnested_filter(std::string& field, std::string& term, std::string& dataType) {

    bson_t* b;
    unsigned long _data_type;
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

    b = bson_new();
//    _data_type = std::stoul(dataType, nullptr, 16);
    _data_type = data_type_map[dataType];

    if (_data_type == BSON_TYPE_EOD) {
        throw "Error: Filter not generated, data type should not be BSON_TYPE_EOD!";
    }
    else if (_data_type == BSON_TYPE_DOUBLE) {
        BSON_APPEND_DOUBLE(b, field.c_str(), std::stod(term));
    }
    else if (_data_type == BSON_TYPE_UTF8) {
        BSON_APPEND_UTF8(b, field.c_str(), term.c_str());
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

    std::cout << "filter generated: " << bson_as_json(b, NULL) << std::endl;

    return b;
}


bool Filter::filter_satisfied (int flag, std::string& _operator) {

    if (flag == 1) {
        return _operator == ">=" || _operator == ">" || _operator == "!=" || _operator == "*";
    }
    else if (flag == 0) {
        return _operator == "=" || _operator == ">=" || _operator == "<=" || _operator == "*";
    }
    else if (flag == -1) {
        return _operator == "<=" || _operator == "<" || _operator == "!=" || _operator == "*";
    } else if (flag == IGNORE_NUM) {
        return _operator == "!";
    }
    // this statement should not be reached since we only have 4 flags
    return false;
}

void Filter::print_map() {
    for (auto map_it = arg_map.begin(); map_it != arg_map.end(); ++map_it) {
        std::cout << "\n\nkey: " << map_it -> first << std::endl;
        for (auto it = map_it -> second.begin() ; it != map_it -> second.end(); ++it)
            std::cout << "\t" << *it;
    }
    std::cout << std::endl;
}

void Filter::print_filters() {
    unsigned long size = filters.size();
    std::cout << "We have " << size << " filters!" << std::endl;

    if (size != 0) {
        for (auto it = filters.begin(); it != filters.end(); it++) {
            std::cout << "\t" << bson_as_json(*it, NULL) << std::endl;
        }
        std::cout << std::endl;
    }
}

bson_t* Filter::generate_input_doc() {
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