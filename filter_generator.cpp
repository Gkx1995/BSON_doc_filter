//
// Created by kai on 8/3/18.
//

#include "filter_generator.h"

//////////////////////////////////////////////////////////
// Filter methods
//////////////////////////////////////////////////////////
using namespace CommonConstants;

// Constructor
Filter::Filter(std::string& query, const std::string& shard_keys) {

    perform_pegtl_parser(query, shard_keys);
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

void Filter::perform_pegtl_parser(std::string& query, const std::string& shard_keys) {

    auto* parser = new Parser();
    parser->perform_pegtl_parser(query, arg_map);

    generate_data_type_map();

    try {
        generate_filters();
    } catch (const char *msg) {
        std::cerr << msg << std::endl;
    }
    append_shard_keys_and_id(shard_keys);
}

void Filter::append_shard_keys_and_id(const std::string &shard_keys) {
    std::istringstream iss(shard_keys);
    std::string shard_key;
    std::string _id;

    while (std::getline(iss, shard_key, ' ')) {
        if (!shard_key.empty()
            && std::find(arg_map[SELECTED_FIELD_LIST].begin(), arg_map[SELECTED_FIELD_LIST].end(), shard_key) == arg_map[SELECTED_FIELD_LIST].end()) {
            arg_map[SELECTED_FIELD_LIST].push_back(shard_key);
            std::cout << "Query not included shard key. Adding shard key to select_fields_list: " << shard_key << std::endl;
        }
    }

    _id = "_id";
    if (std::find(arg_map[SELECTED_FIELD_LIST].begin(), arg_map[SELECTED_FIELD_LIST].end(), _id) == arg_map[SELECTED_FIELD_LIST].end()) {
        arg_map[SELECTED_FIELD_LIST].push_back(_id);
        std::cout << "Query not included _id. Adding shard key to select_fields_list: " << _id << std::endl;
    }
}

const bson_t* Filter::get_input_doc_if_satisfied_filter (const bson_t* input_doc) {
    Projector* projector = NULL;

    if (!should_insert(input_doc))
        return nullptr;

    projector = new Projector(arg_map[SELECTED_FIELD_LIST]);
    return projector->get_input_doc_if_satisfied_filter(input_doc);

}

bool Filter::should_insert(const bson_t* input_doc) {

    long filters_count = filters.size();
    long restrictions_count = arg_map[QUERY_FIELD_LIST].size();
    bool should_insert;
    auto* restrictions_satisfied_arr = new bool[restrictions_count];

    // no restrictions, all satisfied
    if (restrictions_count == 0)
        return true;

    for (long i = 0, filter_idx = 0; i < restrictions_count; i++) {
        int flag;
        bson_iter_t doc_iter;
        bson_iter_t target_iter;
        bson_iter_t filter_iter;
        std::string _field;
        std::string _operator;
        std::string _datatype;

        std::cout << "Input doc: " << bson_as_json(input_doc, NULL) << std::endl;


        _field = arg_map[QUERY_FIELD_LIST].at(i);
        _operator = arg_map[NUMERIC_OPERATOR_LIST].at(i);
        _datatype = arg_map[DATA_TYPE_LIST].at(i);

        // first handle * and ! operator
        if (_operator == "*" || _operator == "!") {
            bool exists;

            // check existence of field
            exists = bson_iter_init(&doc_iter, input_doc)
                     && bson_iter_find_descendant(&doc_iter, _field.c_str(), &target_iter)
                     && bson_iter_type(&target_iter) == data_type_map[_datatype];

            flag = exists ? 0 : IGNORE_NUM;
        }

            // handle normal operators
        else if (filter_idx < filters_count) {
            // check if the filter is a dot-notation key or not
            if (_field.find('.') != std::string::npos) {

                // check if input doc contains this dot field
                if (bson_iter_init(&doc_iter, input_doc) && bson_iter_find_descendant(&doc_iter, _field.c_str(), &target_iter)) {
                    int cmp_rst;
                    bson_iter_init(&filter_iter, filters.at(filter_idx));
                    bson_iter_next(&filter_iter);
//                std::cout << "target iter type: " << bson_iter_type(&target_iter) << ", filter iter type: " << bson_iter_type(&filter_iter) << std::endl;
                    cmp_rst = filter_compare_elements(&target_iter, &filter_iter);
                    if (cmp_rst == IGNORE_NUM) {
                        flag = IGNORE_NUM;
                    }
                    else if (cmp_rst > 0) {
                        flag = 1;
                    } else {
                        flag = cmp_rst < 0 ? -1 : 0;
                    }

                    // did not find this dot field
                } else {
                    flag = IGNORE_NUM;
                }
            } else {
                flag = filter_compare_object(input_doc, filters.at(filter_idx));
            }
            filter_idx++;
        }

        restrictions_satisfied_arr[i] = filter_satisfied(flag, _operator);
        std::cout << "restriction : " << i << ", flag: " << flag << ", satisfied : " << restrictions_satisfied_arr[i] << std::endl;
    }

    should_insert = restrictions_satisfied_arr[0];

    long bool_relations_size = arg_map[BOOL_OPERATOR_LIST].size();

    // only one filter
    if (bool_relations_size == 0) {
        return should_insert;
    }

    else if (restrictions_count > 0 && bool_relations_size == restrictions_count - 1) {

        should_insert = satisfy_query(restrictions_satisfied_arr);
    } else {
        //TODO: throw exception
    }

    delete[] restrictions_satisfied_arr;

    return should_insert;
}

bool Filter::satisfy_query(bool restrictions_satisfied_arr[]) {
    std::stack<std::string> bool_expr_stack;
    std::vector<std::string> bool_expr_list;
    bool satisfy_query;
    std::string bool_operator;
    std::string restriction;
    bool restriction_value;
    bool braced_value;
    std::string curt_symbol;
    std::string pushed_value;

    bool_expr_list = arg_map[BOOL_EXPR_LIST];
    satisfy_query = false;

    // we already checks the validation of braces in the checker, so we do not check again here
    for (int i = 0; i < bool_expr_list.size(); i++) {
        curt_symbol = bool_expr_list.at(i);
        if (curt_symbol != ")") {

            bool_expr_stack.push(curt_symbol);
//            std::cout << "stack pushed: " << curt_symbol << std::endl;

        }
            // we never push ")" into stack, only take it to backtrack latest "(" in stack
        else {

            // current top element must be a restriction
            // top element has not been modified and is still index of restrictions_satisfied_arr
            if (bool_expr_stack.top().find_first_not_of("0123456789") == std::string::npos) {
                braced_value = restrictions_satisfied_arr[stoi(bool_expr_stack.top())];

//                std::cout << "stack poped: " << bool_expr_stack.top() << std::endl;
                bool_expr_stack.pop();

                // else bool_expr_stack.top() should have been modified as "true" or "false"
            } else {
                braced_value = bool_expr_stack.top() == "true";

//                std::cout << "stack poped: " << bool_expr_stack.top() << std::endl;
                bool_expr_stack.pop();
            }

            while (!bool_expr_stack.empty() && bool_expr_stack.top() != "(") {

                // there must exist one or more [restriction, bool_operator] combinations
                bool_operator = bool_expr_stack.top();
//                std::cout << "stack poped: " << bool_expr_stack.top() << std::endl;
                bool_expr_stack.pop();

                restriction = bool_expr_stack.top();
//                std::cout << "stack poped: " << bool_expr_stack.top() << std::endl;
                bool_expr_stack.pop();

                if (restriction.find_first_not_of("0123456789") == std::string::npos) {
                    restriction_value = restrictions_satisfied_arr[stoi(restriction)];
                } else {
                    restriction_value = restriction == "true";
                }

                if (bool_operator == "|")
                    braced_value = braced_value || restriction_value;
                else
                    // else bool_operator is "&"
                    braced_value = braced_value && restriction_value;
            }

            if (!bool_expr_stack.empty() && bool_expr_stack.top() == "(") {

//                std::cout << "stack poped: " << bool_expr_stack.top() << std::endl;
                bool_expr_stack.pop();
                // the current deepest braced expression in stack is replaced with pushed value
                pushed_value = braced_value ? "true" : "false";

                bool_expr_stack.push(pushed_value);
//                std::cout << "stack pushed: " << bool_expr_stack.top() << std::endl;
            }

        }
    }

    if (!bool_expr_stack.empty()) {
        // deal with non-braced expression
        if (bool_expr_stack.top().find_first_not_of("0123456789") == std::string::npos) {
            braced_value = restrictions_satisfied_arr[stoi(bool_expr_stack.top())];
        } else {
            braced_value = bool_expr_stack.top() == "true";
        }
//        std::cout << "stack poped: " << bool_expr_stack.top() << std::endl;
        bool_expr_stack.pop();

        while (!bool_expr_stack.empty()) {

            // there must exist one or more [restriction, bool_operator] combinations
            bool_operator = bool_expr_stack.top();
//            std::cout << "stack poped: " << bool_expr_stack.top() << std::endl;
            bool_expr_stack.pop();

            restriction = bool_expr_stack.top();
//            std::cout << "stack poped: " << bool_expr_stack.top() << std::endl;
            bool_expr_stack.pop();

            if (restriction.find_first_not_of("0123456789") == std::string::npos) {
                restriction_value = restrictions_satisfied_arr[stoi(restriction)];
            } else {
                restriction_value = restriction == "true";
            }

            if (bool_operator == "|")
                braced_value = braced_value || restriction_value;
            else
                // else bool_operator is "&"
                braced_value = braced_value && restriction_value;
        }
    }

    satisfy_query = braced_value;

    std::cout << "input doc satisfy all the restrictions: " << satisfy_query << std::endl;
    return satisfy_query;
}

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

    if (arg_map.find(QUERY_FIELD_LIST) != arg_map.end() &&
        arg_map.find(QUERY_VALUE_LIST) != arg_map.end() &&
        arg_map.find(DATA_TYPE_LIST) != arg_map.end()) {

        std::vector<std::string> field_list = arg_map[QUERY_FIELD_LIST];
        std::vector<std::string> term_list = arg_map[QUERY_VALUE_LIST];
        std::vector<std::string> data_type_list = arg_map[DATA_TYPE_LIST];
        std::vector<std::string> _operator_list = arg_map[NUMERIC_OPERATOR_LIST];

        if (field_list.size() == term_list.size()
            && data_type_list.size() == term_list.size()) {
            long size = field_list.size();
            for (long i = 0; i < size; i++) {

                // we do not generate filter for "*" and "!", we directly handle them in should_insert()
                if (_operator_list.at(i) != "*" && _operator_list.at(i) != "!") {
                    try {
                        filters.push_back(generate_filter(field_list.at(i), term_list.at(i), data_type_list.at(i)));
                    } catch (const char *msg) {
                        std::cerr << msg << std::endl;
                    }
                }
            }
        } else {
            throw "Error: Query parsed wrong! Fields amount is not equal to term amount!";
        }
    } else {
        std::cout << "Only have \"*\" or \"!\" restrictions, no filter generated." << std::endl;
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

    return filter;
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
    bson_oid_t oid_oid;
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
        std::cerr << "Currently we do not support directly query for array type" << std::endl;
    }
    else if (_data_type == BSON_TYPE_DOCUMENT) {
        // TODO: need to specifically define this case
        std::cerr << "Currently we do not support directly query for document type" << std::endl;
    }
    else if (_data_type == BSON_TYPE_BINARY) {
        // TODO: need to specifically define this case
        std::cerr << "Currently we do not support directly query for binary type" << std::endl;
    }
    else if (_data_type == BSON_TYPE_UNDEFINED) {
        BSON_APPEND_UNDEFINED(b, field.c_str());
    }
    else if (_data_type == BSON_TYPE_OID) {
        bson_oid_init_from_string(&oid_oid, term.c_str());
        BSON_APPEND_OID(b, field.c_str(), &oid_oid);
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
        BSON_APPEND_NULL(b, field.c_str());
    }
    else if (_data_type == BSON_TYPE_REGEX) {
        // TODO: need to include [const char *options] as a param
        std::cerr << "Currently we do not support directly query for regex type" << std::endl;
    }
    else if (_data_type == BSON_TYPE_DBPOINTER) {
        //TODO: need to specifically define OID
        // Warning: The dbpointer field type is DEPRECATED and should only be used when interacting with legacy systems.
        std::cerr << "Currently we do not support directly query for dbpointer type" << std::endl;
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
        std::cerr << "Currently we do not support directly query for codewscope type" << std::endl;
    }
    else if (_data_type == BSON_TYPE_INT32) {
        BSON_APPEND_INT32(b, field.c_str(), std::stoi(term));
    }
    else if (_data_type == BSON_TYPE_INT64) {
        BSON_APPEND_INT64(b, field.c_str(), std::stoi(term));
    }
    else if (_data_type == BSON_TYPE_TIMESTAMP) {
        // This function is not similar in functionality to bson_append_date_time().
        // Timestamp elements are different in that they include only second precision and an increment field.
        // They are primarily used for intra-MongoDB server communication.
        std::istringstream iss(term);
        std::vector<std::string> tokens;
        std::string token;

        while (std::getline(iss, token, '_')) {
            if (!token.empty())
                tokens.push_back(token);
        }

        timestamp = strtoul(tokens.at(0).c_str(), NULL, 10);
        increment = strtoul(tokens.at(1).c_str(), NULL, 10);
        BSON_APPEND_TIMESTAMP(b, field.c_str(), timestamp, increment);
    }
    else if (_data_type == BSON_TYPE_DECIMAL128) {
        bson_decimal128_from_string(term.c_str(), &decimal128);
        BSON_APPEND_DECIMAL128(b, field.c_str(), &decimal128);
    }
    else if (_data_type == BSON_TYPE_MAXKEY) {
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
    bson_t* input_doc;
    bson_oid_t oid;
    bson_t* a = bson_new();
    bson_t* b = bson_new();
    bson_t* c = bson_new();

    bson_oid_init(&oid, NULL);
    input_doc = BCON_NEW("foo", "{", "bar", "[", "{", "baz_0", BCON_INT32 (0), "}", "{", "baz_1", BCON_INT32 (1), "}", "]", "}");
    BSON_APPEND_OID(input_doc, "_id", &oid);
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
    BSON_APPEND_UNDEFINED(input_doc, "undefined");

    BSON_APPEND_UTF8(input_doc, "utf 8", "utf 8");

    // generate nested element {document:{a: {b: {c: 1}}}}
    BSON_APPEND_INT32(c, "c", 1);
    BSON_APPEND_DOCUMENT(b, "b", c);
    BSON_APPEND_DOCUMENT(a, "a", b);
    BSON_APPEND_DOCUMENT(input_doc, "document", a);



    bson_decimal128_from_string("500", &decimal128);
    BSON_APPEND_DECIMAL128(input_doc, "decimal128", &decimal128);

    return input_doc;

}