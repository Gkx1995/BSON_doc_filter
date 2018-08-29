//
// Created by kai on 8/28/18.
//

#include "projection_generator.h"

// Constructor
Projector::Projector(std::vector<std::string> &selected_fields_list) {
    this->selected_fields_list = selected_fields_list;
}

// Destructor
Projector::~Projector() {}

const bson_t* Projector::get_input_doc_if_satisfied_filter (const bson_t* input_doc) {
    bson_t* returned_doc;
    long selected_num;
    long valid_selected_num;

//    selected_list = arg_map["selected"];
    selected_num = selected_fields_list.size();
    valid_selected_num = selected_num;
    std::cout << "selected_num = " << selected_num << std::endl;

    if (selected_num == 0 || selected_fields_list.at(0) == "*")
        return input_doc;

    returned_doc = bson_new();
    // find and append OId
    if (find_and_append_unique_id(returned_doc, input_doc)) {

        for (long i = 0; i < selected_num; ++i) {
            std::istringstream iss(selected_fields_list.at(i));
            std::vector<std::string> tokens;
            std::string token, last_token;
            bson_iter_t iter, last_token_iter;
            bson_t* element_doc;
            bool valid_field;

            valid_field = true;
            while (std::getline(iss, token, '.')) {
                if (!token.empty())
                    tokens.push_back(token);
            }

            bson_iter_init(&iter, input_doc);
            last_token = tokens.at(tokens.size() - 1);

            // input_doc does not contain this field, ignore this selected field
            if (!bson_iter_find_descendant(&iter, selected_fields_list.at(i).c_str(), &last_token_iter)) {
                std::cout << "field did not find: " << selected_fields_list.at(i) << ". Will ignore this selected field for projection." << std::endl;
                valid_field = false;
            }

            // ignore this selected field and loop next one, decrease valid num
            if (!valid_field) {
                valid_selected_num--;
                continue;
            }

            if (tokens.size() == 1) {
                generate_basic_element_doc(returned_doc, &last_token_iter);
            } else {
                element_doc = bson_new();
                generate_basic_element_doc(element_doc, &last_token_iter);
                for (long j = tokens.size() - 2; j > 0; j--) {

                    // this token contains only digit and is supposed to be appended as array
                    if (tokens.at(j).find_first_not_of("0123456789") == std::string::npos && j > 0) {
                        std::string arr_key = "0";
                        element_doc = append_document(element_doc, arr_key);
                        element_doc = append_array(element_doc, tokens.at(--j));
                    }
                    else
                        element_doc = append_document(element_doc, tokens.at(j));
                }
                BSON_APPEND_DOCUMENT(returned_doc, tokens.at(0).c_str(), element_doc);
            }
        }

        if (valid_selected_num > 0)
            return returned_doc;
    }
    return nullptr;
}

bool Projector::find_and_append_unique_id(bson_t* returned_doc, const bson_t* input_doc) {
    bson_iter_t iter;
    const char* key;
    bson_type_t type;
    const bson_value_t* value;

    if (bson_iter_init(&iter, input_doc)) {

        while (bson_iter_next(&iter)) {
            type = bson_iter_type(&iter);
            key = bson_iter_key(&iter);
            if (strncmp(key, "_id", 5) == 0) {

                value = bson_iter_value(&iter);
                std::cout << "_id found for this input doc" << std::endl;
                BSON_APPEND_OID(returned_doc, key, &value->value.v_oid);
                return true;
            }
        }
    }
    std::cerr << "_id not found for this input doc" << std::endl;
    return false;
}

void Projector::generate_basic_element_doc(bson_t* b, bson_iter_t* last_token_iter) {
    const char* key;
    bson_type_t type;
    const bson_value_t* value;

    key = bson_iter_key(last_token_iter);
    type = bson_iter_type(last_token_iter);
    value = bson_iter_value(last_token_iter);

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
            uint32_t array_len = 0;
            const uint8_t* array = NULL;
            bson_iter_array(last_token_iter, &array_len, &array);
            BSON_APPEND_ARRAY(b, key, bson_new_from_data(array, array_len));
            break;
        }
        case BSON_TYPE_DOCUMENT: {
            uint32_t doc_len = 0;
            const uint8_t * doc = NULL;
            bson_iter_document(last_token_iter, &doc_len, &doc);
            BSON_APPEND_DOCUMENT(b, key, bson_new_from_data(doc, doc_len));
            break;
        }
        case BSON_TYPE_BINARY:
            BSON_APPEND_BINARY(b, key, value->value.v_binary.subtype, value->value.v_binary.data, value->value.v_binary.data_len);
            break;

        case BSON_TYPE_UNDEFINED:
            BSON_APPEND_UNDEFINED(b, key);

        case BSON_TYPE_OID:
            // we have already inserted _id by default
            if (strncmp(key, "_id", 5) == 0)
                return;

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
            bson_iter_decimal128(last_token_iter, dec);
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
}

bson_t* Projector::append_document(bson_t* bson_doc, std::string& field) {
    bson_t* return_doc;
    return_doc = bson_new();
    BSON_APPEND_DOCUMENT(return_doc, field.c_str(), bson_doc);
    std::cout << "nested doc appended: " << bson_as_json(return_doc, NULL) << std::endl;
    return return_doc;
}

bson_t* Projector::append_array(bson_t* bson_doc, std::string& field) {
    bson_t* return_doc;
    return_doc = bson_new();
    BSON_APPEND_ARRAY(return_doc, field.c_str(), bson_doc);
    std::cout << "nested array appended: " << bson_as_json(return_doc, NULL) << std::endl;
    return return_doc;
}