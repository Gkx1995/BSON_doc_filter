//
// Created by kai on 8/29/18.
//

#include "query_parser.h"

using namespace CommonConstants;
//////////////////////////////////////////////////////////
// PEGTL rules and actions
//////////////////////////////////////////////////////////

namespace tao {
    namespace pegtl {
        namespace test {

//            const std::string PLACE_HOLDER = "0";
            int restriction_count = 0;
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
            struct not_quoted_field: plus<not_one<' '>> {};
            struct nested_field: plus<not_one<'\''>>{};
            struct single_quoted_field: seq<one<'\''>, nested_field,  one<'\''>> {};
            struct field: sor<single_quoted_field, not_quoted_field>{};

            struct not_exist: string<'!'>{};
            struct _exist: string<'*'>{};
            struct exist_or_not: sor<not_exist, _exist>{};
            struct relationType: sor<string<'<', '='>, string<'>', '='>, string<'!', '='>, string<'>'>,
                    string<'<'>, string<'='>> {};

            struct not_quoted_term: plus<seq<not_one<' ', ')'>>> {};
            struct nested_term: plus<seq<not_one<'\'', ')'>>> {};
            struct single_quoted_term: seq<one<'\''>, nested_term,  one<'\''>> {};
            struct term: sor<single_quoted_term, not_quoted_term>{};
            struct select_field: plus<not_one<' ', ','>> {};
            struct left_brace: string<'('> {};
            struct right_brace: string<')'> {};
            struct restriction: seq<dataType, blank, field, blank, sor< seq<relationType, blank, term>, exist_or_not>> {};
            struct braced_restriction: seq<star<left_brace>, restriction, star<right_brace>> {};
            struct select_clause: seq<_select, blank, select_field, star<seq<one<','>, select_field>>>{};
            struct where_clause: seq<_where, blank, sor<_all, seq<braced_restriction, star<blank, boolType, blank, braced_restriction>>>> {};
            struct grammar: must<select_clause, opt<blank, where_clause>, eof>{};


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
                    restriction_count = 0;

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

            template<> struct action< left_brace > {
                template <typename Input>
                static void apply(const Input& in, std::map<std::string, std::vector<std::string>>& arg_map) {
                    arg_map["boolExpression"].push_back(in.string());

                    std::cout << "boolExpression appended: " << in.string() << std::endl;
                }
            };

            template<> struct action< right_brace > {
                template <typename Input>
                static void apply(const Input& in, std::map<std::string, std::vector<std::string>>& arg_map) {
                    arg_map["boolExpression"].push_back(in.string());

                    std::cout << "boolExpression appended: " << in.string() << std::endl;
                }
            };

            template<> struct action< restriction > {
                template <typename Input>
                static void apply(const Input& in, std::map<std::string, std::vector<std::string>>& arg_map) {
                    arg_map["boolExpression"].push_back(std::to_string(restriction_count));

                    std::cout << "boolExpression appended: restriction index is " << restriction_count << std::endl;
                    restriction_count++;
                }
            };

            template<> struct action< _and > {
                template <typename Input>
                static void apply(const Input& in, std::map<std::string, std::vector<std::string>>& arg_map) {
                    arg_map["boolExpression"].push_back("&");

                    std::cout << "boolExpression appended: and" << std::endl;
                }
            };

            template<> struct action< _or > {
                template <typename Input>
                static void apply(const Input& in, std::map<std::string, std::vector<std::string>>& arg_map) {
                    arg_map["boolExpression"].push_back("|");

                    std::cout << "boolExpression appended: or" << std::endl;
                }
            };

        }
    }
}

Parser::Parser() {}

void Parser::perform_pegtl_parser(std::string& query, std::map<std::string, std::vector<std::string>> &arg_map) {
    // generate memory_input
    tao::pegtl::memory_input<> in(query, "");

    // apply input and generate arg_map
    tao::pegtl::parse<tao::pegtl::test::grammar, tao::pegtl::test::action> (in, arg_map);
}
