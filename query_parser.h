//
// Created by kai on 8/29/18.
//

#ifndef UNTITLED_QUERY_PARSER_H
#define UNTITLED_QUERY_PARSER_H

// C include
#include <iostream>
#include <fstream>
#include <sstream>
// third party include
#include <tao/pegtl.hpp>
#include "common_constants.h"

class Parser {
public:
    Parser();
    ~Parser();
    void perform_pegtl_parser(std::string& query, std::map<std::string, std::vector<std::string>> &arg_map);
};

#endif //UNTITLED_QUERY_PARSER_H
