/*
 *    Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "mongo/base/status.h"

#include <iostream>
#include <string>
#include <vector>

namespace mongo {

namespace optionenvironment {
class OptionSection;
class Environment;
}  // namespace optionenvironment

namespace moe = mongo::optionenvironment;

struct LDAPToolOptions {
    bool color;
    std::string user;
};

extern LDAPToolOptions globalLDAPToolOptions;

Status addLDAPToolOptions(moe::OptionSection* options);

Status storeLDAPToolOptions(const moe::Environment& params, const std::vector<std::string>& args);

}  // mongo
