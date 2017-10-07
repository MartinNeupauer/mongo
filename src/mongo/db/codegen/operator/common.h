#pragma once

#include "mongo/db/codegen/anta/Sema.h"
#include "mongo/db/codegen/anta/Generator.h"

namespace rohan
{
    class CommonDeclarations : public anta::Generator
    {
        void generateCollectionScan();
    public:
        CommonDeclarations(anta::SemaFactory& f) : anta::Generator(f) {}

        void generate();
    };
}