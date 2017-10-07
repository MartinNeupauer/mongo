#include "mongo/db/codegen/operator/common.h"

namespace rohan
{
    void CommonDeclarations::generate()
    {
        // Low level BSON manipulation
        {
            // Find a field in a document
            // Assume the document is valid (e.g. the size is correct and every byte is readable)
            function_(pint8_, "BSON::getField");
                auto document = param_(pint8_, "%document");
                auto fieldName = param_(weak_string_, "%fieldName");
            body_();
                auto size = var_("%size", *cast_(pint_, document));
                auto begin = var_("%begin", document + const_(4));
                auto end = var_("%end", document + size);

                // If we have hit the end of document then we are done
                if_ (*begin == const8_(0));
                    return_(nullptr_(pint8_));
                end_();

                // Check the name match
                auto lhs = var_("lhs", begin + const_(1)); // skip the field type tag
                auto rhs = var_("rhs", cast_(pint8_, fieldName));
                auto match = var_("match", const1_(true));
                loop_();
                    if_ (*lhs != *rhs);
                        match = const1_(false);
                        break_();
                    end_();
                    if_ (*lhs == const8_(0) || *rhs == const8_(0));
                        break_();
                    end_();

                    lhs = lhs + const_(1);
                    rhs = rhs + const_(1);
                end_();

                // If we have a match we are done
                if_ (match);
                    return_(begin);
                end_();

                //fake
                return_(nullptr_(pint8_));
            end_();
        }
        generateCollectionScan();
    }
}