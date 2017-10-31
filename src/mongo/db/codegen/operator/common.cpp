#include "mongo/db/codegen/operator/common.h"
#include "mongo/bson/bsontypes.h"

namespace rohan
{
    const int64_t kFNVoffset = -3750763034362895579;

    void CommonDeclarations::generate()
    {
        // Low level BSON manipulation
        {
            function_("BSON::checkTag")->setConst();
                auto tag = param_(int8_, "%tag");
            body_();
                if_(tag == const8_(0));
                    call_("llvm.trap",{});
                end_();
                return_();
            end_();
        }

        {
            // Find a field in a document
            // Assume the document is valid (e.g. the size is correct and every byte is readable)
            function_(pint8_, "BSON::getField");
                auto document = param_(pint8_, "%document");
                auto fieldName = param_(weak_string_, "%fieldName");
            body_();
                auto begin = var_("begin", document + const_(4));

                auto tag = var_(int8_, "tag");

                loop_();
                    tag = *begin;
                    // If we have hit the end of document then we are done
                    if_ (tag == const8_(mongo::BSONType::EOO));
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

                    // Skip the field name
                    begin = begin + const_(1);
                    for_(nothing_(), *begin, begin = begin + const_(1) );
                    end_();
                    begin = begin + const_(1);

                    if_ (tag == const8_(mongo::BSONType::NumberDouble));
                        begin = begin + const_(sizeof(double));
                        continue_();
                    end_();
                    if_ (tag == const8_(mongo::BSONType::String));
                        begin = begin + *cast_(pint_, begin) + const_(4);
                        continue_();
                    end_();
                    if_ (tag == const8_(mongo::BSONType::Object));
                        begin = begin + *cast_(pint_, begin);
                        continue_();
                    end_();
                    if_ (tag == const8_(mongo::BSONType::Array));
                        begin = begin + *cast_(pint_, begin);
                        continue_();
                    end_();
                    if_ (tag == const8_(mongo::BSONType::BinData));
                        begin = begin + *cast_(pint_, begin) + const_(1);
                        continue_();
                    end_();
                    if_ (tag == const8_(mongo::BSONType::Undefined));
                        continue_();
                    end_();
                    if_ (tag == const8_(mongo::BSONType::jstOID));
                        begin = begin + const_(12);
                        continue_();
                    end_();
                    if_ (tag == const8_(mongo::BSONType::Bool));
                        begin = begin + const_(1);
                        continue_();
                    end_();
                    if_ (tag == const8_(mongo::BSONType::Date));
                        begin = begin + const_(sizeof(int64_t));
                        continue_();
                    end_();
                    if_ (tag == const8_(mongo::BSONType::jstNULL));
                        continue_();
                    end_();
                    if_ (tag == const8_(mongo::BSONType::RegEx));
                        call_("llvm.trap",{}); // TODO - fixit
                        continue_();
                    end_();
                    if_ (tag == const8_(mongo::BSONType::DBRef));
                        call_("llvm.trap",{}); // TODO - fixit
                        continue_();
                    end_();
                    if_ (tag == const8_(mongo::BSONType::Code));
                        call_("llvm.trap",{}); // TODO - fixit
                        continue_();
                    end_();
                    if_ (tag == const8_(mongo::BSONType::Symbol));
                        call_("llvm.trap",{}); // TODO - fixit
                        continue_();
                    end_();
                    if_ (tag == const8_(mongo::BSONType::CodeWScope));
                        call_("llvm.trap",{}); // TODO - fixit
                        continue_();
                    end_();
                    if_ (tag == const8_(mongo::BSONType::NumberInt));
                        begin = begin + const_(sizeof(int32_t));
                        continue_();
                    end_();
                    if_ (tag == const8_(mongo::BSONType::bsonTimestamp));
                        begin = begin + const_(sizeof(uint64_t));
                        continue_();
                    end_();
                    if_ (tag == const8_(mongo::BSONType::NumberLong));
                        begin = begin + const_(sizeof(int64_t));
                        continue_();
                    end_();
                    if_ (tag == const8_(mongo::BSONType::NumberDecimal));
                        begin = begin + const_(16);
                        continue_();
                    end_();


                    if_ (tag == const8_(mongo::BSONType::MinKey));
                        continue_();
                    end_();
                    if_ (tag == const8_(mongo::BSONType::MaxKey));
                        continue_();
                    end_();

                    // unhandled types - need a better bail out
                    call_("llvm.trap",{});
                end_();
            end_();
        }
        {
			function_(bsonvariant_, "BSON::createString");
				auto alloc = param_(pint8_, "%allocator");
				auto str = param_(pint8_, "%str");
				auto len = param_(int_, "%len"); // including 0 at the end
            body_();

                auto result = var_(bsonvariant_, "result");
				auto rawptr = var_("%rawptr", cast_(pint8_, &result));

                auto memory = var_(pint8_, "memory");

				if_(len < const_(anta::BSONVariant::kSize));
					// store the tag
					*(rawptr + const_(anta::BSONVariant::kTagOffset)) = const8_(mongo::BSONType::String);
					memory = rawptr + const_(anta::BSONVariant::kInlineStringOffset);
				else_();
					// allocate memory
					memory = eval_("allocateMemory", { alloc, len });
					// store the tag
					*(rawptr + const_(anta::BSONVariant::kTagOffset)) = const8_(0x80 | mongo::BSONType::String);
					*cast_(ptr_(pint8_), rawptr + const_(anta::BSONVariant::kMemoryPtrOffset)) = memory;
					*cast_(ptr_(pint8_), rawptr + const_(anta::BSONVariant::kAllocatorPtrOffset)) = alloc;
				end_();

                // copy the string
				auto i = var_("i", const_(0));
				auto iter = var_("iter", str);
				for_(nothing_(), i < len, i = i + const_(1));
					*memory = *iter;
					memory = memory + const_(1);
					iter = iter + const_(1);
				end_();

				return_(result);
			end_();            
        }
        {
            function_(bsonvariant_, "BSON::getVariant");
                auto field = param_(pint8_, "%field");
            body_();
                auto result = var_(bsonvariant_, "result");
                auto rawptr = var_("%rawptr", cast_(pint8_, &result));

                if_ (field == nullptr_(pint8_));
                    *(rawptr + const_(anta::BSONVariant::kTagOffset)) = const8_(0x1f); //  void 
                    return_ (result);
                end_();

                auto begin = var_("begin", field);
                auto tag = var_("%tag", *begin);
                auto len = var_(int_, "len");

                // Skip the field name
                begin = begin + const_(1);
                for_(nothing_(), *begin, begin = begin + const_(1) );
                end_();
                begin = begin + const_(1);

                if_ (tag == const8_(mongo::BSONType::NumberDouble));
                    *(rawptr + const_(anta::BSONVariant::kTagOffset)) = tag;
                    *cast_(ptr_(double_), rawptr + const_(anta::BSONVariant::kScalarValueOffset)) = *cast_(ptr_(double_), begin);
                    return_(result);
                end_();
                if_ (tag == const8_(mongo::BSONType::String));
                    len = *cast_(pint_, begin);
                    begin = begin + const_(4);

                    return_(eval_("BSON::createString", {nullptr_(pint8_), begin, len}));
                end_();
                
                return_(result);
            end_();
        }
        {
            function_(bsonvariant_, "BSON::getVariantFromDocument");
                auto document = param_(pint8_, "%document");
                auto fieldName = param_(weak_string_, "%fieldName");
            body_();
                auto field = var_("%field", eval_("BSON::getField", {document, fieldName}));
                return_(eval_("BSON::getVariant", {field}));
            end_();
        }
        {
            function_(int64_, "BSON::hashString")->setConst();
                auto str = param_(pint8_, "str");
            body_();
                auto hash = var_(int64_, "hash");
                // in lieu of casts do a poor man zero extend - have to fix it eventually as this is suboptimal code
                auto widechar = var_("widechar", const64_(0));

                //hash = const64_(14695981039346656037);
                hash = const64_(kFNVoffset);

                for_(nothing_(), *str != const8_(0), str = str + const_(1));
                    *cast_(pint8_, &widechar) = *str;
                    hash = hash ^ widechar;
                    hash = hash * const64_(1099511628211);
                end_();

                return_(hash);
            end_();
        }
        {
            function_(int64_, "BSON::hashByRef")->setConst();
                auto v = param_(ptr_(bsonvariantview_), "%v");
            body_();
                auto pv = var_("%pv", cast_(pint8_, v));
                auto tag = var_("%tag", *(pv+const_(anta::BSONVariant::kTagOffset)));

                if_ (tag == const8_(mongo::BSONType::NumberDouble));
                    return_(const64_(kFNVoffset) * (*cast_(pint64_, pv + const_(anta::BSONVariant::kScalarValueOffset))));
                end_();
                if_( (tag | const8_(0x80)) == (const8_(0x80 | mongo::BSONType::String)) );
                    if_(tag == const8_(mongo::BSONType::String));
                        // the string is inline
                        return_(eval_("BSON::hashString", { pv + const_(anta::BSONVariant::kInlineStringOffset)}));
                    else_();
                        // the allocated string
                        auto memory = var_("%memory", *cast_(ptr_(pint8_), pv + const_(anta::BSONVariant::kMemoryPtrOffset)));
                        return_(eval_("BSON::hashString", {memory}));
                    end_();
                end_();

                // unsupported type
                call_("llvm.trap",{});

                return_(const64_(0));
            end_();
        }
        {
            function_(int64_, "BSON::hash")->setConst();
                auto v = param_(bsonvariantview_, "v");
            body_();
                return_(eval_("BSON::hashByRef",{ &v }));
            end_();
        }
        {
            function_(int1_, "BSON::compareEQString")->setConst();
                auto lhs = param_(pint8_, "lhs");
                auto rhs = param_(pint8_, "rhs");
            body_();
                loop_();
                    if_ (*lhs != *rhs);
                        return_(const1_(false));
                    end_();

                    if_ (*lhs == const8_(0));
                        return_(const1_(true));
                    end_();

                    lhs = lhs + const_(1);
                    rhs = rhs + const_(1);
                end_();
            end_();
        }
        {
            function_(int1_, "BSON::compareEQByRef")->setConst();
                auto lhs = param_(ptr_(bsonvariantview_), "%lhs");
                auto rhs = param_(ptr_(bsonvariantview_), "%rhs");
            body_();
                auto plhs = var_("%plhs", cast_(pint8_, lhs));
                auto prhs = var_("%prhs", cast_(pint8_, rhs));
                auto taglhs = var_("%taglhs", *(plhs+const_(anta::BSONVariant::kTagOffset)));
                auto tagrhs = var_("%tagrhs", *(prhs+const_(anta::BSONVariant::kTagOffset)));
                
                // values of different types are not equal (how about equivalency ? i.e. 1 == "1")
                if_ ((taglhs | const8_(0x80)) != (tagrhs | const8_(0x80)));
                    return_(const1_(false));
                end_();

                // double
                if_ (taglhs == const8_(mongo::BSONType::NumberDouble));
                    if_ (*cast_(pdouble_, plhs + const_(anta::BSONVariant::kScalarValueOffset)) == *cast_(pdouble_, prhs + const_(anta::BSONVariant::kScalarValueOffset)));
                        return_(const1_(true));
                    else_();
                        return_(const1_(false));
                    end_();
                end_();
                // string type
                if_( (taglhs | const8_(0x80)) == (const8_(0x80 | mongo::BSONType::String)) );
                    // left hand side string
                    auto lhsstr = var_(pint8_, "lhsstr");
                    if_(taglhs == const8_(mongo::BSONType::String));
                        // the string is inline
                        lhsstr = plhs + const_(anta::BSONVariant::kInlineStringOffset);
                    else_();
                        // the allocated string
                        lhsstr = *cast_(ptr_(pint8_), plhs + const_(anta::BSONVariant::kMemoryPtrOffset));
                    end_();
                    // right hand side string
                    auto rhsstr = var_(pint8_, "rhsstr");
                    if_(tagrhs == const8_(mongo::BSONType::String));
                        // the string is inline
                        rhsstr = prhs + const_(anta::BSONVariant::kInlineStringOffset);
                    else_();
                        // the allocated string
                        rhsstr = *cast_(ptr_(pint8_), prhs + const_(anta::BSONVariant::kMemoryPtrOffset));
                    end_();

                    return_(eval_("BSON::compareEQString",{lhsstr, rhsstr}));
                end_();

                // unsupported type
                call_("llvm.trap",{});

                return_(const1_(false));
            end_();
        }
        {
            function_(int1_, "BSON::compareEQ")->setConst();
                auto lhs = param_(bsonvariantview_, "lhs");
                auto rhs = param_(bsonvariantview_, "rhs");
            body_();
                return_(eval_("BSON::compareEQByRef",{ &lhs, &rhs }));
            end_();
        }
        generateCommonBSON();
        generateCollectionScan();
    }
}
