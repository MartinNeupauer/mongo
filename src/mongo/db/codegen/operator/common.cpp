#include "mongo/db/codegen/operator/common.h"
#include "mongo/bson/bsontypes.h"

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
                auto begin = var_("begin", document + const_(4));
                auto end = var_("%end", document + size);

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
                auto begin = var_("begin", field);
                auto tag = var_("%tag", *begin);
                auto result = var_(bsonvariant_, "result");
                auto rawptr = var_("%rawptr", cast_(pint8_, &result));
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
        generateCollectionScan();
    }
}