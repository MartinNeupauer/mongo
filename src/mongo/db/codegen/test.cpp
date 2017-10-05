#include "mongo/db/codegen/anta/Generator.h"
#include "mongo/db/codegen/anta/CodeGenCtx.h"
#include "mongo/db/codegen/machine/jitter.h"

#include <llvm/IR/Verifier.h>

#include <iostream>

namespace anta
{
    class Test : public Generator
    {
    public:
        Test(SemaFactory& f) : Generator(f) {}

        void gen()
        {
            function_ (int_, "testFunc");
                auto p1 = param_(int_, "%p1");
            body_();
                call_("printInt", {p1});
                return_(p1);
            end_();
        }
    };

    int theTestFunction()
    {
        CodeGenContext ctx("codegen");
        SemaFactory f;
        Test t(f);

        t.gen();

        f.finalizeCodeGen(ctx);

        llvm::verifyModule(*ctx.module_, &llvm::errs());
        
        ctx.module_->dump();
        
        machine::Jitter j;
        j.jit(ctx.module_);
        
        ctx.module_->dump();
        
        auto fn = f.GlobalFunction("testFunc");

        using NativeFuncPtr = int(*)(int);
        auto fnative = j.find<NativeFuncPtr>(fn->fullName().c_str());
        int s1 = fnative(1);
        std::cout << s1 << "\n";

        return 5;
    }
}