#include "mongo/db/codegen/anta/Generator.h"
#include "mongo/db/codegen/anta/CodeGenCtx.h"
#include "mongo/db/codegen/machine/jitter.h"
#include "mongo/db/codegen/operator/operator.h"
#include "mongo/db/codegen/operator/collection_scan.h"
#include "mongo/db/codegen/operator/entry_functions.h"

#include <llvm/IR/Verifier.h>

#include <memory>
#include <string>

#include <iostream>

using namespace std;

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

    int theTestFunction2()
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

    rohan::RuntimeState generateNative(
        SemaFactory& f, 
        machine::Jitter& jitter,
        const std::unique_ptr<rohan::XteOperator>& root,
        rohan::NativeOpenFunction& openFn,
        rohan::NativeGetNextFunction& getNextFn)
    {
        rohan::RuntimeState state;
        root->calculateRuntimeState(state);
      
        rohan::GenContext gctx{ false };
        root->generate(gctx);

        rohan::EntryFunctions e(f);
        auto fnOpen = e.generateOpen(gctx);
        auto fnGetNext = e.generateGetNext(gctx);        

        CodeGenContext ctx("codegen");

        f.finalizeCodeGen(ctx);
        
        llvm::verifyModule(*ctx.module_, &llvm::errs());
                
        jitter.jit(ctx.module_); 

        ctx.module_->dump();
        
        openFn = jitter.find<rohan::NativeOpenFunction>(fnOpen->fullName().c_str());
        getNextFn = jitter.find<rohan::NativeGetNextFunction>(fnGetNext->fullName().c_str());

        return state;
    }

    unsigned theTestFunction(machine::Jitter& jitter, rohan::NativeOpenFunction& openFn, rohan::NativeGetNextFunction& getNextFn)
    {
        CodeGenContext ctx("codegen");
        SemaFactory f;
        
        mongo::CollectionScanParams params;
        unique_ptr<rohan::XteOperator> _root = make_unique<rohan::XteCollectionScan>(f, params);

        rohan::RuntimeState state;

        _root->calculateRuntimeState(state);
        
        
        rohan::GenContext gctx{ false };
        _root->generate(gctx);
        
        rohan::EntryFunctions e(f);
        auto fnOpen = e.generateOpen(gctx);
        auto fnGetNext = e.generateGetNext(gctx);

        f.finalizeCodeGen(ctx);
        
        llvm::verifyModule(*ctx.module_, &llvm::errs());
        
        jitter.jit(ctx.module_); 

        openFn = jitter.find<rohan::NativeOpenFunction>(fnOpen->fullName().c_str());
        getNextFn = jitter.find<rohan::NativeGetNextFunction>(fnGetNext->fullName().c_str());

        return state._size;
    }
}