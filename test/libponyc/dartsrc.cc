#include <gtest/gtest.h>
#include <platform.h>
#include <string>

#include <ast/ast.h>
#include <ast/source.h>
#include <ast/token.h>
#include <pass/pass.h>
#include <pkg/package.h>
#include <program/program.h>

#include "util.h"

using std::string;


class DartSrcTest : public testing::Test
{
protected:
  string _expected;

  virtual void SetUp()
  {
    _expected = "";
  }

  void test(const char* src)
  {
    //printf("src:\n%s\n", src);

    errors_t* errors = errors_alloc();
    source_t* source = source_open_string(src);
    package_add_source(source, NULL, errors);
    ast_t* program = program_load(source, errors);
    bool pass = ast_passes(program, NULL, PASS_DARTSRC);
    ast_free(program);
    errors_free(errors);

    ASSERT_TRUE(pass);
  }
};


TEST_F(DartSrcTest, HelloWorld)
{
  const char* src =
    "actor Main\n"
    "  new create(env: Env) =>\n"
    "    env.out.print(\"Hello, world!\")\n";

  _expected =
    "void main() {\n"
    "    print('Hello, world!');\n"
    "}\n";

  // TODO: get the output from the DARTSRC pass
  // into got_dart_src and compare it to _expected.
  // For now, just run the pass and assert true.
  DO(test(src));
}
