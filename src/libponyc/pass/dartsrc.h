#ifndef PASS_DARTSRC_H
#define PASS_DARTSRC_H

#include <platform.h>
#include "../ast/ast.h"
#include "pass.h"

PONY_EXTERN_C_BEGIN

bool pass_dartsrc(ast_t* program, pass_opt_t* options);

PONY_EXTERN_C_END

#endif
