package qframe

import (
	"fmt"
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/function"
	"github.com/tobgu/qframe/types"
	"math"
	"reflect"
	"strconv"
)

// TODO
// - Auto cast int -> float for operations on float columns where the operand is an int?
// - Tests
// - Review all public QFrame functions so that they contain guards against Err execution.

type functionsByArgCount struct {
	singleArgs map[string]interface{}
	doubleArgs map[string]interface{}
}

type functionsByArgType map[types.FunctionType]functionsByArgCount

type argCount byte

const (
	argCountOne argCount = iota
	argCountTwo
)

//go:generate stringer -type=argCount

type ExprCtx struct {
	functions functionsByArgType
}

func DefaultExprCtx() *ExprCtx {
	// TODO: More functions and types
	// TODO: Cast function Itoa, Atoi, ...
	return &ExprCtx{
		functionsByArgType{
			types.FunctionTypeFloat: functionsByArgCount{
				singleArgs: map[string]interface{}{
					"abs": math.Abs,
				},
				doubleArgs: map[string]interface{}{
					"+": function.PlusF,
					"-": function.MinusF,
					"*": function.MulF,
					"/": function.DivF,
				},
			},
			types.FunctionTypeInt: functionsByArgCount{
				singleArgs: map[string]interface{}{
					"abs": function.AbsI,
				},
				doubleArgs: map[string]interface{}{
					"+": function.PlusI,
					"-": function.MinusI,
					"*": function.MulI,
					"/": function.DivI,
				},
			},
			types.FunctionTypeBool: functionsByArgCount{
				singleArgs: map[string]interface{}{
					"not": function.NotB,
				},
				doubleArgs: map[string]interface{}{
					"and":  function.AndB,
					"or":   function.OrB,
					"xor":  function.XorB,
					"nand": function.NandB,
				},
			},
			types.FunctionTypeString: functionsByArgCount{
				singleArgs: map[string]interface{}{
					"upper": function.UpperS,
					"lower": function.LowerS,
				},
				doubleArgs: map[string]interface{}{
					"+": function.ConcatS,
				},
			},
		},
	}
}

func (ctx *ExprCtx) getFunc(typ types.FunctionType, ac argCount, name string) (interface{}, bool) {
	var fn interface{}
	var ok bool
	if ac == argCountOne {
		fn, ok = ctx.functions[typ].singleArgs[name]
	} else {
		fn, ok = ctx.functions[typ].doubleArgs[name]
	}

	return fn, ok
}

func (ctx *ExprCtx) setFunc(typ types.FunctionType, ac argCount, name string, fn interface{}) {
	if ac == argCountOne {
		ctx.functions[typ].singleArgs[name] = fn
	} else {
		ctx.functions[typ].doubleArgs[name] = fn
	}
}

func (ctx *ExprCtx) SetFunc(name string, fn interface{}) error {
	// Since there's such a flexibility in the function types that can be
	// used and there is no static typing to support it this function
	// acts as the gate keeper for adding new functions.
	var ac argCount
	var typ types.FunctionType
	switch fn.(type) {
	// Int
	case func(int, int) int:
		ac, typ = argCountTwo, types.FunctionTypeInt
	case func(int) int:
		ac, typ = argCountOne, types.FunctionTypeInt
	case func(int) bool:
		ac, typ = argCountOne, types.FunctionTypeInt
	case func(int) float64:
		ac, typ = argCountOne, types.FunctionTypeInt
	case func(int) *string:
		ac, typ = argCountOne, types.FunctionTypeInt

	// Float
	case func(float64, float64) float64:
		ac, typ = argCountTwo, types.FunctionTypeFloat
	case func(float64) float64:
		ac, typ = argCountOne, types.FunctionTypeFloat
	case func(float64) int:
		ac, typ = argCountOne, types.FunctionTypeFloat
	case func(float64) bool:
		ac, typ = argCountOne, types.FunctionTypeFloat
	case func(float64) *string:
		ac, typ = argCountOne, types.FunctionTypeFloat

	// Bool
	case func(bool, bool) bool:
		ac, typ = argCountTwo, types.FunctionTypeBool
	case func(bool) bool:
		ac, typ = argCountOne, types.FunctionTypeBool
	case func(bool) int:
		ac, typ = argCountOne, types.FunctionTypeBool
	case func(bool) float64:
		ac, typ = argCountOne, types.FunctionTypeBool
	case func(bool) *string:
		ac, typ = argCountOne, types.FunctionTypeBool

	// String
	case func(*string, *string) *string:
		ac, typ = argCountTwo, types.FunctionTypeString
	case func(*string) *string:
		ac, typ = argCountOne, types.FunctionTypeString
	case func(*string) int:
		ac, typ = argCountOne, types.FunctionTypeString
	case func(*string) float64:
		ac, typ = argCountOne, types.FunctionTypeString
	case func(*string) bool:
		ac, typ = argCountOne, types.FunctionTypeString

	default:
		return errors.New("SetFunc", "unknown function type: %v", reflect.TypeOf(fn))
	}

	ctx.setFunc(typ, ac, name, fn)
	return nil
}

func getFunc(ctx *ExprCtx, ac argCount, qf QFrame, colName, funcName string) (QFrame, interface{}) {
	if qf.Err != nil {
		return qf, nil
	}

	typ, err := qf.functionType(colName)
	if err != nil {
		return qf.withErr(errors.Propagate("getFunc", err)), nil
	}

	fn, ok := ctx.getFunc(typ, ac, funcName)
	if !ok {
		return qf.withErr(errors.New("getFunc", "Could not find %s function with name '%s'", ac, funcName)), nil
	}

	return qf, fn
}

type Expression interface {
	execute(f QFrame, ctx *ExprCtx) (QFrame, string)
}

func NewExpr(expr interface{}) (Expression, error) {
	// Try, in turn, to decode expr into a valid expression type.
	if e, ok := newColExpr(expr); ok {
		return e, nil
	}

	if e, ok := newConstExpr(expr); ok {
		return e, nil
	}

	if e, ok := newUnaryExpr(expr); ok {
		return e, nil
	}

	if e, ok := newColConstExpr(expr); ok {
		return e, nil
	}

	if e, ok := newColColExpr(expr); ok {
		return e, nil
	}

	return newExprExpr(expr)
}

func trimQuotes(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

func isStringConstant(s string) bool {
	return s != trimQuotes(s)
}

// Either an operation or a column identifier
func expressionString(x interface{}) (string, bool) {
	s, ok := x.(string)
	return s, ok && !isStringConstant(s)
}

// This will just pass the src column on
type colExpr struct {
	srcCol string
}

func newColExpr(x interface{}) (colExpr, bool) {
	srcCol, cOk := expressionString(x)
	return colExpr{srcCol: srcCol}, cOk
}

func (e colExpr) execute(qf QFrame, _ *ExprCtx) (QFrame, string) {
	return qf, e.srcCol
}

func tempColName(qf QFrame, prefix string) string {
	for i := 0; i < 10000; i++ {
		colName := prefix + "-temp-" + strconv.Itoa(i)
		if !qf.Contains(colName) {
			return colName
		}
	}

	// This is really strange, somehow there are more than 10000 columns
	// in the sequence we're trying from. This should never happen, Panic...
	panic(fmt.Sprintf("Could not find temp column name for prefix %s", prefix))
}

// Generating a new column with a given content (eg. 42)
type constExpr struct {
	value interface{}
}

func newConstExpr(x interface{}) (constExpr, bool) {
	// TODO: Support const functions somehow? Or perhaps add some kind of
	//       "variable" (accessed by $...?) to the context?
	value := x
	isConst := false
	switch t := x.(type) {
	case int:
		isConst = true
	case float64:
		isConst = true
	case bool:
		isConst = true
	case string:
		isConst := isStringConstant(t)
		if isConst {
			value = trimQuotes(t)
		}
	default:
		isConst = false
	}

	return constExpr{value: value}, isConst
}

func (e constExpr) execute(qf QFrame, _ *ExprCtx) (QFrame, string) {
	if qf.Err != nil {
		return qf, ""
	}

	colName := tempColName(qf, "const")
	return qf.Assign(Instruction{Fn: e.value, DstCol: colName}), colName
}

// Use the content of a single column and nothing else as input (eg. abs(x))
type unaryExpr struct {
	operation string
	srcCol    string
}

func newUnaryExpr(x interface{}) (unaryExpr, bool) {
	// TODO: Might want to accept slice of strings here as well?
	l, ok := x.([]interface{})
	if ok && len(l) == 2 {
		operation, oOk := expressionString(l[0])
		srcCol, cOk := expressionString(l[1])
		return unaryExpr{operation: operation, srcCol: srcCol}, oOk && cOk
	}

	return unaryExpr{}, false
}

func (e unaryExpr) execute(qf QFrame, ctx *ExprCtx) (QFrame, string) {
	qf, fn := getFunc(ctx, argCountOne, qf, e.srcCol, e.operation)
	if qf.Err != nil {
		return qf, ""
	}

	colName := tempColName(qf, "unary")
	return qf.Assign(Instruction{Fn: fn, DstCol: colName, SrcCol1: e.srcCol}), colName
}

// Use the content of a single column and a constant as input (eg. age + 1)
type colConstExpr struct {
	operation string
	srcCol    string
	value     interface{}
}

func newColConstExpr(x interface{}) (colConstExpr, bool) {
	l, ok := x.([]interface{})
	if ok && len(l) == 3 {
		operation, oOk := expressionString(l[0])

		srcCol, colOk := expressionString(l[1])
		constE, constOk := newConstExpr(l[2])
		if !colOk || !constOk {
			// Test flipping order
			srcCol, colOk = expressionString(l[2])
			constE, constOk = newConstExpr(l[1])
		}

		return colConstExpr{operation: operation, srcCol: srcCol, value: constE.value}, colOk && constOk && oOk
	}

	return colConstExpr{}, false
}

func (e colConstExpr) execute(qf QFrame, ctx *ExprCtx) (QFrame, string) {
	if qf.Err != nil {
		return qf, ""
	}

	// Fill temp column with the constant part and then apply col col expression.
	// There are other ways to do this that would avoid the temp column but it would
	// require more special case logic.
	cE, _ := newConstExpr(e.value)
	result, constColName := cE.execute(qf, ctx)
	ccE, _ := newColColExpr([]interface{}{e.operation, e.srcCol, constColName})
	result, colName := ccE.execute(result, ctx)
	result = result.Drop(constColName)
	return result, colName
}

// Use the content of two columns as input (eg. weight / length)
type colColExpr struct {
	operation string
	srcCol1   string
	srcCol2   string
}

func newColColExpr(x interface{}) (colColExpr, bool) {
	l, ok := x.([]interface{})
	if ok && len(l) == 3 {
		operation, oOk := expressionString(l[0])
		srcCol1, col1Ok := expressionString(l[1])
		srcCol2, col2Ok := expressionString(l[2])
		return colColExpr{operation: operation, srcCol1: srcCol1, srcCol2: srcCol2}, oOk && col1Ok && col2Ok
	}

	return colColExpr{}, false
}

func (e colColExpr) execute(qf QFrame, ctx *ExprCtx) (QFrame, string) {
	qf, fn := getFunc(ctx, argCountOne, qf, e.srcCol1, e.operation)
	if qf.Err != nil {
		return qf, ""
	}

	// Fill temp column with the constant part and then apply col col expression.
	// There are other ways to do this that would avoid the temp column but it would
	// require more special case logic.
	colName := tempColName(qf, "colcol")
	result := qf.Assign(Instruction{Fn: fn, DstCol: colName, SrcCol1: e.srcCol1, SrcCol2: e.srcCol2})
	return result, colName
}

// Nested expressions
type exprExpr struct {
	operation string
	lhs       Expression
	rhs       Expression
}

func newExprExpr(x interface{}) (exprExpr, error) {
	// In contrast to other expression constructors this one returns an error instead
	// of a bool to denote success or failure. This is to be able to pinpoint the
	// subexpression where the error occurred.

	l, ok := x.([]interface{})
	if ok && len(l) == 3 {
		operation, oOk := expressionString(l[0])
		if !oOk {
			return exprExpr{}, errors.New("newExprExpr", "invalid operation: %v", l[0])
		}

		lhs, err := NewExpr(l[1])
		if err != nil {
			return exprExpr{}, err
		}

		rhs, err := NewExpr(l[1])
		if err != nil {
			return exprExpr{}, err
		}

		return exprExpr{operation: operation, lhs: lhs, rhs: rhs}, nil
	}

	return exprExpr{}, errors.New("newExprExpr", "Expected a list with three elements, was: %v", x)
}

func (e exprExpr) execute(qf QFrame, ctx *ExprCtx) (QFrame, string) {
	result, lColName := e.lhs.execute(qf, ctx)
	result, rColName := e.lhs.execute(result, ctx)
	ccE, _ := newColColExpr([]interface{}{e.operation, lColName, rColName})
	result, colName := ccE.execute(result, ctx)

	// Drop intermediate results
	result = result.Drop(lColName, rColName)
	return result, colName
}
