package qframe

import (
	"fmt"
	"github.com/tobgu/qframe/config/eval"
	"github.com/tobgu/qframe/errors"
	"strconv"
)

func getFunc(ctx *eval.Context, ac eval.ArgCount, qf QFrame, colName, funcName string) (QFrame, interface{}) {
	if qf.Err != nil {
		return qf, nil
	}

	typ, err := qf.functionType(colName)
	if err != nil {
		return qf.withErr(errors.Propagate("getFunc", err)), nil
	}

	fn, ok := ctx.GetFunc(typ, ac, funcName)
	if !ok {
		return qf.withErr(errors.New("getFunc", "Could not find %s %s function with name '%s'", typ, ac, funcName)), nil
	}

	return qf, fn
}

type Expression interface {
	execute(f QFrame, ctx *eval.Context) (QFrame, string)
	Err() error
}

func newExpr(expr interface{}) Expression {
	// Try, in turn, to decode expr into a valid expression type.
	if e, ok := expr.(Expression); ok {
		return e
	}

	if e, ok := newColExpr(expr); ok {
		return e
	}

	if e, ok := newConstExpr(expr); ok {
		return e
	}

	if e, ok := newUnaryExpr(expr); ok {
		return e
	}

	if e, ok := newColConstExpr(expr); ok {
		return e
	}

	if e, ok := newColColExpr(expr); ok {
		return e
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

func (e colExpr) execute(qf QFrame, _ *eval.Context) (QFrame, string) {
	return qf, e.srcCol
}

// TODO-C
func (e colExpr) Err() error {
	return nil
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
		isConst = isStringConstant(t)
		if isConst {
			s := trimQuotes(t)
			value = &s
		}
	default:
		isConst = false
	}

	return constExpr{value: value}, isConst
}

func (e constExpr) execute(qf QFrame, _ *eval.Context) (QFrame, string) {
	if qf.Err != nil {
		return qf, ""
	}

	colName := tempColName(qf, "const")
	return qf.Apply(Instruction{Fn: e.value, DstCol: colName}), colName
}

func (e constExpr) Err() error {
	return nil
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

func (e unaryExpr) execute(qf QFrame, ctx *eval.Context) (QFrame, string) {
	qf, fn := getFunc(ctx, eval.ArgCountOne, qf, e.srcCol, e.operation)
	if qf.Err != nil {
		return qf, ""
	}

	colName := tempColName(qf, "unary")
	return qf.Apply(Instruction{Fn: fn, DstCol: colName, SrcCol1: e.srcCol}), colName
}

func (e unaryExpr) Err() error {
	return nil
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

func (e colConstExpr) execute(qf QFrame, ctx *eval.Context) (QFrame, string) {
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

func (e colConstExpr) Err() error {
	return nil
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

func (e colColExpr) execute(qf QFrame, ctx *eval.Context) (QFrame, string) {
	qf, fn := getFunc(ctx, eval.ArgCountTwo, qf, e.srcCol1, e.operation)
	if qf.Err != nil {
		return qf, ""
	}

	// Fill temp column with the constant part and then apply col col expression.
	// There are other ways to do this that would avoid the temp column but it would
	// require more special case logic.
	colName := tempColName(qf, "colcol")
	result := qf.Apply(Instruction{Fn: fn, DstCol: colName, SrcCol1: e.srcCol1, SrcCol2: e.srcCol2})
	return result, colName
}

func (e colColExpr) Err() error {
	return nil
}

// Nested expressions
type exprExpr struct {
	operation string
	lhs       Expression
	rhs       Expression
}

func newExprExpr(x interface{}) Expression {
	// In contrast to other expression constructors this one returns an error instead
	// of a bool to denote success or failure. This is to be able to pinpoint the
	// subexpression where the error occurred.

	l, ok := x.([]interface{})
	if ok && len(l) == 3 {
		operation, oOk := expressionString(l[0])
		if !oOk {
			return errorExpr{err: errors.New("newExprExpr", "invalid operation: %v", l[0])}
		}

		lhs := newExpr(l[1])
		if lhs.Err() != nil {
			return errorExpr{err: errors.Propagate("newExprExpr", lhs.Err())}
		}

		rhs := newExpr(l[2])
		if rhs.Err() != nil {
			return errorExpr{err: errors.Propagate("newExprExpr", rhs.Err())}
		}

		return exprExpr{operation: operation, lhs: lhs, rhs: rhs}
	}

	return errorExpr{err: errors.New("newExprExpr", "Expected a list with three elements, was: %v", x)}
}

func (e exprExpr) execute(qf QFrame, ctx *eval.Context) (QFrame, string) {
	result, lColName := e.lhs.execute(qf, ctx)
	result, rColName := e.rhs.execute(result, ctx)
	ccE, _ := newColColExpr([]interface{}{e.operation, lColName, rColName})
	result, colName := ccE.execute(result, ctx)

	// Drop intermediate results if not present in original frame
	dropCols := make([]string, 0)
	for _, s := range []string{lColName, rColName} {
		if !qf.Contains(s) {
			dropCols = append(dropCols, s)
		}
	}
	result = result.Drop(dropCols...)

	return result, colName
}

func (e exprExpr) Err() error {
	return nil
}

type errorExpr struct {
	err error
}

func (e errorExpr) execute(qf QFrame, ctx *eval.Context) (QFrame, string) {
	if qf.Err != nil {
		return qf, ""
	}

	return qf.withErr(e.err), ""
}

func (e errorExpr) Err() error {
	return e.err
}

// TODO-C
func Val(value interface{}) Expression {
	return newExpr(value)
}

// TODO-C
func Expr1(name, column string) Expression {
	return newExpr([]interface{}{name, column})
}

// TODO-C
func Expr2(name, val1, val2 interface{}) Expression {
	return newExpr([]interface{}{name, val1, val2})
}
