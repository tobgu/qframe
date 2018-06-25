package qframe

import (
	"fmt"
	"strconv"

	"github.com/tobgu/qframe/config/eval"
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/types"
)

func getFunc(ctx *eval.Context, ac eval.ArgCount, qf QFrame, colName types.ColumnName, funcName string) (QFrame, interface{}) {
	if qf.Err != nil {
		return qf, nil
	}

	typ, err := qf.functionType(string(colName))
	if err != nil {
		return qf.withErr(errors.Propagate("getFunc", err)), nil
	}

	fn, ok := ctx.GetFunc(typ, ac, funcName)
	if !ok {
		return qf.withErr(errors.New("getFunc", "Could not find %s %s function with name '%s'", typ, ac, funcName)), nil
	}

	return qf, fn
}

// Expression is an internal interface representing an expression that can be executed on a QFrame.
type Expression interface {
	execute(f QFrame, ctx *eval.Context) (QFrame, types.ColumnName)

	// Err returns an error if the expression could not be constructed for some reason.
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

// Either an operation or a column identifier
func opIdentifier(x interface{}) (string, bool) {
	s, ok := x.(string)
	return s, ok
}

// This will just pass the src column on
type colExpr struct {
	srcCol types.ColumnName
}

func colIdentifier(x interface{}) (types.ColumnName, bool) {
	srcCol, cOk := x.(types.ColumnName)
	return srcCol, cOk
}

func newColExpr(x interface{}) (colExpr, bool) {
	srcCol, cOk := colIdentifier(x)
	return colExpr{srcCol: srcCol}, cOk
}

func (e colExpr) execute(qf QFrame, _ *eval.Context) (QFrame, types.ColumnName) {
	return qf, e.srcCol
}

func (e colExpr) Err() error {
	return nil
}

func tempColName(qf QFrame, prefix string) types.ColumnName {
	for i := 0; i < 10000; i++ {
		colName := prefix + "-temp-" + strconv.Itoa(i)
		if !qf.Contains(colName) {
			return types.ColumnName(colName)
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
	var isConst bool
	switch x.(type) {
	case int, float64, bool, string:
		isConst = true
	default:
		isConst = false
	}

	return constExpr{value: value}, isConst
}

func (e constExpr) execute(qf QFrame, _ *eval.Context) (QFrame, types.ColumnName) {
	if qf.Err != nil {
		return qf, ""
	}

	colName := tempColName(qf, "const")
	return qf.Apply(Instruction{Fn: e.value, DstCol: string(colName)}), colName
}

func (e constExpr) Err() error {
	return nil
}

// Use the content of a single column and nothing else as input (eg. abs(x))
type unaryExpr struct {
	operation string
	srcCol    types.ColumnName
}

func newUnaryExpr(x interface{}) (unaryExpr, bool) {
	// TODO: Might want to accept slice of strings here as well?
	l, ok := x.([]interface{})
	if ok && len(l) == 2 {
		operation, oOk := opIdentifier(l[0])
		srcCol, cOk := colIdentifier(l[1])
		return unaryExpr{operation: operation, srcCol: srcCol}, oOk && cOk
	}

	return unaryExpr{}, false
}

func (e unaryExpr) execute(qf QFrame, ctx *eval.Context) (QFrame, types.ColumnName) {
	qf, fn := getFunc(ctx, eval.ArgCountOne, qf, e.srcCol, e.operation)
	if qf.Err != nil {
		return qf, ""
	}

	colName := tempColName(qf, "unary")
	return qf.Apply(Instruction{Fn: fn, DstCol: string(colName), SrcCol1: string(e.srcCol)}), colName
}

func (e unaryExpr) Err() error {
	return nil
}

// Use the content of a single column and a constant as input (eg. age + 1)
type colConstExpr struct {
	operation string
	srcCol    types.ColumnName
	value     interface{}
}

func newColConstExpr(x interface{}) (colConstExpr, bool) {
	l, ok := x.([]interface{})
	if ok && len(l) == 3 {
		operation, oOk := opIdentifier(l[0])

		srcCol, colOk := colIdentifier(l[1])
		constE, constOk := newConstExpr(l[2])
		if !colOk || !constOk {
			// Test flipping order
			srcCol, colOk = colIdentifier(l[2])
			constE, constOk = newConstExpr(l[1])
		}

		return colConstExpr{operation: operation, srcCol: srcCol, value: constE.value}, colOk && constOk && oOk
	}

	return colConstExpr{}, false
}

func (e colConstExpr) execute(qf QFrame, ctx *eval.Context) (QFrame, types.ColumnName) {
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
	result = result.Drop(string(constColName))
	return result, colName
}

func (e colConstExpr) Err() error {
	return nil
}

// Use the content of two columns as input (eg. weight / length)
type colColExpr struct {
	operation string
	srcCol1   types.ColumnName
	srcCol2   types.ColumnName
}

func newColColExpr(x interface{}) (colColExpr, bool) {
	l, ok := x.([]interface{})
	if ok && len(l) == 3 {
		op, oOk := opIdentifier(l[0])
		srcCol1, col1Ok := colIdentifier(l[1])
		srcCol2, col2Ok := colIdentifier(l[2])
		return colColExpr{operation: op, srcCol1: srcCol1, srcCol2: srcCol2}, oOk && col1Ok && col2Ok
	}

	return colColExpr{}, false
}

func (e colColExpr) execute(qf QFrame, ctx *eval.Context) (QFrame, types.ColumnName) {
	qf, fn := getFunc(ctx, eval.ArgCountTwo, qf, e.srcCol1, e.operation)
	if qf.Err != nil {
		return qf, ""
	}

	// Fill temp column with the constant part and then apply col col expression.
	// There are other ways to do this that would avoid the temp column but it would
	// require more special case logic.
	colName := tempColName(qf, "colcol")
	result := qf.Apply(Instruction{Fn: fn, DstCol: string(colName), SrcCol1: string(e.srcCol1), SrcCol2: string(e.srcCol2)})
	return result, colName
}

func (e colColExpr) Err() error {
	return nil
}

// Nested expressions
type exprExpr1 struct {
	operation string
	expr      Expression
}

type exprExpr2 struct {
	operation string
	lhs       Expression
	rhs       Expression
}

func newExprExpr(x interface{}) Expression {
	// In contrast to other expression constructors this one returns an error instead
	// of a bool to denote success or failure. This is to be able to pinpoint the
	// subexpression where the error occurred.

	l, ok := x.([]interface{})
	if ok {
		if len(l) == 2 || len(l) == 3 {
			operation, oOk := opIdentifier(l[0])
			if !oOk {
				return errorExpr{err: errors.New("newExprExpr", "invalid operation: %v", l[0])}
			}

			lhs := newExpr(l[1])
			if lhs.Err() != nil {
				return errorExpr{err: errors.Propagate("newExprExpr", lhs.Err())}
			}

			if len(l) == 2 {
				// Single argument functions such as "abs"
				return exprExpr1{operation: operation, expr: lhs}
			}

			rhs := newExpr(l[2])
			if rhs.Err() != nil {
				return errorExpr{err: errors.Propagate("newExprExpr", rhs.Err())}
			}

			return exprExpr2{operation: operation, lhs: lhs, rhs: rhs}
		}
		return errorExpr{err: errors.New("newExprExpr", "Expected a list with two or three elements, was: %v", x)}
	}

	return errorExpr{err: errors.New("newExprExpr", "Expected a list of elements, was: %v", x)}
}

func (e exprExpr1) execute(qf QFrame, ctx *eval.Context) (QFrame, types.ColumnName) {
	result, tempColName := e.expr.execute(qf, ctx)
	ccE, _ := newUnaryExpr([]interface{}{e.operation, types.ColumnName(tempColName)})
	result, colName := ccE.execute(result, ctx)

	// Drop intermediate result if not present in original frame
	if !qf.Contains(string(tempColName)) {
		result = result.Drop(string(tempColName))
	}

	return result, colName
}

func (e exprExpr1) Err() error {
	return nil
}

func (e exprExpr2) execute(qf QFrame, ctx *eval.Context) (QFrame, types.ColumnName) {
	result, lColName := e.lhs.execute(qf, ctx)
	result, rColName := e.rhs.execute(result, ctx)
	ccE, _ := newColColExpr([]interface{}{e.operation, lColName, rColName})
	result, colName := ccE.execute(result, ctx)

	// Drop intermediate results if not present in original frame
	dropCols := make([]string, 0)
	for _, c := range []types.ColumnName{lColName, rColName} {
		s := string(c)
		if !qf.Contains(s) {
			dropCols = append(dropCols, s)
		}
	}
	result = result.Drop(dropCols...)

	return result, colName
}

func (e exprExpr2) Err() error {
	return nil
}

type errorExpr struct {
	err error
}

func (e errorExpr) execute(qf QFrame, ctx *eval.Context) (QFrame, types.ColumnName) {
	if qf.Err != nil {
		return qf, ""
	}

	return qf.withErr(e.err), ""
}

func (e errorExpr) Err() error {
	return e.err
}

// Val represents a constant or column.
func Val(value interface{}) Expression {
	return newExpr(value)
}

// Expr represents an expression with one or more arguments.
// The arguments may be values, columns or the result of other expressions.
//
// If more arguments than two are passed, the expression will be evaluated by
// repeatedly applying the function to pairwise elements from the left.
// Temporary columns will be created as necessary to hold intermediate results.
//
// Pseudo example:
//     ["/", 18, 2, 3] is evaluated as ["/", ["/", 18, 2], 3] (= 3)
func Expr(name string, args ...interface{}) Expression {
	if len(args) == 0 {
		// This is currently the case. It may change if introducing variables for example.
		return errorExpr{err: errors.New("Expr", "Expressions require at least one argument")}

	}

	if len(args) == 1 {
		return newExpr([]interface{}{name, args[0]})
	}

	if len(args) == 2 {
		return newExpr([]interface{}{name, args[0], args[1]})
	}

	newArgs := make([]interface{}, len(args)-1)
	newArgs[0] = newExpr([]interface{}{name, args[0], args[1]})
	copy(newArgs[1:], args[2:])
	return Expr(name, newArgs...)
}
