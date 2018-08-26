package qplot

import (
	"strconv"

	"github.com/tobgu/qframe"
	"github.com/tobgu/qframe/errors"
	"github.com/tobgu/qframe/types"
)

// LabelFunc returns a string representation of
// the value in row i.
type LabelFunc func(i int) string

// LabelOfString returns a StringView compatible LabelFunc
func LabelOfString(view qframe.StringView) LabelFunc {
	return func(i int) string {
		return *view.ItemAt(i)
	}
}

// LabelOfEnum returns a EnumView compatible LabelFunc
func LabelOfEnum(view qframe.EnumView) LabelFunc {
	return func(i int) string {
		return *view.ItemAt(i)
	}
}

// LabelOfFloat returns a FloatView compatible LabelFunc
// fmt determines the float format when creating a string
func LabelOfFloat(fmt byte, view qframe.FloatView) LabelFunc {
	return func(i int) string {
		return strconv.FormatFloat(view.ItemAt(i), fmt, -1, 64)
	}
}

// LabelOfInt returns an IntView compatible LabelFunc
func LabelOfInt(view qframe.IntView) LabelFunc {
	return func(i int) string {
		return strconv.FormatInt(int64(view.ItemAt(i)), 64)
	}
}

// LabelOfBool returns a BoolView compatible LabelFunc
func LabelOfBool(view qframe.BoolView) LabelFunc {
	return func(i int) string {
		return strconv.FormatBool(view.ItemAt(i))
	}
}

// Labeller implements the Labeller interface
// defined in gonum.org/v1/plot/plotter. It accepts
// any of the predefined LabelFunc methods in this
// package or a custom function may be specified.
type Labeller struct {
	len int
	fn  LabelFunc
}

// Label returns the label at i
func (l Labeller) Label(i int) string { return l.fn(i) }

// NewLabeller returns a new Labeller
func NewLabeller(len int, fn LabelFunc) Labeller {
	return Labeller{len: len, fn: fn}
}

// XYLabeller implements the XYLabeller interface
// defined in gonum.org/v1/plot/plotter.
// It is a union of the Labeller and XYer
// types defined in this package.
type XYLabeller struct {
	Labeller
	XYer
}

// ValueFunc returns a float representation of
// the value in row i.
type ValueFunc func(i int) float64

// NewValueFunc returns a ValueFunc for column col
// if it is a numeric column, or returns an error.
func NewValueFunc(col string, qf qframe.QFrame) (ValueFunc, error) {
	if !isNumCol(col, qf) {
		return nil, errors.New("NewValueFunc", "Column %s is not a numeric value", col)
	}
	if !qf.Contains(col) {
		return nil, errors.New("NewValueFunc", "QFrame does not contain column %s", col)
	}
	switch qf.ColumnTypeMap()[col] {
	case types.Int:
		return ValueOfInt(qf.MustIntView(col)), nil
	case types.Float:
		return ValueOfFloat(qf.MustFloatView(col)), nil
	default:
		panic(errors.New("NewValueFunc", "forgot to support a new column type?"))
	}
}

// MustNewValueFunc returns a ValueFunc and panics when
// an error is encountered.
func MustNewValueFunc(col string, qf qframe.QFrame) ValueFunc {
	fn, err := NewValueFunc(col, qf)
	if err != nil {
		panic(errors.Propagate("MustNewValueFunc", err))
	}
	return fn
}

// ValueOfInt returns an IntView compatible ValueFunc
func ValueOfInt(view qframe.IntView) ValueFunc {
	return func(i int) float64 {
		return float64(view.ItemAt(i))
	}
}

// ValueOfFloat returns an FloatView compatible ValueFunc
func ValueOfFloat(view qframe.FloatView) ValueFunc {
	return func(i int) float64 {
		return view.ItemAt(i)
	}
}

// Valuer implements the Valuer interface
// defined in gonum.org/v1/plot/plotter.Valuer
type Valuer struct {
	len int
	fn  ValueFunc
}

// Len returns the length of the underlying view
func (v Valuer) Len() int { return v.len }

// Value returns the value in row i of the underlying view
func (v Valuer) Value(i int) float64 { return v.fn(i) }

// NewValuer returns a new Valuer from the values
// in col. The column must be a numeric type.
func NewValuer(col string, qf qframe.QFrame) (Valuer, error) {
	fn, err := NewValueFunc(col, qf)
	if err != nil {
		return Valuer{}, err
	}
	return Valuer{len: qf.Len(), fn: fn}, nil
}

// MustNewValuer returns a new Valuer from the values
// in col.
func MustNewValuer(col string, qf qframe.QFrame) Valuer {
	valuer, err := NewValuer(col, qf)
	if err != nil {
		panic(errors.Propagate("MustNewValuer", err))
	}
	return valuer
}

// XYer implements the XYer interface
// defined in gonum.org/v1/plot/plotter.
type XYer struct {
	len int
	xfn ValueFunc
	yfn ValueFunc
}

// Len returns the length of the underlying view
func (xy XYer) Len() int { return xy.len }

// XY returns the values of X and Y in the underlying view
func (xy XYer) XY(i int) (float64, float64) { return xy.xfn(i), xy.yfn(i) }

// NewXYer returns a new XYer from the values
// in column x and y. Both columns must have numeric types.
func NewXYer(x, y string, qf qframe.QFrame) (XYer, error) {
	xvals, err := NewValueFunc(x, qf)
	if err != nil {
		return XYer{}, errors.Propagate("NewXYer", err)
	}
	yvals, err := NewValueFunc(y, qf)
	if err != nil {
		return XYer{}, errors.Propagate("NewXYer", err)
	}
	return XYer{len: qf.Len(), xfn: xvals, yfn: yvals}, nil
}

// MustNewXYer returns a new XYer from the values
// in column x and y. Both columns must have numeric types.
func MustNewXYer(x, y string, qf qframe.QFrame) XYer {
	xyer, err := NewXYer(x, y, qf)
	if err != nil {
		panic(errors.Propagate("MustNewXYer", err))
	}
	return xyer
}

// XYZer implements the XYZer interface
// defined in gonum.org/v1/plot/plotter
type XYZer struct {
	len int
	xfn ValueFunc
	yfn ValueFunc
	zfn ValueFunc
}

// Len returns the length of the underlying view
func (xyz XYZer) Len() int { return xyz.len }

// XYZ returns the values of X, Y, and Z in the underlying view
func (xyz XYZer) XYZ(i int) (float64, float64, float64) {
	return xyz.xfn(i), xyz.yfn(i), xyz.zfn(i)
}

// XY returns the values of X and Y in the underlying view
func (xyz XYZer) XY(i int) (float64, float64) {
	return xyz.xfn(i), xyz.yfn(i)
}

// NewXYZer returns a new XYZer from the values
// in column x, y, and z. All columns must have numeric types.
func NewXYZer(x, y, z string, qf qframe.QFrame) (XYZer, error) {
	xvals, err := NewValueFunc(x, qf)
	if err != nil {
		return XYZer{}, errors.Propagate("NewXYZer", err)
	}
	yvals, err := NewValueFunc(y, qf)
	if err != nil {
		return XYZer{}, errors.Propagate("NewXYZer", err)
	}
	zvals, err := NewValueFunc(z, qf)
	if err != nil {
		return XYZer{}, errors.Propagate("NewXYZer", err)
	}
	return XYZer{len: qf.Len(), xfn: xvals, yfn: yvals, zfn: zvals}, nil
}

// MustNewXYZer returns a new XYZer from the values
// in column x, y, and z. All columns must have numeric types.
func MustNewXYZer(x, y, z string, qf qframe.QFrame) XYZer {
	xyzer, err := NewXYZer(x, y, z, qf)
	if err != nil {
		panic(errors.Propagate("MustNewXYZer", err))
	}
	return xyzer
}
