package qfplot

import (
	"strconv"

	"github.com/tobgu/qframe"
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
// package or a custom function many be specified.
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

// ValueOfInt returns an IntView compatible ValueFunc
func ValueOfInt(view qframe.IntView) ValueFunc {
	return func(i int) float64 {
		return float64(view.ItemAt(i))
	}
}

// ValueOfInt returns an FloatView compatible ValueFunc
func ValueOfFloat(view qframe.FloatView) ValueFunc {
	return func(i int) float64 {
		return view.ItemAt(i)
	}
}

// Valuer impelements the Valuer interface
// defined in gonum.org/v1/plot/plotter.
type Valuer struct {
	len int
	fn  ValueFunc
}

// Len returns the length of the underlying view
func (v Valuer) Len() int { return v.len }

// Value returns the value in row i of the underlying view
func (v Valuer) Value(i int) float64 { return v.fn(i) }

// NewValuer returns a new Valuer
func NewValuer(len int, fn ValueFunc) Valuer {
	return Valuer{len: len, fn: fn}
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

// NewXYer returns a new XYer
func NewXYer(len int, xfn, yfn ValueFunc) XYer {
	return XYer{len: len, xfn: xfn, yfn: yfn}
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

// XYZ returns the values of X, Y, and X in the underlying view
func (xyz XYZer) XYZ(i int) (float64, float64, float64) {
	return xyz.xfn(i), xyz.yfn(i), xyz.zfn(i)
}

// XY returns the values of X and Y in the underlying view
func (xyz XYZer) XY(i int) (float64, float64) {
	return xyz.xfn(i), xyz.yfn(i)
}

// NewXYZer returns a new XYZer
func NewXYZer(len int, xfn, yfn, zfn ValueFunc) XYZer {
	return XYZer{len: len, xfn: xfn, yfn: yfn, zfn: zfn}
}
