package qfplot

import (
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"

	"github.com/tobgu/qframe"
	//"github.com/tobgu/qframe/errors"
)

// PlotterFunc returns a plot.Plotter from data in a QFrame.
type PlotterFunc func(qf qframe.QFrame) (plot.Plotter, error)

// LineConfig is an optional function which
// configures a Line after creation.
type LineConfig func(*plotter.Line)

// LinePlotter returns a PlotterFunc which plots a line along
// an XY axis of a QFrame.
func LinePlotter(x, y string, cfg LineConfig) PlotterFunc {
	return func(qf qframe.QFrame) (plot.Plotter, error) {
		xvals, err := NewValueFunc(x, qf)
		if err != nil {
			return nil, err
		}
		yvals, err := NewValueFunc(y, qf)
		if err != nil {
			return nil, err
		}
		line, err := plotter.NewLine(NewXYer(qf.Len(), xvals, yvals))
		if err != nil {
			return nil, err
		}
		if cfg != nil {
			cfg(line)
		}
		return line, nil
	}
}

// BarConfig is an optional function which
// configures a BarChart after creation.
type BarConfig func(*plotter.BarChart)

// BarPlotter returns a PlotterFunc which plots a bar for
// the values in column col of a QFrame.
func BarPlotter(col string, width vg.Length, cfg BarConfig) PlotterFunc {
	return func(qf qframe.QFrame) (plot.Plotter, error) {
		valuer, err := NewValueFunc(col, qf)
		if err != nil {
			return nil, err
		}
		bar, err := plotter.NewBarChart(NewValuer(qf.Len(), valuer), width)
		if err != nil {
			return nil, err
		}
		if cfg != nil {
			cfg(bar)
		}
		return bar, nil
	}
}
