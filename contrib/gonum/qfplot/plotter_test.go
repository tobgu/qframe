package qfplot_test

import (
	"gonum.org/v1/plot/plotter"

	"github.com/tobgu/qframe/contrib/gonum/qfplot"
)

var (
	_ plotter.XYer       = (*qfplot.XYer)(nil)
	_ plotter.XYZer      = (*qfplot.XYZer)(nil)
	_ plotter.Labeller   = (*qfplot.Labeller)(nil)
	_ plotter.XYLabeller = (*qfplot.XYLabeller)(nil)
)
