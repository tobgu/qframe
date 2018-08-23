package qplot_test

import (
	"gonum.org/v1/plot/plotter"

	"github.com/tobgu/qframe/contrib/gonum/qplot"
)

var (
	_ plotter.XYer       = (*qplot.XYer)(nil)
	_ plotter.XYZer      = (*qplot.XYZer)(nil)
	_ plotter.Labeller   = (*qplot.Labeller)(nil)
	_ plotter.XYLabeller = (*qplot.XYLabeller)(nil)
)
