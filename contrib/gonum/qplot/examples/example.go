package main

import (
	"os"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"

	"github.com/tobgu/qframe"
	"github.com/tobgu/qframe/contrib/gonum/qplot"
)

func main() {
	// Create a simple QFrame
	qf := qframe.New(map[string]interface{}{
		"A": []int{1, 2, 3, 4, 5},
		"B": []float64{0.9, 1.9, 2.9, 3.9, 4.9},
	})

	// Create a new configuration
	cfg := qplot.NewConfig(
		qplot.PlotConfig(
			func(plt *plot.Plot) {
				plt.Title.Text = "My Cool Chart"
			},
		),
		qplot.Plotter(
			// Extract values from column A.
			qplot.BarPlotter("A", 24*vg.Millimeter,
				// Configure the color and various aspects
				// of the BarChart.
				func(bar *plotter.BarChart) {
					bar.Color = plotutil.Color(1)
					bar.Offset = 1 * vg.Inch
				}),
		),
		qplot.Plotter(
			qplot.LinePlotter("A", "B",
				// Configure the color and various aspects
				// of the Line.
				func(line *plotter.Line) {
					line.Color = plotutil.Color(2)
				})),
	)
	// Create a new QPlot
	qp := qplot.NewQPlot(qf, cfg)

	qp.WriteTo(os.Stdout)
}
