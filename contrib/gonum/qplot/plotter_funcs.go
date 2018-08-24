package qplot

import (
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

// PlotterFunc returns a plot.Plotter.
type PlotterFunc func(plt *plot.Plot) (plot.Plotter, error)

// LineConfig is an optional function which
// configures a Line after creation.
type LineConfig func(*plot.Plot, *plotter.Line)

// LinePlotter returns a new PlotterFunc that plots a line
func LinePlotter(xyer plotter.XYer, cfg LineConfig) PlotterFunc {
	return func(plt *plot.Plot) (plot.Plotter, error) {
		pltr, err := plotter.NewLine(xyer)
		if err != nil {
			return nil, err
		}
		if cfg != nil {
			cfg(plt, pltr)
		}
		return pltr, nil
	}
}

// BarConfig is an optional function which
// configures a BarChart after creation.
type BarConfig func(*plot.Plot, *plotter.BarChart)

// BarPlotter returns a new PlotterFunc that plots a bar
func BarPlotter(valuer plotter.Valuer, width vg.Length, cfg BarConfig) PlotterFunc {
	return func(plt *plot.Plot) (plot.Plotter, error) {
		pltr, err := plotter.NewBarChart(valuer, width)
		if err != nil {
			return nil, err
		}
		if cfg != nil {
			cfg(plt, pltr)
		}
		return pltr, nil
	}
}

// HistConfig is an optional function which
// configures a Histogram after creation.
type HistogramConfig func(*plot.Plot, *plotter.Histogram)

// HistogramPlotter returns a new PlotterFunc that plots a histogram
func HistogramPlotter(xyer plotter.XYer, n int, cfg HistogramConfig) PlotterFunc {
	return func(plt *plot.Plot) (plot.Plotter, error) {
		pltr, err := plotter.NewHistogram(xyer, n)
		if err != nil {
			return nil, err
		}
		if cfg != nil {
			cfg(plt, pltr)
		}
		return pltr, nil
	}
}

// PolygonConfig is an optional function which
// configures a Polygon after creation.
type PolygonConfig func(*plot.Plot, *plotter.Polygon)

// PolygonPlotter returns a new PlotterFunc that plots a polygon
func PolygonPlotter(xyer plotter.XYer, cfg PolygonConfig) PlotterFunc {
	return func(plt *plot.Plot) (plot.Plotter, error) {
		pltr, err := plotter.NewPolygon(xyer)
		if err != nil {
			return nil, err
		}
		if cfg != nil {
			cfg(plt, pltr)
		}
		return pltr, nil
	}
}

// ScatterConfig is an optional function which
// configures a Scatter after creation.
type ScatterConfig func(*plot.Plot, *plotter.Scatter)

// ScatterPlotter returns a new PlotterFunc that plots a scatter
func ScatterPlotter(xyer plotter.XYer, cfg ScatterConfig) PlotterFunc {
	return func(plt *plot.Plot) (plot.Plotter, error) {
		pltr, err := plotter.NewScatter(xyer)
		if err != nil {
			return nil, err
		}
		if cfg != nil {
			cfg(plt, pltr)
		}
		return pltr, nil
	}
}

// TODO - These don't really make sense to include
// in the API but can easily be added with a custom PlotterFunc
// type ImageConfig func(*plotter.Image)
// type QuartConfig func(*plotter.QuartPlot)
// type SankeyConfig func(*plotter.Sankey)
//type BoxPlotConfig func(*plotter.BoxPlot)
//type HeatMapConfig func(*plotter.HeatMap)
//type LabelConfig func(*plotter.Labels)
//func NewLabelPlotter() PlotterFunc {}
