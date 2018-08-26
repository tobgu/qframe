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

// HistogramConfig is an optional function which
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

// ScatterPlotter returns a new PlotterFunc that plots a Scatter.
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

// BoxPlotConfig is an optional function which
// configures a BoxPlot after creation.
type BoxPlotConfig func(*plot.Plot, *plotter.BoxPlot)

// BotPlot returns a new PlotterFunc that plots a BoxPlot.
func BoxPlot(w vg.Length, loc float64, values plotter.Valuer, cfg BoxPlotConfig) PlotterFunc {
	return func(plt *plot.Plot) (plot.Plotter, error) {
		pltr, err := plotter.NewBoxPlot(w, loc, values)
		if err != nil {
			return nil, err
		}
		if cfg != nil {
			cfg(plt, pltr)
		}
		return pltr, nil
	}
}

// LabelsConfig is an optional function which
// configures a Labels after creation.
type LabelsConfig func(*plot.Plot, *plotter.Labels)

// Labels returns a new PlotterFunc that plots a plotter.Labels.
func Labels(labeller XYLabeller, cfg LabelsConfig) PlotterFunc {
	return func(plt *plot.Plot) (plot.Plotter, error) {
		pltr, err := plotter.NewLabels(labeller)
		if err != nil {
			return nil, err
		}
		if cfg != nil {
			cfg(plt, pltr)
		}
		return pltr, nil
	}
}

// QuartConfig is an optional function which
// configures a QuartPlot after creation.
type QuartConfig func(*plot.Plot, *plotter.QuartPlot)

// QuartPlot returns a new PloterFunc that plots a QuartPlot.
func QuartPlot(loc float64, values plotter.Valuer, cfg QuartConfig) PlotterFunc {
	return func(plt *plot.Plot) (plot.Plotter, error) {
		pltr, err := plotter.NewQuartPlot(loc, values)
		if err != nil {
			return nil, err
		}
		if cfg != nil {
			cfg(plt, pltr)
		}
		return pltr, nil
	}
}

// satisfies New<X,Y>ErrorBars function interface
type errorBars struct {
	XYer
	YErrorer
	XErrorer
}

// YErrorBarsConfig is an optional function which
// configures a YErrorBars after creation.
type YErrorBarsConfig func(*plot.Plot, *plotter.YErrorBars)

// YErrorBars returns a new PlotterFunc that plots a YErrorBars.
func YErrorBars(xyer XYer, yerr YErrorer, cfg YErrorBarsConfig) PlotterFunc {
	return func(plt *plot.Plot) (plot.Plotter, error) {
		pltr, err := plotter.NewYErrorBars(errorBars{XYer: xyer, YErrorer: yerr})
		if err != nil {
			return nil, err
		}
		if cfg != nil {
			cfg(plt, pltr)
		}
		return pltr, nil
	}
}

// XErrorBarsConfig is an optional function which
// configures a XErrorBars after creation.
type XErrorBarsConfig func(*plot.Plot, *plotter.XErrorBars)

// XErrorBars returns a new PlotterFunc that plots a XErrorBars.
func XErrorBars(xyer XYer, xerr XErrorer, cfg XErrorBarsConfig) PlotterFunc {
	return func(plt *plot.Plot) (plot.Plotter, error) {
		pltr, err := plotter.NewXErrorBars(errorBars{XYer: xyer, XErrorer: xerr})
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
// plotter.Function
// plotter.HeatMap
// plotter.Grid
// plotter.Image
// plotter.Sankey
