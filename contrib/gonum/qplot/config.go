package qplot

import (
	"image/color"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/vg"
)

// FormatType indicates the output format
// for the plot.
type FormatType string

const (
	SVG = FormatType("svg")
	PNG = FormatType("png")
)

// Config specifies the QPlot configuration.
type Config struct {
	Plotters        []PlotterFunc
	BackgroundColor color.Color
	ShowGrid        bool
	Width           vg.Length
	Height          vg.Length
	Format          FormatType
	PlotConfig      func(*plot.Plot)
}

// ConfigFunc is a functional option for configuring QPlot.
type ConfigFunc func(*Config)

// NewConfig returns a new QPlot config.
func NewConfig(fns ...ConfigFunc) Config {
	cfg := Config{
		// Defaults
		Format:          PNG,
		BackgroundColor: color.White,
		Width:           245 * vg.Millimeter,
		Height:          127 * vg.Millimeter,
	}
	for _, fn := range fns {
		fn(&cfg)
	}
	return cfg
}

// Plotter appends a PlotterFunc to the plot.
func Plotter(fn PlotterFunc) ConfigFunc {
	return func(cfg *Config) {
		cfg.Plotters = append(cfg.Plotters, fn)
	}
}

// Format sets the output format of the plot.
func Format(format FormatType) ConfigFunc {
	return func(cfg *Config) {
		cfg.Format = format
	}
}

// PlotConfig is an optional function
// which configures a plot.Plot prior
// to serialization.
func PlotConfig(fn func(*plot.Plot)) ConfigFunc {
	return func(cfg *Config) {
		cfg.PlotConfig = fn
	}
}

// Height sets the height of the plot.
func Height(height vg.Length) ConfigFunc {
	return func(cfg *Config) {
		cfg.Height = height
	}
}

// Width sets the width of the plot.
func Width(width vg.Length) ConfigFunc {
	return func(cfg *Config) {
		cfg.Width = width
	}
}
