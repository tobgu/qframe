package qplot_test

import (
	"crypto/sha256"
	"math"
	"os"
	"testing"
	"time"

	"gonum.org/v1/gonum/stat"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"

	"github.com/tobgu/qframe"
	"github.com/tobgu/qframe/contrib/gonum/qplot"
)

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}

// SlidingWindow returns a function that finds
// the average of n time periods.
func SlidingWindow(n int) func(float64) float64 {
	var buf []float64
	return func(value float64) float64 {
		if len(buf) < n {
			buf = append(buf, value)
			return value
		}
		buf = append(buf[1:], value)
		return stat.Mean(buf, nil)
	}
}

func ExampleQPlot() {
	fp, err := os.Open("testdata/GlobalTemperatures.csv")
	panicOnErr(err)
	defer fp.Close()

	qf := qframe.ReadCSV(fp)
	// Filter out any missing values
	qf = qf.Filter(qframe.Filter{
		Column:     "LandAndOceanAverageTemperature",
		Comparator: func(f float64) bool { return !math.IsNaN(f) },
	})
	// QFrame does not yet have native support for timeseries
	// data so we convert the timestamp to epoch time.
	qf = qf.Apply(qframe.Instruction{
		Fn: func(ts *string) int {
			tm, err := time.Parse("2006-01-02", *ts)
			if err != nil {
				panic(err)
			}
			return int(tm.Unix())
		},
		SrcCol1: "dt",
		DstCol:  "time",
	})
	// Compute the average of the last 2 years of temperatures.
	window := SlidingWindow(24)
	qf = qf.Apply(qframe.Instruction{
		Fn: func(value float64) float64 {
			return window(value)
		},
		SrcCol1: "LandAndOceanAverageTemperature",
		DstCol:  "SMA",
	})

	// Create a new configuration
	cfg := qplot.NewConfig(
		// Configure the base Plot
		qplot.PlotConfig(
			func(plt *plot.Plot) {
				plt.Add(plotter.NewGrid())
				plt.Title.Text = "Global Land & Ocean Temperatures"
				plt.X.Label.Text = "Time"
				plt.Y.Label.Text = "Temperature"
			},
		),
		// Plot each recorded temperature as a scatter plot
		qplot.Plotter(
			qplot.ScatterPlotter(
				qplot.MustNewXYer("time", "LandAndOceanAverageTemperature", qf),
				func(plt *plot.Plot, line *plotter.Scatter) {
					plt.Legend.Add("Temperature", line)
					line.Color = plotutil.Color(2)
				},
			)),
		// Plot the SMA as a line
		qplot.Plotter(
			qplot.LinePlotter(
				qplot.MustNewXYer("time", "SMA", qf),
				func(plt *plot.Plot, line *plotter.Line) {
					plt.Legend.Add("SMA", line)
					line.Color = plotutil.Color(1)
				},
			)),
	)
	// Create a new QPlot
	qp := qplot.NewQPlot(cfg)
	// Write the plot to disk
	panicOnErr(os.WriteFile("testdata/GlobalTemperatures.png", qp.MustBytes(), 0644))
}

func getHash(t *testing.T, path string) [32]byte {
	raw, err := os.ReadFile(path)
	panicOnErr(err)
	return sha256.Sum256(raw)
}

func TestQPlot(t *testing.T) {
	original := getHash(t, "testdata/GlobalTemperatures.png")
	ExampleQPlot()
	modified := getHash(t, "testdata/GlobalTemperatures.png")
	if original != modified {
		t.Errorf("output image has changed")
	}
}
