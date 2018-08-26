package qplot_test

import (
	"crypto/sha256"
	"fmt"
	"io/ioutil"
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

func maybe(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		os.Exit(1)
	}
}

func FilterNaN(col string) qframe.Filter {
	return qframe.Filter{
		Column:     col,
		Comparator: func(f float64) bool { return !math.IsNaN(f) },
	}
}

// SMAFn returns a function for computing SMA
func SMAFn(n int) func(float64) float64 {
	var buf []float64
	return func(value float64) float64 {
		buf = append(buf, value)
		if len(buf) < n {
			return value
		}
		return stat.Mean(buf, nil)
	}
}

func ExampleQPlot(t *testing.T) {
	fp, err := os.Open("testdata/GlobalTemperatures.csv")
	maybe(t, err)
	defer fp.Close()

	qf := qframe.ReadCSV(fp)
	// Filter out any missing values
	qf = qf.Filter(FilterNaN("LandAndOceanAverageTemperature"))
	// QFrame does not yet have native support for timeseries
	// data so we convert the timestamp to epoch time.
	qf = qf.Apply(qframe.Instruction{
		Fn: func(ts *string) int {
			tm, err := time.Parse("2006-02-01", *ts)
			maybe(t, err)
			return int(tm.Unix())
		},
		SrcCol1: "dt",
		DstCol:  "time",
	})
	// Compute an SMA of the temperatures
	sma := SMAFn(10)
	qf = qf.Apply(qframe.Instruction{
		Fn: func(value float64) float64 {
			return sma(value)
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
	maybe(t, ioutil.WriteFile("testdata/GlobalTemperatures.png", qp.MustBytes(), 0644))
}

func getHash(t *testing.T, path string) [32]byte {
	raw, err := ioutil.ReadFile(path)
	maybe(t, err)
	return sha256.Sum256(raw)
}

func TestQPlot(t *testing.T) {
	original := getHash(t, "testdata/GlobalTemperatures.png")
	ExampleQPlot(t)
	modified := getHash(t, "testdata/GlobalTemperatures.png")
	if original != modified {
		t.Errorf("output image has changed")
	}
}
