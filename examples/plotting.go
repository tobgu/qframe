package main

import (
	"math"
	"os"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
	"gonum.org/v1/plot/vg/draw"

	"github.com/tobgu/qframe"
	qfnum "github.com/tobgu/qframe/contrib/gonum"
)

func Sin(n int, max float64) qframe.QFrame {
	xs := make([]float64, n)
	ys := make([]float64, n)
	for i := 0; i < n; i++ {
		xs[i] = max / float64(n) * float64(i)
		ys[i] = math.Sin(xs[i]) * 100
	}
	return qframe.New(map[string]interface{}{
		"X": xs,
		"Y": ys,
	})
}

func main() {
	plt, _ := plot.New()
	qf := Sin(100, 2*math.Pi)
	// Get the views for X and Y columns
	xview, _ := qf.FloatView("X")
	yview, _ := qf.FloatView("Y")
	// Create a type which implements the XYer interface defined in Gonum
	xyer := qfnum.NewXYer(qf.Len(), qfnum.ValueOfFloat(xview), qfnum.ValueOfFloat(yview))
	// Draw a new line
	line, _ := plotter.NewLine(xyer)
	// Plot the line
	plt.Add(plotter.NewGrid(), line)
	canvas, _ := draw.NewFormattedCanvas(15*vg.Inch, 5*vg.Inch, "svg")
	plt.Draw(draw.New(canvas))
	// Write the SVG to stdout
	canvas.WriteTo(os.Stdout)
}
