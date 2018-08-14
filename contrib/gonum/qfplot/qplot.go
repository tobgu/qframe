package qfplot

import (
	"bytes"
	"io"

	"gonum.org/v1/plot"

	"github.com/tobgu/qframe"
	"github.com/tobgu/qframe/errors"
)

// QPlot is a abstraction over Gonum's ploting interface
// for a less verbose experience in interactive environments
// such as Jypter notebooks.
type QPlot struct {
	qf qframe.QFrame
	Config
}

// NewQPlot returns a new QPlot.
func NewQPlot(qf qframe.QFrame, cfg Config) QPlot {
	return QPlot{qf: qf, Config: cfg}
}

// WriteTo writes a plot to an io.Writer
func (qp QPlot) WriteTo(writer io.Writer) error {
	plt, err := plot.New()
	if err != nil {
		return err
	}
	for _, fn := range qp.Plotters {
		pltr, err := fn(qp.qf)
		if err != nil {
			return errors.Propagate("QPlot.Plot", err)
		}
		plt.Add(pltr)
	}
	if qp.PlotConfig != nil {
		qp.PlotConfig(plt)
	}
	w, err := plt.WriterTo(qp.Width, qp.Height, string(qp.Format))
	if err != nil {
		return err
	}
	_, err = w.WriteTo(writer)
	return err
}

// Bytes returns a plot in the configured FormatType.
func (qp QPlot) Bytes() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	err := qp.WriteTo(buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// MustBytes returns a plot in the configured FormatType
// and panics if it encounters an error.
func (qp QPlot) MustBytes() []byte {
	raw, err := qp.Bytes()
	if err != nil {
		panic(err)
	}
	return raw
}
