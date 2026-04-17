package crresultswriters

import (
	"github.com/USACE/go-consequences/consequences"
	"github.com/USACE/go-consequences/resultswriters"
)

type lifecycleResultsWriter struct {
	SummaryWriter consequences.ResultsWriter
	EventsWriter  consequences.ResultsWriter
}

func InitLifecycleResultsWriter(
	summaryFilepath string,
	summaryLayername string,
	summaryDriver string,
	eventsFilepath string,
	eventsLayername string,
	eventsDriver string,
) (*lifecycleResultsWriter, error) {
	srw, err := resultswriters.InitSpatialResultsWriter(summaryFilepath, summaryLayername, summaryDriver)
	if err != nil {
		panic(err)
	}
	erw, err := resultswriters.InitSpatialResultsWriter(eventsFilepath, eventsLayername, eventsDriver)
	if err != nil {
		panic(err)
	}
	return &lifecycleResultsWriter{SummaryWriter: srw, EventsWriter: erw}, nil
}

func (lrw *lifecycleResultsWriter) Write(r consequences.Result) {
	// Lifecycle compute includes a column which contains results for each event in the lifecycle
	// 1. remove the event results column and parse them into a series of discrete Results
	// 2. pass each event result to lrw.EventsWriter.Write()
	// 3. pass the remaining columns to lrw.SummaryWriter.Write()
	fd_id, err := r.Fetch("fd_id")
	if err != nil {
		panic(err)
	}
	e, err := r.Fetch("hazard results")
	if err != nil {
		panic(err)
	}
	eventResults := e.(consequences.Result)
	for _, ei := range eventResults.Result {
		er := ei.(consequences.Result)
		hi := append([]string{"fd_id"}, er.Headers...)
		ri := append([]interface{}{fd_id}, er.Result...)
		lrw.EventsWriter.Write(consequences.Result{Headers: hi, Result: ri})
	}

	summaryHeaders := make([]string, 0)
	summaryResults := make([]interface{}, 0)

	for i, h := range r.Headers {
		if h != "hazard results" {
			summaryHeaders = append(summaryHeaders, h)
			summaryResults = append(summaryResults, r.Result[i])
		}
	}
	lrw.SummaryWriter.Write(consequences.Result{Headers: summaryHeaders, Result: summaryResults})
}

func (lrw *lifecycleResultsWriter) Close() {
	// Will this fail if both writers are using the same geopackage?
	lrw.SummaryWriter.Close()
	lrw.EventsWriter.Close()
}
