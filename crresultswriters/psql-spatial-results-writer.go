package crresultswriters

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/USACE/go-consequences/consequences"
	"github.com/USACE/go-consequences/hazards"
	"github.com/dewberry/gdal"
)

// results writer for spatial data to psql database
type psqlResultsWriter struct {
	ConnectionStr      string
	LayerName          string
	Layer              *gdal.Layer
	ds                 *gdal.DataSource
	TransactionStarted bool
	index              int
}

func (srw *psqlResultsWriter) Write(r consequences.Result) {
	result := r.Result
	if !srw.TransactionStarted {
		srw.TransactionStarted = true
		srw.Layer.StartTransaction()
	}

	//add a feature to a layer?
	layerDef := srw.Layer.Definition()
	//if header has been built, add the feature, and the attributes.

	feature := layerDef.Create()
	defer feature.Destroy() // Destroy feature. I believe this also destroys the geometry object g, defined below. If feature is not destroyed, memory is not released
	feature.SetFieldInteger(0, srw.index)
	//create a point geometry - not sure the best way to do that.
	x := 0.0
	y := 0.0
	g := gdal.Create(gdal.GeometryType(gdal.GT_Point))
	// defer g.Destroy() // Don't Destroy g (I believe this is handled in feature.Destroy())
	for i, val := range r.Headers {
		if val == "x" {
			x = result[i].(float64)
		}
		if val == "y" {
			y = result[i].(float64)
		}
		fieldName := val
		if len(val) > 10 {
			fieldName = val[0:10]
			fieldName = strings.TrimSpace(fieldName)
		}
		value := result[i]
		att := reflect.TypeOf(result[i])
		valType := att.Kind()
		if val == "hazard" {
			fieldName = "depth"
			de, dok := value.(hazards.HazardEvent)
			if dok {
				valType = reflect.Float64
				if de.Has(hazards.Depth) {
					fieldName = "depth"
					value = de.Depth()
				}
			} else {
				//must be an array - bummer.
				//get at the elements of the slice, add all depths to the table?
				fieldName = "multidepths"
				valType = reflect.Float64
				value = 123.456
			}

		}
		if val == "hazards" {
			fieldName = "hazards"
			de, dok := value.(string)
			if dok {
				valType = reflect.String
				value = de
			} else {
				//must be an array - bummer.
				//get at the elements of the slice, add all depths to the table?
				fieldName = "multidepths"
				valType = reflect.Float64
				value = 123.456
			}

		}
		idx := layerDef.FieldIndex(fieldName)
		switch valType {
		case reflect.String:
			feature.SetFieldString(idx, value.(string))
		case reflect.Float32:
			gval := float64(value.(float32))
			feature.SetFieldFloat64(idx, gval)
		case reflect.Float64:
			gval := value.(float64)
			feature.SetFieldFloat64(idx, gval)
		case reflect.Int32:
			gval := int(value.(int32))
			feature.SetFieldInteger(idx, gval)
		case reflect.Uint8:
			gval := int(value.(uint8))
			feature.SetFieldInteger(idx, gval)
		}

	}
	g.SetPoint(0, x, y, 0)
	feature.SetGeometryDirectly(g)
	err := srw.Layer.Create(feature)
	if err != nil {
		fmt.Println(err)
	}
	if srw.index%100000 == 0 {
		err2 := srw.Layer.CommitTransaction()
		if err2 != nil {
			fmt.Println(err2)
		}
		srw.Layer.StartTransaction()
	}

	srw.index++ //incriment.
}
func (srw *psqlResultsWriter) Close() {
	//not sure what this should do - Destroy should close resource connections.
	err2 := srw.Layer.CommitTransaction()
	if err2 != nil {
		fmt.Println(err2)
	}
	fmt.Printf("Closing, wrote %v features\n", srw.index)
	srw.ds.Destroy()
}

func InitSpatialResultsWriter_PSQL(connStr string, layerName string, driver string, dbname string) (*psqlResultsWriter, error) {
	driverOut := gdal.OGRDriverByName(driver)
	dsOut, okOut := driverOut.Open(connStr, 1)
	if !okOut {
		return &psqlResultsWriter{}, errors.New("spatial writer at database" + dbname + " of driver type " + driver + " not created")
	}

	newLayer := dsOut.LayerByName(layerName)

	return &psqlResultsWriter{
		ConnectionStr: connStr,
		LayerName:     layerName,
		ds:            &dsOut,
		Layer:         &newLayer,
		index:         0,
	}, nil
}
