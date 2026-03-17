package actions

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/USACE/go-consequences/consequences"
	"github.com/USACE/go-consequences/resultswriters"
	"github.com/corpsmap/gdal"
	"github.com/usace-cloud-compute/cc-go-sdk"
)

const (
	blockFilePathKey                               string = "blockFilePath"
	realizationNumberKey                           string = "realizationNumber"
	resultPathPatternKey                           string = "resultPathPattern"
	realizationResultFilePathKey                   string = "realizationResultFilePath"
	outputTableNameKey                             string = "outputTableName"
	spatialOutputDriverKey                         string = "spatialOutputDriver"
	realizationSpatialResultsFilePathKey           string = "realizationSpatialResultFilePath"
	eadOrdinateCapKey                              string = "eadOrdinateCap"
	summarizeOutputsActionName                     string = "summarize-outputs"
	summarizeOutputsToBlocksActionName             string = "summarize-outputs-to-blocks"
	summarizeOutputsToFrequencyActionName          string = "summarize-outputs-to-frequency"
	summarizeOutputsToWatershedFrequencyActionName string = "summarize-outputs-to-watershed-frequency"
)

func init() {
	cc.ActionRegistry.RegisterAction(summarizeOutputsActionName, &SummarizeOutputsAction{})
	cc.ActionRegistry.RegisterAction(summarizeOutputsToBlocksActionName, &SummarizeOutputsToBlocksAction{})
	cc.ActionRegistry.RegisterAction(summarizeOutputsToFrequencyActionName, &SummarizeOutputsToWatershedFrequencyAction{})
	cc.ActionRegistry.RegisterAction(summarizeOutputsToWatershedFrequencyActionName, &SummarizeOutputsToWatershedFrequencyAction{})
}

type SummarizeOutputsAction struct {
	cc.ActionRunnerBase
}
type SummarizeOutputsToBlocksAction struct {
	cc.ActionRunnerBase
}
type SummarizeOutputsToFrequencyAction struct {
	cc.ActionRunnerBase
}
type SummarizeOutputsToWatershedFrequencyAction struct {
	cc.ActionRunnerBase
}

func (ar *SummarizeOutputsAction) Run() error {
	a := ar.Action
	// get all relevant parameters
	blockFilePath := a.Attributes.GetStringOrFail(blockFilePathKey)
	realizationNumber := a.Attributes.GetIntOrFail(realizationNumberKey)    //get the realization number
	resultPathPattern := a.Attributes.GetStringOrFail(resultPathPatternKey) //get the path pattern
	tablename := a.Attributes.GetStringOrFail(tablenameKey)
	driver := a.Attributes.GetStringOrFail(outputDriverKey) //driver
	realizationResultFilePath := a.Attributes.GetStringOrFail(realizationResultFilePathKey)
	//get the block file
	file, err := os.Open(blockFilePath)

	if err != nil {
		return err
	}
	var blocks Blocks
	err = json.NewDecoder(file).Decode(&blocks)
	if err != nil {
		return err
	}
	//prepare data structures for recieving results
	realizationResults := make([]ConsequenceResult, 0)
	for _, b := range blocks {
		if b.RealizationIndex == realizationNumber {
			//substitute event numbers
			es := b.BlockEventStart
			ee := b.BlockEventEnd
			for i := es; i <= ee; i++ {
				path := fmt.Sprintf(resultPathPattern, i)
				//download geopackage
				//read geopackage
				driverOut := gdal.OGRDriverByName(driver)
				ds, dsok := driverOut.Open(path, int(gdal.ReadOnly))
				if !dsok {
					fmt.Println("error opening file of type " + driver + " at " + path)
				} else {
					hasTable := false
					for i := 0; i < ds.LayerCount(); i++ {
						if tablename == ds.LayerByIndex(i).Name() {
							hasTable = true
						}
					}
					if !hasTable {
						return errors.New("missing table " + tablename)
					}
					l := ds.LayerByName(tablename)
					fc, _ := l.FeatureCount(true)
					def := l.Definition()
					idx := 0
					for idx < fc { // Iterate and fetch the records from result cursor
						f := l.NextFeature()
						idx++
						featureResult := ConsequenceResult{
							EventNumber:       int32(i),
							BlockNumber:       int32(b.BlockIndex),
							RealizationNumber: int32(realizationNumber),
							Fdid:              f.FieldAsString(def.FieldIndex("fd_id")),
							X:                 f.FieldAsFloat64(def.FieldIndex("x")),
							Y:                 f.FieldAsFloat64(def.FieldIndex("y")),
							StructDamage:      f.FieldAsFloat64(def.FieldIndex("structure")),
							ContentDamage:     f.FieldAsFloat64(def.FieldIndex("content da")),
							Depth:             f.FieldAsFloat64(def.FieldIndex("depth")),
							Velocity:          0, //velocity requires parsing the multi hazard json
							Duration:          0,
						}
						multihazardString := f.FieldAsString(def.FieldIndex("multihazar"))
						if strings.Contains(multihazardString, "\"velocity\":") {
							velocity_partial := strings.Split(multihazardString, "\"velocity\":")[1]
							velocity_string := strings.Split(velocity_partial, ",")[0]
							velocity, err := strconv.ParseFloat(velocity_string, 64)
							if err != nil {
								fmt.Println("could not parse " + multihazardString + " for velocity")
							} else {
								featureResult.Velocity = velocity
							}
						}
						if strings.Contains(multihazardString, "\"duration\":") {
							duration_partial := strings.Split(multihazardString, "\"duration\":")[1]
							duration_string := strings.Split(duration_partial, ",")[0]
							duration, err := strconv.ParseFloat(duration_string, 64)
							if err != nil {
								fmt.Println("could not parse " + multihazardString + " for duration")
							} else {
								featureResult.Duration = duration
							}
						}

						realizationResults = append(realizationResults, featureResult)
					} //result rows
				} //dataset exists

			} //events

		}

	}
	// write out realization results
	sb := strings.Builder{}
	sb.WriteString("realization,block,event,fd_id,x,y,sd,cd,td,depth,velocity,duration\n")
	for _, r := range realizationResults {
		sb.WriteString(fmt.Sprintf("%v,%v,%v,%v,%v,%v,%.2f,%.2f,%.2f,%.5f,%.5f,%.2f\n", realizationNumber, r.BlockNumber, r.EventNumber, r.Fdid, r.X, r.Y, r.StructDamage, r.ContentDamage, r.StructDamage+r.ContentDamage, r.Depth, r.Velocity, r.Duration))
	}
	resultwriter, err := os.Create(realizationResultFilePath)

	if err != nil {
		fmt.Println(err)
		return err
	}

	resultwriter.WriteString(sb.String())
	return nil
}

type Blocks []Block
type Block struct {
	RealizationIndex int   `json:"realization_index"`
	BlockIndex       int   `json:"block_index"`
	BlockEventCount  int   `json:"block_event_count"`
	BlockEventStart  int64 `json:"block_event_start"` //inclusive - will be one greater than previous event end
	BlockEventEnd    int64 `json:"block_event_end"`   //inclusive - will be one less than event start if event count is 0.
}
type ConsequenceResult struct { //one structure
	EventNumber       int32
	BlockNumber       int32
	RealizationNumber int32
	Fdid              string  //fd_id
	X                 float64 //x
	Y                 float64 //y
	StructDamage      float64 //structure
	ContentDamage     float64 //content da
	Depth             float64 //depth
	Velocity          float64 //parse multihazar
	Duration          float64 //not present - parse multihazar
}
type EventValue struct {
	EventNumber int32
	Value       float64
}
type ConsequencesBlockResult struct {
	Fdid            string  //fd_id
	X               float64 //x
	Y               float64 //y
	StructureDamage EventValue
	ContentDamage   EventValue
	TotalDamage     EventValue
	Depth           EventValue //depth
	Velocity        EventValue //parse multihazar
	Duration        EventValue
}
type ConsequencesFrequencyResult struct {
	Fdid            string  //fd_id
	X               float64 //x
	Y               float64 //y
	SAAL            float64
	CAAL            float64
	TAAL            float64
	DAEP            float64
	StructureDamage []BlockEventValue
	ContentDamage   []BlockEventValue
	TotalDamage     []BlockEventValue
	Depth           []BlockEventValue //depth
	Velocity        []BlockEventValue //parse multihazar
	Duration        []BlockEventValue
}
type BlockEventValue struct {
	BlockNumber int32
	EventNumber int32
	Value       float64
}

func (ar *SummarizeOutputsToBlocksAction) Run() error {
	a := ar.Action
	// get all relevant parameters
	blockFilePath := a.Attributes.GetStringOrFail(blockFilePathKey)
	realizationNumber := a.Attributes.GetIntOrFail(realizationNumberKey)    //get the realization number
	resultPathPattern := a.Attributes.GetStringOrFail(resultPathPatternKey) //get the path pattern
	tablename := a.Attributes.GetStringOrFail(tablenameKey)
	driver := a.Attributes.GetStringOrFail(outputDriverKey) //driver
	realizationResultFilePath := a.Attributes.GetStringOrFail(realizationResultFilePathKey)
	//get the block file
	file, err := os.Open(blockFilePath)

	if err != nil {
		return err
	}
	var blocks Blocks
	err = json.NewDecoder(file).Decode(&blocks)
	if err != nil {
		return err
	}
	//prepare data structures for recieving results
	realizationBlockResults := make(map[int32]map[string]ConsequencesBlockResult)
	for _, b := range blocks {
		if b.RealizationIndex == realizationNumber {
			blockMap := make(map[string]ConsequencesBlockResult)
			//substitute event numbers
			es := b.BlockEventStart
			ee := b.BlockEventEnd
			for i := es; i <= ee; i++ {
				path := fmt.Sprintf(resultPathPattern, i)
				//download geopackage
				//read geopackage
				driverOut := gdal.OGRDriverByName(driver)
				ds, dsok := driverOut.Open(path, int(gdal.ReadOnly))
				if !dsok {
					fmt.Println("error opening file of type " + driver + " at " + path)
				} else {
					hasTable := false
					for i := 0; i < ds.LayerCount(); i++ {
						if tablename == ds.LayerByIndex(i).Name() {
							hasTable = true
						}
					}
					if !hasTable {
						return errors.New("missing table " + tablename)
					}
					l := ds.LayerByName(tablename)
					fc, _ := l.FeatureCount(true)
					def := l.Definition()
					idx := 0
					for idx < fc { // Iterate and fetch the records from result cursor
						f := l.NextFeature()
						idx++
						fd_id := f.FieldAsString(def.FieldIndex("fd_id"))
						sval := f.FieldAsFloat64(def.FieldIndex("structure"))
						cval := f.FieldAsFloat64(def.FieldIndex("content da"))
						multihazardString := f.FieldAsString(def.FieldIndex("multihazar"))
						depth, err := parseMultiHazardString(multihazardString, "depth")
						if err != nil {
							return err
						}
						velocity, err := parseMultiHazardString(multihazardString, "velocity")
						if err != nil {
							log.Println(err)
						}
						duration, err := parseMultiHazardString(multihazardString, "duration")
						if err != nil {
							//log.Println(err)
						}
						result, ok := blockMap[fd_id]
						if !ok {
							//create first entry for the structure
							result = ConsequencesBlockResult{
								Fdid: fd_id,
								X:    f.FieldAsFloat64(def.FieldIndex("x")),
								Y:    f.FieldAsFloat64(def.FieldIndex("y")),
								StructureDamage: EventValue{
									EventNumber: int32(i),
									Value:       sval,
								},
								ContentDamage: EventValue{
									EventNumber: int32(i),
									Value:       cval,
								},
								TotalDamage: EventValue{
									EventNumber: int32(i),
									Value:       sval + cval,
								},
								Depth: EventValue{
									EventNumber: int32(i),
									Value:       depth,
								},
								Velocity: EventValue{
									EventNumber: int32(i),
									Value:       velocity,
								},
								Duration: EventValue{
									EventNumber: int32(i),
									Value:       duration,
								},
							}
						} else {
							//update entry
							if result.StructureDamage.Value > sval {
								result.StructureDamage.EventNumber = int32(i)
								result.StructureDamage.Value = sval
							}
							if result.ContentDamage.Value > cval {
								result.ContentDamage.EventNumber = int32(i)
								result.ContentDamage.Value = cval
							}
							if result.TotalDamage.Value > (sval + cval) {
								result.TotalDamage.EventNumber = int32(i)
								result.TotalDamage.Value = sval + cval
							}
							if result.Depth.Value > depth {
								result.Depth.EventNumber = int32(i)
								result.Depth.Value = depth
							}
							if result.Velocity.Value > velocity {
								result.Velocity.EventNumber = int32(i)
								result.Velocity.Value = velocity
							}
							if result.Duration.Value > duration {
								result.Duration.EventNumber = int32(i)
								result.Duration.Value = duration
							}
						}
						blockMap[fd_id] = result
					} //result rows
				} //dataset exists

			} //events
			realizationBlockResults[int32(b.BlockIndex)] = blockMap
		}

	}
	// write out realization results
	sb := strings.Builder{}
	sb.WriteString("realization,block,fd_id,x,y,sd_event,sd,cd_event,cd,td_event,td,depth_event,depth,velocity_event,velocity,duration_event,duration\n")
	count := len(realizationBlockResults)
	for b := 1; b < count; b++ {
		m, ok := realizationBlockResults[int32(b)]
		if ok {
			for fdid, r := range m {
				sb.WriteString(fmt.Sprintf("%v,%v,%v,%v,%v,%v,%.2f,%v,%.2f,%v,%.2f,%v,%.5f,%v,%.5f,%v,%.2f\n", realizationNumber, b, fdid, r.X, r.Y, r.StructureDamage.EventNumber, r.StructureDamage.Value, r.ContentDamage.EventNumber, r.ContentDamage.Value, r.TotalDamage.EventNumber, r.TotalDamage.Value, r.Depth.EventNumber, r.Depth.Value, r.Velocity.EventNumber, r.Velocity.Value, r.Duration.EventNumber, r.Duration.Value))
			}
		}
	}
	resultwriter, err := os.Create(realizationResultFilePath)

	if err != nil {
		fmt.Println(err)
		return err
	}

	resultwriter.WriteString(sb.String())
	return nil
}
func (ar *SummarizeOutputsToWatershedFrequencyAction) Run() error {
	a := ar.Action
	// get all relevant parameters
	blockFilePath := a.Attributes.GetStringOrFail(blockFilePathKey)
	realizationNumber := a.Attributes.GetIntOrFail(realizationNumberKey)    //get the realization number
	resultPathPattern := a.Attributes.GetStringOrFail(resultPathPatternKey) //get the path pattern
	tablename := a.Attributes.GetStringOrFail(tablenameKey)
	driver := a.Attributes.GetStringOrFail(outputDriverKey) //driver
	realizationResultFilePath := a.Attributes.GetStringOrFail(realizationResultFilePathKey)

	//get the block file
	file, err := os.Open(blockFilePath)

	if err != nil {
		return err
	}
	var blocks Blocks
	err = json.NewDecoder(file).Decode(&blocks)
	if err != nil {
		return err
	}
	//prepare data structures for recieving results
	blockCount := 0
	realizationStructureResults := make(map[string]ConsequencesFrequencyResult)
	realizationBlockWatershedTotalResults := make(map[int]float64)
	wkt := ""
	for _, b := range blocks {
		if b.RealizationIndex == realizationNumber {
			//substitute event numbers
			es := b.BlockEventStart
			ee := b.BlockEventEnd
			blockCount += 1
			blockTotalDamage := 0.0
			for i := es; i <= ee; i++ {
				path := fmt.Sprintf(resultPathPattern, i)
				//download geopackage
				//read geopackage
				driverOut := gdal.OGRDriverByName(driver)
				ds, dsok := driverOut.Open(path, int(gdal.ReadOnly))
				if !dsok {
					fmt.Println("error opening file of type " + driver + " at " + path)
				} else {

					hasTable := false
					for i := 0; i < ds.LayerCount(); i++ {
						if tablename == ds.LayerByIndex(i).Name() {
							hasTable = true
						}
					}
					if !hasTable {
						return errors.New("missing table " + tablename)
					}
					l := ds.LayerByName(tablename)
					if wkt == "" {
						wkt, _ = l.SpatialReference().ToWKT()
					}
					fc, _ := l.FeatureCount(true)
					def := l.Definition()
					idx := 0
					for idx < fc { // Iterate and fetch the records from result cursor
						f := l.NextFeature()
						idx++
						fd_id := f.FieldAsString(def.FieldIndex("fd_id"))
						sval := f.FieldAsFloat64(def.FieldIndex("structure"))
						cval := f.FieldAsFloat64(def.FieldIndex("content da"))
						multihazardString := f.FieldAsString(def.FieldIndex("multihazar"))
						depth, err := parseMultiHazardString(multihazardString, "depth")
						if err != nil {
							return err
						}
						velocity, err := parseMultiHazardString(multihazardString, "velocity")
						if err != nil {
							log.Println(err)
						}
						duration, err := parseMultiHazardString(multihazardString, "duration")
						if err != nil {
							//log.Println(err)
						}
						result, ok := realizationStructureResults[fd_id]
						if !ok {
							//create first entry for the structure
							result = ConsequencesFrequencyResult{
								Fdid: fd_id,
								X:    f.FieldAsFloat64(def.FieldIndex("x")),
								Y:    f.FieldAsFloat64(def.FieldIndex("y")),
								StructureDamage: []BlockEventValue{{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       sval,
								}},
								ContentDamage: []BlockEventValue{{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       cval,
								}},
								TotalDamage: []BlockEventValue{{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       sval + cval,
								}},
								Depth: []BlockEventValue{{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       depth,
								}},
								Velocity: []BlockEventValue{{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       velocity,
								}},
								Duration: []BlockEventValue{{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       duration,
								}},
							}
						} else {
							//entry already existed. check if last element matches block number
							previous := len(result.StructureDamage) - 1
							if result.StructureDamage[previous].BlockNumber == int32(b.BlockIndex) {
								//update.
								if result.StructureDamage[previous].Value > sval {
									result.StructureDamage[previous].EventNumber = int32(i)
									result.StructureDamage[previous].Value = sval
								}
								if result.ContentDamage[previous].Value > cval {
									result.ContentDamage[previous].EventNumber = int32(i)
									result.ContentDamage[previous].Value = cval
								}
								if result.TotalDamage[previous].Value > (sval + cval) {
									result.TotalDamage[previous].EventNumber = int32(i)
									result.TotalDamage[previous].Value = sval + cval
								}
								if result.Depth[previous].Value > depth {
									result.Depth[previous].EventNumber = int32(i)
									result.Depth[previous].Value = depth
								}
								if result.Velocity[previous].Value > velocity {
									result.Velocity[previous].EventNumber = int32(i)
									result.Velocity[previous].Value = velocity
								}
								if result.Duration[previous].Value > duration {
									result.Duration[previous].EventNumber = int32(i)
									result.Duration[previous].Value = duration
								}
							} else {
								//assume we need to append a new value.
								sdam := BlockEventValue{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       sval,
								}
								result.StructureDamage = append(result.StructureDamage, sdam)
								cdam := BlockEventValue{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       cval,
								}
								result.ContentDamage = append(result.ContentDamage, cdam)
								tdam := BlockEventValue{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       sval + cval,
								}
								result.TotalDamage = append(result.TotalDamage, tdam)
								dhaz := BlockEventValue{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       depth,
								}
								result.Depth = append(result.Depth, dhaz)
								vhaz := BlockEventValue{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       velocity,
								}
								result.Velocity = append(result.Velocity, vhaz)
								durhaz := BlockEventValue{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       duration,
								}
								result.Duration = append(result.Duration, durhaz)

							}

						}
						realizationStructureResults[fd_id] = result
					} //result rows
				} //dataset exists

			} //events
			//at the end of all events in a block, process the block maximum total loss across all structures.
			for _, s := range realizationStructureResults {
				for _, t := range s.TotalDamage {
					if b.BlockIndex == int(t.BlockNumber) {
						blockTotalDamage += t.Value
					}
				}

			}
			realizationBlockWatershedTotalResults[b.BlockIndex] = blockTotalDamage
		}

	}
	//sort
	for k, v := range realizationStructureResults {
		sort.Slice(v.StructureDamage, func(i int, j int) bool {
			return v.StructureDamage[i].Value > v.StructureDamage[j].Value
		})
		sort.Slice(v.ContentDamage, func(i int, j int) bool {
			return v.ContentDamage[i].Value > v.ContentDamage[j].Value
		})
		sort.Slice(v.TotalDamage, func(i int, j int) bool {
			return v.TotalDamage[i].Value > v.TotalDamage[j].Value
		})
		sort.Slice(v.Depth, func(i int, j int) bool {
			return v.Depth[i].Value > v.Depth[j].Value
		})
		sort.Slice(v.Velocity, func(i int, j int) bool {
			return v.Velocity[i].Value > v.Velocity[j].Value
		})
		sort.Slice(v.Duration, func(i int, j int) bool {
			return v.Duration[i].Value > v.Duration[j].Value
		})
		realizationStructureResults[k] = v
	}
	//compute ead
	for k, v := range realizationStructureResults {
		for _, val := range v.StructureDamage {
			v.SAAL += val.Value
		}
		for _, val := range v.ContentDamage {
			v.CAAL += val.Value
		}
		for _, val := range v.TotalDamage {
			v.TAAL += val.Value
		}
		depthcount := blockCount - len(v.Depth)
		v.SAAL = v.SAAL / float64(blockCount)
		v.CAAL = v.CAAL / float64(blockCount)
		v.TAAL = v.TAAL / float64(blockCount)
		v.DAEP = 1.0 - (float64(depthcount) / float64(blockCount))
		realizationStructureResults[k] = v
	}
	// write out realization results
	sb := strings.Builder{}
	/*sb.WriteString("blockid,TotalDamage\n")
	for k, v := range realizationBlockWatershedTotalResults {
		sb.WriteString(fmt.Sprintf("%v,%.2f\n", k, v))
	}*/
	sb.WriteString("aal\n")
	totalAAL := 0.0
	for _, v := range realizationStructureResults {
		totalAAL += v.TAAL
	}
	sb.WriteString(fmt.Sprintf("%.2f\n", totalAAL))
	resultwriter, err := os.Create(realizationResultFilePath)

	if err != nil {
		fmt.Println(err)
		return err
	}

	resultwriter.WriteString(sb.String())
	return nil
}
func generateHazardRows(variable string, data []BlockEventValue) string {
	s1 := fmt.Sprintf(",,,,,%v,%v", variable, "event_id")
	s2 := fmt.Sprintf(",,,,,%v,%v", variable, "block_id")
	s3 := fmt.Sprintf(",,,,,%v,%v", variable, "value")
	for _, val := range data {
		s1 = fmt.Sprintf("%v,%v", s1, val.EventNumber)
		s2 = fmt.Sprintf("%v,%v", s2, val.BlockNumber)
		s3 = fmt.Sprintf("%v,%.2f", s3, val.Value)
	}
	return fmt.Sprintf("%v\n%v\n%v\n", s1, s2, s3)
}
func parseMultiHazardString(input string, parameter string) (float64, error) {
	if strings.Contains(input, "\""+parameter+"\":") {
		partial := strings.Split(input, "\""+parameter+"\":")[1]
		s := strings.Split(partial, ",")[0]
		value, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return -1.0, errors.New("could not parse " + input + " for " + parameter)
		} else {
			return value, nil
		}
	} else {
		return -1.0, errors.New("could not find parameter " + parameter)
	}
}
func (ar *SummarizeOutputsToFrequencyAction) Run() error {
	a := ar.Action
	// get all relevant parameters
	blockFilePath := a.Attributes.GetStringOrFail(blockFilePathKey)
	realizationNumber := a.Attributes.GetIntOrFail(realizationNumberKey)    //get the realization number
	resultPathPattern := a.Attributes.GetStringOrFail(resultPathPatternKey) //get the path pattern
	tablename := a.Attributes.GetStringOrFail(tablenameKey)
	driver := a.Attributes.GetStringOrFail(outputDriverKey) //driver
	realizationResultFilePath := a.Attributes.GetStringOrFail(realizationResultFilePathKey)
	outDriver := a.Attributes.GetStringOrFail(spatialOutputDriverKey)
	outTableName := a.Attributes.GetStringOrFail(outputTableNameKey)
	realizationSpatialResultFilePath := a.Attributes.GetStringOrFail(realizationSpatialResultsFilePathKey)
	eadOrdinateCap := a.Attributes.GetIntOrFail(eadOrdinateCapKey)
	//get the block file
	file, err := os.Open(blockFilePath)

	if err != nil {
		return err
	}
	var blocks Blocks
	err = json.NewDecoder(file).Decode(&blocks)
	if err != nil {
		return err
	}
	//prepare data structures for recieving results
	blockCount := 0
	realizationStructureResults := make(map[string]ConsequencesFrequencyResult)
	wkt := ""
	for _, b := range blocks {
		if b.RealizationIndex == realizationNumber {
			//substitute event numbers
			es := b.BlockEventStart
			ee := b.BlockEventEnd
			blockCount += 1
			for i := es; i <= ee; i++ {
				path := fmt.Sprintf(resultPathPattern, i)
				//download geopackage
				//read geopackage
				driverOut := gdal.OGRDriverByName(driver)
				ds, dsok := driverOut.Open(path, int(gdal.ReadOnly))
				if !dsok {
					fmt.Println("error opening file of type " + driver + " at " + path)
				} else {

					hasTable := false
					for i := 0; i < ds.LayerCount(); i++ {
						if tablename == ds.LayerByIndex(i).Name() {
							hasTable = true
						}
					}
					if !hasTable {
						return errors.New("missing table " + tablename)
					}
					l := ds.LayerByName(tablename)
					if wkt == "" {
						wkt, _ = l.SpatialReference().ToWKT()
					}
					fc, _ := l.FeatureCount(true)
					def := l.Definition()
					idx := 0
					for idx < fc { // Iterate and fetch the records from result cursor
						f := l.NextFeature()
						idx++
						fd_id := f.FieldAsString(def.FieldIndex("fd_id"))
						sval := f.FieldAsFloat64(def.FieldIndex("structure"))
						cval := f.FieldAsFloat64(def.FieldIndex("content da"))
						multihazardString := f.FieldAsString(def.FieldIndex("multihazar"))
						depth, err := parseMultiHazardString(multihazardString, "depth")
						if err != nil {
							return err
						}
						velocity, err := parseMultiHazardString(multihazardString, "velocity")
						if err != nil {
							log.Println(err)
						}
						duration, err := parseMultiHazardString(multihazardString, "duration")
						if err != nil {
							//log.Println(err)
						}
						result, ok := realizationStructureResults[fd_id]
						if !ok {
							//create first entry for the structure
							result = ConsequencesFrequencyResult{
								Fdid: fd_id,
								X:    f.FieldAsFloat64(def.FieldIndex("x")),
								Y:    f.FieldAsFloat64(def.FieldIndex("y")),
								StructureDamage: []BlockEventValue{{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       sval,
								}},
								ContentDamage: []BlockEventValue{{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       cval,
								}},
								TotalDamage: []BlockEventValue{{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       sval + cval,
								}},
								Depth: []BlockEventValue{{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       depth,
								}},
								Velocity: []BlockEventValue{{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       velocity,
								}},
								Duration: []BlockEventValue{{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       duration,
								}},
							}
						} else {
							//entry already existed. check if last element matches block number
							previous := len(result.StructureDamage) - 1
							if result.StructureDamage[previous].BlockNumber == int32(b.BlockIndex) {
								//update.
								if result.StructureDamage[previous].Value > sval {
									result.StructureDamage[previous].EventNumber = int32(i)
									result.StructureDamage[previous].Value = sval
								}
								if result.ContentDamage[previous].Value > cval {
									result.ContentDamage[previous].EventNumber = int32(i)
									result.ContentDamage[previous].Value = cval
								}
								if result.TotalDamage[previous].Value > (sval + cval) {
									result.TotalDamage[previous].EventNumber = int32(i)
									result.TotalDamage[previous].Value = sval + cval
								}
								if result.Depth[previous].Value > depth {
									result.Depth[previous].EventNumber = int32(i)
									result.Depth[previous].Value = depth
								}
								if result.Velocity[previous].Value > velocity {
									result.Velocity[previous].EventNumber = int32(i)
									result.Velocity[previous].Value = velocity
								}
								if result.Duration[previous].Value > duration {
									result.Duration[previous].EventNumber = int32(i)
									result.Duration[previous].Value = duration
								}
							} else {
								//assume we need to append a new value.
								sdam := BlockEventValue{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       sval,
								}
								result.StructureDamage = append(result.StructureDamage, sdam)
								cdam := BlockEventValue{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       cval,
								}
								result.ContentDamage = append(result.ContentDamage, cdam)
								tdam := BlockEventValue{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       sval + cval,
								}
								result.TotalDamage = append(result.TotalDamage, tdam)
								dhaz := BlockEventValue{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       depth,
								}
								result.Depth = append(result.Depth, dhaz)
								vhaz := BlockEventValue{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       velocity,
								}
								result.Velocity = append(result.Velocity, vhaz)
								durhaz := BlockEventValue{
									BlockNumber: int32(b.BlockIndex),
									EventNumber: int32(i),
									Value:       duration,
								}
								result.Duration = append(result.Duration, durhaz)

							}

						}
						realizationStructureResults[fd_id] = result
					} //result rows
				} //dataset exists

			} //events
		}

	}
	//sort
	for k, v := range realizationStructureResults {
		sort.Slice(v.StructureDamage, func(i int, j int) bool {
			return v.StructureDamage[i].Value > v.StructureDamage[j].Value
		})
		sort.Slice(v.ContentDamage, func(i int, j int) bool {
			return v.ContentDamage[i].Value > v.ContentDamage[j].Value
		})
		sort.Slice(v.TotalDamage, func(i int, j int) bool {
			return v.TotalDamage[i].Value > v.TotalDamage[j].Value
		})
		sort.Slice(v.Depth, func(i int, j int) bool {
			return v.Depth[i].Value > v.Depth[j].Value
		})
		sort.Slice(v.Velocity, func(i int, j int) bool {
			return v.Velocity[i].Value > v.Velocity[j].Value
		})
		sort.Slice(v.Duration, func(i int, j int) bool {
			return v.Duration[i].Value > v.Duration[j].Value
		})
		realizationStructureResults[k] = v
	}
	//compute ead
	//cap at the 10 year or the 50th ordinate

	for k, v := range realizationStructureResults {
		for i, val := range v.StructureDamage {
			if i <= eadOrdinateCap {
				v.SAAL += val.Value
			}

		}
		for i, val := range v.ContentDamage {
			if i <= eadOrdinateCap {
				v.CAAL += val.Value
			}
		}
		for i, val := range v.TotalDamage {
			if i <= eadOrdinateCap {
				v.TAAL += val.Value
			}
		}
		depthcount := blockCount - len(v.Depth)
		v.SAAL = v.SAAL / float64(blockCount)
		v.CAAL = v.CAAL / float64(blockCount)
		v.TAAL = v.TAAL / float64(blockCount)
		v.DAEP = 1.0 - (float64(depthcount) / float64(blockCount))
		realizationStructureResults[k] = v
	}
	rw, err := resultswriters.InitSpatialResultsWriter_WKT_Projected(realizationSpatialResultFilePath, outTableName, outDriver, wkt)
	if err != nil {
		panic(err)
	}

	// write out realization results
	sb := strings.Builder{}
	sb.WriteString("fd_id,x,y,SAAL,CAAL,TAAL,DAEP")
	rh := []string{"fd_id", "x", "y", "SAAL", "CAAL", "TAAL", "DAEP", "500yrDam", "250yrDam", "100yrDam", "50yrDam", "10yrDam", "500yrD", "250yrD", "100yrD", "50yrD", "10yrD", "500yrV", "250yrV", "100yrV", "50yrV", "10yrV"}

	for b := 1; b <= blockCount; b++ {
		sb.WriteString(fmt.Sprintf(",%.5f", float32(b)/float32(blockCount)))
	}
	sb.WriteString("\n")

	for _, v := range realizationStructureResults {
		/*var dam500 int32 = 0 //0.0
		var dam250 int32 = 0 //0.0
		var dam100 int32 = 0 //0.0
		var dam50 int32 = 0  //0.0
		var dam10 int32 = 0  //0.0
		var d500 int32 = 0   //0.0
		var d250 int32 = 0   //0.0
		var d100 int32 = 0   //0.0
		var d50 int32 = 0    //0.0
		var d10 int32 = 0    //0.0
		var v500 int32 = 0   //0.0
		var v250 int32 = 0   //0.0
		var v100 int32 = 0   //0.0
		var v50 int32 = 0    //0.0
		var v10 int32 = 0    //0.0
		*/
		var dam500 float64 = 0.0 //0.0
		var dam250 float64 = 0.0 //0.0
		var dam100 float64 = 0.0 //0.0
		var dam50 float64 = 0.0  //0.0
		var dam10 float64 = 0.0  //0.0
		var d500 float64 = 0.0   //0.0
		var d250 float64 = 0.0   //0.0
		var d100 float64 = 0.0   //0.0
		var d50 float64 = 0.0    //0.0
		var d10 float64 = 0.0    //0.0
		var v500 float64 = 0.0   //0.0
		var v250 float64 = 0.0   //0.0
		var v100 float64 = 0.0   //0.0
		var v50 float64 = 0.0    //0.0
		var v10 float64 = 0.0    //0.0
		/*if len(v.TotalDamage) >= 50 {
			dam10 = v.TotalDamage[50-1].EventNumber //Value
			d10 = v.Depth[50-1].EventNumber         //Value
			v10 = v.Velocity[50-1].EventNumber      //Value
		}
		if len(v.TotalDamage) >= 10 {
			dam50 = v.TotalDamage[10-1].EventNumber //Value
			d50 = v.Depth[10-1].EventNumber         //Value
			v50 = v.Velocity[10-1].EventNumber      //Value
		}
		if len(v.TotalDamage) >= 5 {
			dam100 = v.TotalDamage[5-1].EventNumber //Value
			d100 = v.Depth[5-1].EventNumber         //Value
			v100 = v.Velocity[5-1].EventNumber      //Value
		}
		if len(v.TotalDamage) >= 2 {
			dam250 = v.TotalDamage[2-1].EventNumber //Value
			d250 = v.Depth[2-1].EventNumber         //Value
			v250 = v.Velocity[2-1].EventNumber      //Value
		}
		if len(v.TotalDamage) >= 1 {
			dam500 = v.TotalDamage[0].EventNumber //Value
			d500 = v.Depth[0].EventNumber         //Value
			v500 = v.Velocity[0].EventNumber      //Value
		}
		*/
		if len(v.TotalDamage) >= 50 {
			dam10 = v.TotalDamage[50-1].Value
			d10 = v.Depth[50-1].Value
			v10 = v.Velocity[50-1].Value
		}
		if len(v.TotalDamage) >= 10 {
			dam50 = v.TotalDamage[10-1].Value
			d50 = v.Depth[10-1].Value
			v50 = v.Velocity[10-1].Value
		}
		if len(v.TotalDamage) >= 5 {
			dam100 = v.TotalDamage[5-1].Value
			d100 = v.Depth[5-1].Value
			v100 = v.Velocity[5-1].Value
		}
		if len(v.TotalDamage) >= 2 {
			dam250 = v.TotalDamage[2-1].Value
			d250 = v.Depth[2-1].Value
			v250 = v.Velocity[2-1].Value
		}
		if len(v.TotalDamage) >= 1 {
			dam500 = v.TotalDamage[0].Value
			d500 = v.Depth[0].Value
			v500 = v.Velocity[0].Value
		}
		cr := consequences.Result{
			Headers: rh,
			Result:  []interface{}{v.Fdid, v.X, v.Y, v.SAAL, v.CAAL, v.TAAL, v.DAEP, dam500, dam250, dam100, dam50, dam10, d500, d250, d100, d50, d10, v500, v250, v100, v50, v10},
		}
		rw.Write(cr)
		sb.WriteString(fmt.Sprintf("%v,%v,%v,%.2f,%.2f,%.2f,%.4f\n", v.Fdid, v.X, v.Y, v.SAAL, v.CAAL, v.TAAL, v.DAEP))
		//now write three rows per frequency curve.
		sb.WriteString(generateHazardRows("structure_damage", v.StructureDamage))
		sb.WriteString(generateHazardRows("content_damage", v.ContentDamage))
		sb.WriteString(generateHazardRows("total_damage", v.TotalDamage))
		sb.WriteString(generateHazardRows("depth", v.Depth))
		sb.WriteString(generateHazardRows("velocity", v.Velocity))
		sb.WriteString(generateHazardRows("duration", v.Duration))
	}

	resultwriter, err := os.Create(realizationResultFilePath)

	if err != nil {
		fmt.Println(err)
		return err
	}

	resultwriter.WriteString(sb.String())
	rw.Close()
	return nil
}
