package actions

import "github.com/usace-cloud-compute/cc-go-sdk"

// setup constants
const (
	pluginName                        string = "consequences"
	localData                         string = "/app/data"
	computeEventActionName            string = "compute-event"
	computeFrequencyActionName        string = "compute-frequency"
	computeCoastalEventActionName     string = "compute-coastal-event"
	computeCoastalLifecycleActionName string = "compute-coastal-lifecycle"
	computeCoastalFrequencyActionName string = "compute-coastal-frequency"
)

// shared constants
const (
	tablenameKey          string = "tableName"       //plugin attribute key required
	bucketKey             string = "bucket"          //plugin attribute key required - bucket only. i.e. mmc-storage-6 - will be combined with datastore root parameter
	inventoryDriverKey    string = "inventoryDriver" //plugin attribute key required preferably "PARQUET", could be "GPKG"
	outputDriverKey       string = "outputDriver"    //plugin attribute key required preferably "PARQUET", could be "GPKG"
	outputFileNameKey     string = "outputFileName"  //plugin attribute key required should include extension compatable with driver name.
	outputLayerName       string = "damages"
	inventoryPathKey      string = "Inventory"
	pgUserKey             string = "PG_USER"
	pgPasswordKey         string = "PG_PASSWORD"
	pgDbnameKey           string = "PG_DBNAME"
	pgHostKey             string = "PG_HOST"
	pgPortKey             string = "PG_PORT"
	pgSchemaKey           string = "PG_SCHEMA"
	damageFunctionPathKey string = "damage-functions" //expected this is local - needs to agree with the payload input datasource name
)

// ComputeEventAction constants
const (
	depthgridDatasourceName    string = "depth-grid"    //plugin datasource name required
	velocitygridDatasourceName string = "velocity-grid" //plugin datasource name required
	durationgridDatasourceName string = "duration-grid" //plugin datasource name required
)

// ComputeCoastalEventAction constants
const (
	projectIdKey string = "project-id"
	runIdKey     string = "run-id"
)

// ComputeFrequencyAction constants
const (
	DepthGridPathsKey    string = "depth-grids"    // expected to contain the fully qualified vsis3 path set comma separated or the local path if the resource is included as an inputdatasource
	VelocityGridPathsKey string = "velocity-grids" // expected to contain the fully qualified vsis3 path set comma separated or the local path if the resource is included as an inputdatasource
	FrequenciesKey       string = "frequencies"    //expected to be comma separated string
)

// ComputeCoastalLifecycleAction constants
const (
	stormSimEventsPath      string = "ss-events"
	stormSimEventsDriver    string = "ss-events-driver"
	stormSimEventsLayer     string = "ss-events-layer"
	stormSimResponsesPath   string = "ss-responses"
	stormSimResponsesDriver string = "ss-responses-driver"
	stormSimResponsesLayer  string = "ss-responses-layer"
	stormSimReachesPath     string = "ss-reaches"
	stormSimReachesDriver   string = "ss-reaches-driver"
	stormSimReachesLayer    string = "ss-reaches-layer"
	stormSimLifecycle       string = "ss-lifecycle"
)

// ComputeCoastalFrequencyAction constants
const (
	adcircGrdPath string = "adcirc-grd"
	adcircSwlPath string = "adcirc-swl"
	adcircHm0Path string = "adcirc-hm0"
)

// Unused/Deprecated constants
// const(
// useKnowledgeUncertaintyKey string = "knowledgeUncertainty"
// outputDatasourceName string = "Damages"
// seedsDatasourceName        string = "seeds.json"
// studyAreaKey               string = "studyArea"
// )

type ComputeEventAction struct {
	cc.ActionRunnerBase
}
type ComputeFrequencyAction struct {
	cc.ActionRunnerBase
}
type ComputeCoastalEventAction struct {
	cc.ActionRunnerBase
}
type ComputeCoastalLifecycleAction struct {
	cc.ActionRunnerBase
}
type ComputeCoastalFrequencyAction struct {
	cc.ActionRunnerBase
}

func init() {
	cc.ActionRegistry.RegisterAction(computeEventActionName, &ComputeEventAction{})
	cc.ActionRegistry.RegisterAction(computeFrequencyActionName, &ComputeFrequencyAction{})
	cc.ActionRegistry.RegisterAction(computeCoastalEventActionName, &ComputeCoastalEventAction{})
	cc.ActionRegistry.RegisterAction(computeCoastalLifecycleActionName, &ComputeCoastalLifecycleAction{})
	cc.ActionRegistry.RegisterAction(computeCoastalFrequencyActionName, &ComputeCoastalFrequencyAction{})
}
