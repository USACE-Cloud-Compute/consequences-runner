package actions

import (
	"github.com/usace-cloud-compute/cc-go-sdk"
)

const (
	copyActionName string = "copy-inputs"
)

func init() {
	cc.ActionRegistry.RegisterAction(copyActionName, &CopyAction{})
}

type CopyAction struct {
	cc.ActionRunnerBase
}

func (ca *CopyAction) Run() error {
	pm := ca.PluginManager
	pl := pm.Payload

	for _, i := range pl.Inputs {
		for pk, _ := range i.Paths {
			input := cc.CopyToLocalInput{
				DsName:    i.Name,
				PathKey:   pk,
				LocalPath: localData, //fmt.Sprintf("%v/%v", localData, i.Name),
			}
			err := pm.CopyFileToLocal(input) //should be pv.filename or somesuch
			if err != nil {
				return err
			}
		}
	}
	return nil
}
