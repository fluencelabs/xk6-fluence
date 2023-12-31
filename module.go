package fluence

import (
	"github.com/dop251/goja"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
)

func init() {
	modules.Register("k6/x/fluence", New())
}

type (
	Fluence struct {
		vu      modules.VU
		metrics fluenceMetrics
		exports *goja.Object
	}
	RootModule struct{}
	Module     struct {
		*Fluence
	}
)

var (
	_ modules.Instance = &Module{}
	_ modules.Module   = &RootModule{}
)

func New() *RootModule {
	return &RootModule{}
}

func (*RootModule) NewModuleInstance(virtualUser modules.VU) modules.Instance {
	runtime := virtualUser.Runtime()

	metrics, err := registerMetrics(virtualUser)
	if err != nil {
		common.Throw(virtualUser.Runtime(), err)
	}

	moduleInstance := &Module{
		Fluence: &Fluence{
			vu:      virtualUser,
			metrics: metrics,
			exports: runtime.NewObject(),
		},
	}

	mustExport := func(name string, value interface{}) {
		if err := moduleInstance.exports.Set(name, value); err != nil {
			common.Throw(runtime, err)
		}
	}

	mustExport("sendParticle", moduleInstance.SendParticle)
	mustExport("builder", moduleInstance.Builder)
	mustExport("injectPrometheusMetrics", moduleInstance.InjectPrometheusMetrics)

	return moduleInstance
}

func (m *Module) Exports() modules.Exports {
	return modules.Exports{
		Default: m.Fluence.exports,
	}
}

