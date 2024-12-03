package di

import (
	"github.com/samber/do/v2"
	"github.com/strahe/curio-sentinel/config"
)

func SetupContainer(cfgPath string) do.Injector {

	injector := do.New()

	do.ProvideNamedValue(injector, "configPath", cfgPath)
	do.Provide(injector, NewConfig)

	return injector
}

func NewConfig(i do.Injector) (*config.Config, error) {
	return config.NewFromFile(do.MustInvokeNamed[string](i, "configPath"))
}
