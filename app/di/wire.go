// +build wireinject
// The build tag makes sure the stub is not built in the final build.

package di

import (
	"github.com/google/wire"
	"hxextract/app/dao"
	"hxextract/app/server/dapr"
	"hxextract/app/service"
)

//go:generate wire
func InitApp() (*App, func(), error) {
	panic(wire.Build(dao.Provider, service.Provider, dapr.New, NewApp))
}
