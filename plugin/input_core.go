package plugin

import (
	"errors"
)

var InputRegisterInfoList []*RegisterInfo

/***************************************************/

func InputRegister(r *RegisterInfo) {
	InputRegisterInfoList = append(InputRegisterInfoList, r)
}

func InputPluginProbe(ftype string) (*RegisterInfo, error) {

	for _, v := range InputRegisterInfoList {
		if ftype == v.Type {
			return v, nil
		}
	}

	return nil, errors.New("Input plugin no match")
}
