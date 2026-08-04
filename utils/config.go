package utils

type StorageMapping struct {
	MappingName string `yaml:"mapping_name"`
	Slot        uint64 `yaml:"slot"`
	KeyType     string `yaml:"key_type"`
}

type ContractFunction struct {
	Name       string            `yaml:"name"`
	Args       int               `yaml:"args"`
	ArgMapping map[string]string `yaml:"arg_mapping"`
	FixedArgs  map[string]uint64 `yaml:"fixed_args"`
}

type ContractConfig struct {
	Name              string               `yaml:"name"`
	Weight            int                  `yaml:"weight"`
	ABIPath           string               `yaml:"abi_path"`
	BinPath           string               `yaml:"bin_path"`
	SourcePath        string               `yaml:"source_path"`
	StorageLayoutPath string               `yaml:"storage_layout_path,omitempty"`
	StorageLayout     []StorageMapping     `yaml:"storage_layout,omitempty"`
	Functions         []ContractFunction   `yaml:"functions"`
	Dependencies      []ContractDependency `yaml:"dependencies,omitempty"`
}

type ContractDependency struct {
	Contract  string `yaml:"contract"`
	InjectAs  string `yaml:"inject_as"`
	ParamName string `yaml:"param_name"`
}

type GlobalConfig struct {
	Seed     int64   `yaml:"seed"`
	AddrNum  uint64  `yaml:"addr_num"`
	ZipfSkew float64 `yaml:"zipf_skew"`
}

type Config struct {
	Global    GlobalConfig     `yaml:"global"`
	Contracts []ContractConfig `yaml:"contracts"`
}

type ContractFunctionPair struct {
	ContractName string
	FunctionName string
}
