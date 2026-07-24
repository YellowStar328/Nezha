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
	Name          string            `yaml:"name"`
	Weight        int               `yaml:"weight"`
	ABIPath       string            `yaml:"abi_path"`
	BinPath       string            `yaml:"bin_path"`
	SourcePath    string            `yaml:"source_path"`
	StorageLayout []StorageMapping  `yaml:"storage_layout"`
	Functions     []ContractFunction `yaml:"functions"`
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