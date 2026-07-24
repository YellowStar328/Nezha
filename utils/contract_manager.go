package utils

import (
	"Nezha/ethereum/go-ethereum/accounts/abi"
	"Nezha/ethereum/go-ethereum/common"
	"Nezha/evm/levm/tools"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"strconv"
	"sync"

	"golang.org/x/crypto/sha3"
	"gopkg.in/yaml.v3"
)

type ContractManager struct {
	config      *Config
	abiCache    map[string]abi.ABI
	sourceCode  map[string]string
	totalWeight int
	mu          sync.RWMutex
}

var contractManager *ContractManager

func InitContractManager(configPath string) error {
	contractManager = &ContractManager{
		abiCache:   make(map[string]abi.ABI),
		sourceCode: make(map[string]string),
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	contractManager.config = &config

	totalWeight := 0
	for _, contract := range config.Contracts {
		totalWeight += contract.Weight
		contractManager.loadContract(contract)
	}
	contractManager.totalWeight = totalWeight

	fmt.Printf("ContractManager initialized: %d contracts, total weight=%d\n", len(config.Contracts), totalWeight)
	return nil
}

func (cm *ContractManager) loadContract(contract ContractConfig) {
	abiObject, _, err := tools.LoadContract(contract.ABIPath, contract.BinPath)
	if err != nil {
		fmt.Printf("Warning: failed to load ABI for contract %s: %v\n", contract.Name, err)
		return
	}
	cm.abiCache[contract.Name] = abiObject

	sourceCode, err := os.ReadFile(contract.SourcePath)
	if err != nil {
		fmt.Printf("Warning: failed to read source code for contract %s: %v\n", contract.Name, err)
		return
	}
	cm.sourceCode[contract.Name] = string(sourceCode)
}

func GetContractManager() *ContractManager {
	return contractManager
}

func (cm *ContractManager) GetContractConfig(contractName string) (*ContractConfig, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	for i := range cm.config.Contracts {
		if cm.config.Contracts[i].Name == contractName {
			return &cm.config.Contracts[i], true
		}
	}
	return nil, false
}

func (cm *ContractManager) GetAllContractNames() []string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	names := make([]string, 0, len(cm.config.Contracts))
	for _, contract := range cm.config.Contracts {
		names = append(names, contract.Name)
	}
	return names
}

func (cm *ContractManager) GetABI(contractName string) (abi.ABI, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	abiObject, ok := cm.abiCache[contractName]
	if !ok {
		return abi.ABI{}, fmt.Errorf("ABI for contract %s not found", contractName)
	}
	return abiObject, nil
}

func (cm *ContractManager) GetSourceCode(contractName string) (string, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	source, ok := cm.sourceCode[contractName]
	if !ok {
		return "", fmt.Errorf("source code for contract %s not found", contractName)
	}
	return source, nil
}

func (cm *ContractManager) GetFunction(contractName, functionName string) (*ContractFunction, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	contract, ok := cm.GetContractConfig(contractName)
	if !ok {
		return nil, false
	}

	for i := range contract.Functions {
		if contract.Functions[i].Name == functionName {
			return &contract.Functions[i], true
		}
	}
	return nil, false
}

func (cm *ContractManager) GetAllFunctionNames(contractName string) []string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	contract, ok := cm.GetContractConfig(contractName)
	if !ok {
		return nil
	}

	names := make([]string, 0, len(contract.Functions))
	for _, fn := range contract.Functions {
		names = append(names, fn.Name)
	}
	return names
}

func (cm *ContractManager) GetAllFunctionsForPreAnalysis() []ContractFunctionPair {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var pairs []ContractFunctionPair
	for _, contract := range cm.config.Contracts {
		for _, fn := range contract.Functions {
			pairs = append(pairs, ContractFunctionPair{
				ContractName: contract.Name,
				FunctionName: fn.Name,
			})
		}
	}
	return pairs
}

func (cm *ContractManager) GetStorageKey(contractName, mappingName string, accountID uint64) ([]byte, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	contractConfig, ok := cm.GetContractConfig(contractName)
	if !ok {
		return nil, fmt.Errorf("contract %s not found", contractName)
	}

	var mappingDef *StorageMapping
	for i := range contractConfig.StorageLayout {
		if contractConfig.StorageLayout[i].MappingName == mappingName {
			mappingDef = &contractConfig.StorageLayout[i]
			break
		}
	}
	if mappingDef == nil {
		return nil, fmt.Errorf("mapping %s not found in contract %s", mappingName, contractName)
	}

	slotBytes := common.LeftPadBytes(
		big.NewInt(int64(mappingDef.Slot)).Bytes(),
		32,
	)

	var keyBytes []byte
	if mappingDef.KeyType == "string" {
		keyBytes = []byte(strconv.FormatUint(accountID, 10))
	} else {
		keyBytes = common.LeftPadBytes(big.NewInt(int64(accountID)).Bytes(), 32)
	}

	data := append(keyBytes, slotBytes...)
	h := sha3.NewLegacyKeccak256()
	h.Write(data)
	return h.Sum(nil), nil
}

func (cm *ContractManager) RandomSelectContract(r *rand.Rand) *ContractConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if len(cm.config.Contracts) == 0 {
		return nil
	}

	randVal := r.Intn(cm.totalWeight)
	cumulative := 0

	for i := range cm.config.Contracts {
		cumulative += cm.config.Contracts[i].Weight
		if randVal < cumulative {
			return &cm.config.Contracts[i]
		}
	}

	return &cm.config.Contracts[0]
}

func (cm *ContractManager) RandomSelectFunction(contractName string, r *rand.Rand) *ContractFunction {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	contract, ok := cm.GetContractConfig(contractName)
	if !ok || len(contract.Functions) == 0 {
		return nil
	}

	idx := r.Intn(len(contract.Functions))
	return &contract.Functions[idx]
}
