pragma solidity >=0.4.0 <0.7.0;

contract USDT {
    mapping(string => uint256) public balancesStore;
    mapping(string => bool) public isBlackListedStore; // 黑名单列表

    uint256 public feeRate; // 简化为百分比或基点，0表示无手续费

    function setAccount(string memory arg0, uint256 _value) public {
        balances[arg0] = _value;
    }

    // 写入操作：添加黑名单（极低频，用于测试读写冲突）
    function addBlackList(string memory _user) public {

        isBlackListed[_user] = true;
    }

    // 核心并发测试函数
    function transfer(string memory arg0, string memory arg1, uint256 _value) public returns (bool) {
        // 1. 读操作：并发友好的黑名单检查
        require(!isBlackListed[arg0]);

        //require(balances[arg0] >= _value);
        //require(balances[arg1] + _value >= balances[arg1]); 

        // 2. 判断是否包含热点写入
        if (feeRate > 0) {
            uint256 fee = (_value * feeRate) / 10000;
            uint256 sendAmount = _value - fee;
            
            balances[arg0] -= _value;
            balances[arg1] += sendAmount;
            
        } else {
            // 无冲突模式
            balances[arg0] -= _value;
            balances[arg1] += _value;
        }

        return true;
    }
}