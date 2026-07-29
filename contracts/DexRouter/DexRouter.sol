// SPDX-License-Identifier: MIT
pragma solidity >=0.4.24 <0.7.0;

contract DexRouter {
        // 记录两个池子的地址（使用引擎传进来的 addr1 和 addr2）
    string public pool3;
    string public pool4;
    mapping(string => uint256) public poolLiquidity;
    mapping(string => uint256) public userBalances;



    function initPool(string memory _poolName, uint256 _liquidity) public {
        poolLiquidity[_poolName] = _liquidity;
        // 简单记录前两个初始化的池子名字
        if (bytes(pool3).length == 0) {
            pool3 = _poolName;
        } else {
            pool4 = _poolName;
        }
    }

    function initAccount(string memory _account, uint256 _balance) public {
        userBalances[_account] = _balance;
    }

    function swap(string memory _user, uint256 _amount) public returns (bool) {
        if (userBalances[_user] < _amount) {
            userBalances[_user] = 1000000; 
        }
        userBalances[_user] -= _amount;

        // 使用记录的动态池子名称
        uint256 liq1 = poolLiquidity[pool3];
        uint256 liq2 = poolLiquidity[pool4];

        if (liq1 > liq2) {
            poolLiquidity[pool3] += _amount;
        } else {
            poolLiquidity[pool4] += _amount;
        }

        return true;
    }
}