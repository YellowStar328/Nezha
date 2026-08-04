// SPDX-License-Identifier: MIT
pragma solidity >=0.4.24 <0.7.0;

interface IUSDT {
    function transfer(string memory arg0, string memory arg1, uint256 _value) external returns (bool);
}

contract DexRouter {
    string public pool3;
    string public pool4;
    mapping(string => uint256) public poolLiquidity;
    mapping(string => uint256) public userBalances;

    IUSDT public usdt;

    constructor(address _usdtAddr) public {
        if (_usdtAddr != address(0)) {
            usdt = IUSDT(_usdtAddr);
        }
    }

    function initPool(string memory _poolName, uint256 _liquidity) public {
        poolLiquidity[_poolName] = _liquidity;
        if (bytes(pool3).length == 0) {
            pool3 = _poolName;
        } else {
            pool4 = _poolName;
        }
    }

    function initAccount(string memory _account, uint256 _balance) public {
        userBalances[_account] = _balance;
    }

    function swap(string memory _user,string memory _user2, uint256 _amount) public returns (bool) {
        if (userBalances[_user] < _amount) {
            userBalances[_user] = 1000000;
        }
        userBalances[_user] -= _amount;
        userBalances[_user2] += _amount;
        uint256 liq1 = poolLiquidity[pool3];
        uint256 liq2 = poolLiquidity[pool4];

        if (liq1 > liq2) {
            poolLiquidity[pool3] += _amount;
        } else {
            poolLiquidity[pool4] += _amount;
        }
        usdt.transfer(_user, _user2, _amount);
        

        return true;
    }
}
