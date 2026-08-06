pragma solidity >=0.4.24 <0.7.0;

interface IUSDT {
    function transfer(string memory arg0, string memory arg1, uint256 _value) external returns (bool);
}

contract StakingPool {
    mapping(string => uint256) public stakedAmount;    // 用户质押额
    mapping(string => uint256) public rewardDebt;       // 用户已结算收益
    uint256 public totalStaked;                         // 总质押（热点）
    uint256 public rewardRate;                          // 全局利率（热点，admin更新）
    uint256 public accRewardPerShare;                   // 每份额累计收益（热点）
    IUSDT public usdt;                                  // 依赖 USDT（构造注入）

    constructor(address _usdtAddr) public {
        if (_usdtAddr != address(0)) {
            usdt = IUSDT(_usdtAddr);
        }
    }


    function stake(string memory arg0, string memory arg1, uint256 arg2) public {
        uint256 cur = stakedAmount[arg0];
        uint256 acc = accRewardPerShare;
        uint256 debt = cur * acc;
        rewardDebt[arg0] = debt;

        usdt.transfer(arg0, arg1, arg2);

        stakedAmount[arg0] = cur + arg2;
        uint256 ts = totalStaked;
        totalStaked = ts + arg2;
        rewardDebt[arg0] = (cur + arg2) * acc;
    }

    function unstake(string memory arg0, string memory arg1, uint256 arg2) public {
        uint256 cur = stakedAmount[arg0];
        if (cur >= arg2) {
            uint256 acc = accRewardPerShare;
            rewardDebt[arg0] = cur * acc;

            usdt.transfer(arg1, arg0, arg2);

            stakedAmount[arg0] = cur - arg2;
            uint256 ts = totalStaked;
            totalStaked = ts - arg2;
            rewardDebt[arg0] = (cur - arg2) * acc;
        }
    }

     function claimReward(string memory arg0, string memory arg1) public {
        uint256 cur = stakedAmount[arg0];
        uint256 acc = accRewardPerShare;
        uint256 debt = rewardDebt[arg0];
        uint256 pending = cur * acc - debt;

        if (pending > 0) {
            usdt.transfer(arg1, arg0, pending);
        }
        rewardDebt[arg0] = cur * acc;
    }


    function updateRewardRate(uint256 arg0) public {
        rewardRate = arg0;
        uint256 acc = accRewardPerShare;
        uint256 ts = totalStaked;
        if (ts > 0) {
            accRewardPerShare = acc + arg0;
        }
    }
}
