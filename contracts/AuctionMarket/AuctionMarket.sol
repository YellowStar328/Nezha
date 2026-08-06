pragma solidity >=0.4.24 <0.7.0;

// 拍卖市场合约 —— 测试热槽位争用（highestBid/highestBidder 单点写冲突）
contract AuctionMarket {
    mapping(string => uint256) public bids;     // 各用户出价
    string public highestBidder;                // 当前最高出价者（热槽位）
    uint256 public highestBid;                  // 当前最高出价（热槽位）
    bool public ended;                          // 拍卖结束标志

    // 用户出价：读取最高价，仅当更高时才写入热槽位 —— 条件写
    function bid(string memory arg0, uint256 arg1) public {
        uint256 cur = highestBid;
        uint256 amount = arg1;

        bids[arg0] = amount;
        if (amount > cur) {
            highestBidder = arg0;
            highestBid = amount;
        }
    }

    // 结算：读取 ended 守卫，写入 ended
    function settle() public {
        bool e = ended;
        if (!e) {
            ended = true;
        }
    }

    // 取消出价：读取并清零个人出价
    function cancelBid(string memory arg0) public {
        uint256 b = bids[arg0];
        if (b > 0) {
            bids[arg0] = 0;
        }
    }

    // 查询当前最高出价（view）
    function getHighest() public view returns (uint256) {
        return highestBid;
    }
}
