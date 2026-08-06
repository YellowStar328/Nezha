pragma solidity >=0.4.24 <0.7.0;

interface IUSDT {
    function transfer(string memory arg0, string memory arg1, uint256 _value) external returns (bool);
}

// 托管支付合约 —— 测试状态机互斥 + 跨合约调用 USDT
// 订单按 buyer 键，状态: 0=Created 1=Funded 2=Released 3=Refunded
contract Escrow {
    mapping(string => uint256) public orderState;    // 订单状态机
    mapping(string => string) public orderSeller;    // 卖方
    mapping(string => uint256) public orderAmount;   // 订单金额
    IUSDT public usdt;                               // 依赖 USDT（构造注入）

    constructor(address _usdtAddr) public {
        if (_usdtAddr != address(0)) {
            usdt = IUSDT(_usdtAddr);
        }
    }

    // 创建订单：arg0=buyer, arg1=seller, arg2=amount
    function createOrder(string memory arg0, string memory arg1, uint256 arg2) public {
        orderState[arg0] = 0;
        orderSeller[arg0] = arg1;
        orderAmount[arg0] = arg2;
    }

    // 付款：require Created，跨合约 buyer->seller，状态 -> Funded
    function deposit(string memory arg0) public {
        uint256 s = orderState[arg0];
        if (s == 0) {
            string memory seller = orderSeller[arg0];
            uint256 amount = orderAmount[arg0];
            usdt.transfer(arg0, seller, amount);
            orderState[arg0] = 1;
        }
    }

    // 确认放款：require Funded，状态 -> Released（与 refund 互斥）
    function release(string memory arg0) public {
        uint256 s = orderState[arg0];
        if (s == 1) {
            orderState[arg0] = 2;
        }
    }

    // 退款：require Funded，跨合约 seller->buyer，状态 -> Refunded（与 release 互斥）
    function refund(string memory arg0) public {
        uint256 s = orderState[arg0];
        if (s == 1) {
            string memory seller = orderSeller[arg0];
            uint256 amount = orderAmount[arg0];
            usdt.transfer(seller, arg0, amount);
            orderState[arg0] = 3;
        }
    }

    // 争议强制退款：读取状态，写 Refunded
    function dispute(string memory arg0) public {
        uint256 s = orderState[arg0];
        if (s != 3) {
            orderState[arg0] = 3;
        }
    }
}
