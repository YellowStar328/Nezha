pragma solidity >=0.4.24 <0.7.0;

// 库存管理合约 —— 测试所有权串行化 + 授权模式 + 条件买
contract InventoryManager {
    mapping(string => string) public ownerOf;       // 物品所有者
    mapping(string => string) public approved;      // 授权地址
    mapping(string => uint256) public stockLevel;   // 库存量
    mapping(string => uint256) public price;        // 单价
    uint256 public totalItems;                      // 物品总数（热点）

    // 铸造：arg0=item, arg1=owner, arg2=price
    function mint(string memory arg0, string memory arg1, uint256 arg2) public {
        ownerOf[arg0] = arg1;
        price[arg0] = arg2;
        stockLevel[arg0] = 1;
        uint256 cur = totalItems;
        totalItems = cur + 1;
    }

    // 所有权转移：arg0=item, arg1=to
    // 读 ownerOf（当前 owner），写 ownerOf，清 approved
    function transfer(string memory arg0, string memory arg1) public {
        string memory cur = ownerOf[arg0];
        if (keccak256(abi.encodePacked(cur)) != keccak256(abi.encodePacked(""))) {
            ownerOf[arg0] = arg1;
            approved[arg0] = "";
        }
    }

    // 授权：arg0=item, arg1=spender
    // 读 ownerOf，写 approved
    function approve(string memory arg0, string memory arg1) public {
        string memory cur = ownerOf[arg0];
        if (keccak256(abi.encodePacked(cur)) != keccak256(abi.encodePacked(""))) {
            approved[arg0] = arg1;
        }
    }

    // 购买：arg0=item, arg1=buyer, arg2=payAmount
    // 读 stockLevel/price，条件判断 pay>=price，写 stockLevel/ownerOf
    function buy(string memory arg0, string memory arg1, uint256 arg2) public {
        uint256 stock = stockLevel[arg0];
        uint256 p = price[arg0];
        if (stock > 0 && arg2 >= p) {
            stockLevel[arg0] = stock - 1;
            ownerOf[arg0] = arg1;
            approved[arg0] = "";
        }
    }

    // 销毁：arg0=item，读 ownerOf，写空，totalItems--
    function burn(string memory arg0) public {
        string memory cur = ownerOf[arg0];
        if (keccak256(abi.encodePacked(cur)) != keccak256(abi.encodePacked(""))) {
            ownerOf[arg0] = "";
            approved[arg0] = "";
            uint256 t = totalItems;
            totalItems = t - 1;
        }
    }
}
