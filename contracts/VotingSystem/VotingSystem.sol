pragma solidity >=0.4.24 <0.7.0;

contract VotingSystem {
    mapping(string => uint256) public voteWeight;        // 选民权重
    mapping(string => bool) public hasVoted;             // 是否已投
    mapping(string => uint256) public proposalVotes;    // 提案得票（热点）
    mapping(string => string) public delegateTo;         // 委托目标
    uint256 public totalVoters;                          // 总投票人（热点）

    function giveRightToVote(string memory arg0) public {
        uint256 w = voteWeight[arg0];
        if (w == 0) {
            voteWeight[arg0] = 1;
            uint256 cur = totalVoters;
            totalVoters = cur + 1;
        }
    }

    function vote(string memory arg0, string memory arg1) public {
        bool voted = hasVoted[arg0];
        if (!voted) {
            uint256 w = voteWeight[arg0];
            uint256 cur = proposalVotes[arg1];
            proposalVotes[arg1] = cur + w;
            hasVoted[arg0] = true;
        }
    }

    function delegate(string memory arg0, string memory arg1) public {
        bool voted = hasVoted[arg0];
        if (!voted) {
            uint256 w = voteWeight[arg0];
            delegateTo[arg0] = arg1;
            uint256 targetW = voteWeight[arg1];
            voteWeight[arg1] = targetW + w;
            hasVoted[arg0] = true;
        }
    }

    // 查询某提案得票（view）
    function winningProposal(string memory arg0) public view returns (uint256) {
        return proposalVotes[arg0];
    }
}
