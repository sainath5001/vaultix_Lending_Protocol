// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/token/ERC20/ERC20.sol";

contract VaultToken is ERC20 {
    address public lendingPool;

    modifier onlyLendingPool() {
        require(msg.sender == lendingPool, "Not LendingPool");
        _;
    }

    constructor(string memory name_, string memory symbol_, address lendingPool_) ERC20(name_, symbol_) {
        require(lendingPool_ != address(0), "Invalid pool");
        lendingPool = lendingPool_;
    }

    function mint(address to, uint256 amount) external onlyLendingPool {
        _mint(to, amount);
    }

    function burn(address from, uint256 amount) external onlyLendingPool {
        _burn(from, amount);
    }
}
