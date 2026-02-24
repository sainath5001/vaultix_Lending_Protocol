// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract InterestRateModel {
    uint256 public constant BASE_RATE = 2e16; // 2%
    uint256 public constant SLOPE1 = 1e17; // 10%
    uint256 public constant SLOPE2 = 3e17; // 30%
    uint256 public constant OPTIMAL_UTIL = 8e17; // 80%

    function getBorrowRate(uint256 totalSupply, uint256 totalBorrow) external pure returns (uint256) {
        if (totalSupply == 0) {
            return BASE_RATE;
        }

        uint256 utilization = (totalBorrow * 1e18) / totalSupply;

        if (utilization <= OPTIMAL_UTIL) {
            return BASE_RATE + (SLOPE1 * utilization) / 1e18;
        } else {
            uint256 excess = utilization - OPTIMAL_UTIL;

            return BASE_RATE + (SLOPE1 * OPTIMAL_UTIL) / 1e18 + (SLOPE2 * excess) / 1e18;
        }
    }
}
