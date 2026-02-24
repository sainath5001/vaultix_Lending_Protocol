// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/access/Ownable.sol";
import "@chainlink/contracts/src/v0.8/shared/interfaces/AggregatorV3Interface.sol";

contract PriceOracle is Ownable {
    constructor(address initialOwner) Ownable(initialOwner) {}

    mapping(address => address) public priceFeeds;

    function setPriceFeed(address asset, address feed) external onlyOwner {
        require(asset != address(0), "Invalid asset");
        require(feed != address(0), "Invalid feed");

        priceFeeds[asset] = feed;
    }

    function getPrice(address asset) external view returns (uint256) {
        address feed = priceFeeds[asset];
        require(feed != address(0), "Feed not set");

        AggregatorV3Interface priceFeed = AggregatorV3Interface(feed);

        (, int256 answer,,,) = priceFeed.latestRoundData();

        require(answer > 0, "Invalid price");

        uint8 decimals = priceFeed.decimals();

        return uint256(answer) * (10 ** (18 - decimals));
    }
}
