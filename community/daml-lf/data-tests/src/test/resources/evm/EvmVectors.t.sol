// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.20;

/// Minimal subset of the Foundry cheatcodes, so no forge-std dependency is needed.
interface Vm {
    function readLine(string calldata path) external view returns (string memory);
    function parseJsonString(string calldata json, string calldata key) external pure returns (string memory);
    function keyExistsJson(string calldata json, string calldata key) external view returns (bool);
    function parseUint(string calldata value) external pure returns (uint256);
    function envOr(string calldata name, string calldata defaultValue) external view returns (string memory);
    function toString(uint256 value) external pure returns (string memory);
    function toString(bytes32 value) external pure returns (string memory);
}

/// Checks every line of evm_vectors.jsonl against the real EVM.
///
/// Each line is `{"opcode": "...", "a": "...", "b": "...", "c": "...", "expected": "...", "note": "..."}`
/// where `b`, `c` and `note` are optional and values are decimal or 0x-prefixed hex strings.
/// Arguments are in EVM order: for SHL/SHR/SAR, `a` is the shift and `b` the value.
/// SLT/SGT expect 0 or 1.
contract EvmVectorsTest {
    Vm private constant vm = Vm(0x7109709ECfa91a80626fF3989D68f67F5b1DD12D);

    function test_vectors() public view {
        string memory path = vm.envOr("EVM_VECTORS", string("evm_vectors.jsonl"));

        uint256 lineNo = 0;
        uint256 failures = 0;
        string memory report = "";

        while (true) {
            string memory line = vm.readLine(path);
            if (bytes(line).length == 0) break; // EOF (the file must not contain blank lines)
            lineNo++;

            string memory op = vm.parseJsonString(line, ".opcode");
            uint256 a = _arg(line, ".a");
            uint256 b = _optArg(line, ".b");
            uint256 c = _optArg(line, ".c");
            uint256 expected = _arg(line, ".expected");

            uint256 actual = _eval(op, a, b, c);
            if (actual != expected) {
                failures++;
                report = string.concat(
                    report, "\n  line ", vm.toString(lineNo), " ", op,
                    ": expected ", vm.toString(bytes32(expected)), ", got ", vm.toString(bytes32(actual))
                );
            }
        }

        require(lineNo > 0, string.concat("no vectors read from ", path));
        require(
            failures == 0,
            string.concat(vm.toString(failures), " of ", vm.toString(lineNo), " vectors do not match the EVM:", report)
        );
    }

    function _arg(string memory line, string memory key) private pure returns (uint256) {
        return vm.parseUint(vm.parseJsonString(line, key));
    }

    function _optArg(string memory line, string memory key) private view returns (uint256) {
        return vm.keyExistsJson(line, key) ? _arg(line, key) : 0;
    }

    function _eval(string memory op, uint256 a, uint256 b, uint256 c) private pure returns (uint256 r) {
        bytes32 h = keccak256(bytes(op));

        if (h == keccak256("NOT_UINT256")) { assembly { r := not(a) } }

        else if (h == keccak256("ADD_UINT256")) { assembly { r := add(a, b) } }
        else if (h == keccak256("SUB_UINT256")) { assembly { r := sub(a, b) } }
        else if (h == keccak256("MUL_UINT256")) { assembly { r := mul(a, b) } }
        else if (h == keccak256("DIV_UINT256")) { assembly { r := div(a, b) } }
        else if (h == keccak256("SDIV_UINT256")) { assembly { r := sdiv(a, b) } }
        else if (h == keccak256("MOD_UINT256")) { assembly { r := mod(a, b) } }
        else if (h == keccak256("SMOD_UINT256")) { assembly { r := smod(a, b) } }
        else if (h == keccak256("EXP_UINT256")) { assembly { r := exp(a, b) } }
        else if (h == keccak256("SIGNEXTEND_UINT256")) { assembly { r := signextend(a, b) } }

        else if (h == keccak256("SLT_UINT256")) { assembly { r := slt(a, b) } }
        else if (h == keccak256("SGT_UINT256")) { assembly { r := sgt(a, b) } }

        else if (h == keccak256("AND_UINT256")) { assembly { r := and(a, b) } }
        else if (h == keccak256("OR_UINT256")) { assembly { r := or(a, b) } }
        else if (h == keccak256("XOR_UINT256")) { assembly { r := xor(a, b) } }
        else if (h == keccak256("BYTE_UINT256")) { assembly { r := byte(a, b) } }
        else if (h == keccak256("SHL_UINT256")) { assembly { r := shl(a, b) } }
        else if (h == keccak256("SHR_UINT256")) { assembly { r := shr(a, b) } }
        else if (h == keccak256("SAR_UINT256")) { assembly { r := sar(a, b) } }

        else if (h == keccak256("ADDMOD_UINT256")) { assembly { r := addmod(a, b, c) } }
        else if (h == keccak256("MULMOD_UINT256")) { assembly { r := mulmod(a, b, c) } }

        else revert(string.concat("unknown opcode: ", op));
    }
}

