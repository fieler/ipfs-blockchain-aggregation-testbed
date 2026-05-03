// SPDX-License-Identifier: MIT
pragma solidity 0.8.19;

/**
 * @title ThesisTraceability
 * @dev This contract provides an optimized way to anchor data hashes on the blockchain,
 * typically for proving the existence and timestamp of data stored off-chain (e.g., in IPFS).
 * It uses bytes32 for identifiers and IPFS digests to save gas.
 */
contract ThesisTraceability {
    struct Record {
        // Store only the 32-byte digest of the IPFS multihash.
        // The client is responsible for handling the full CID encoding/decoding.
        // A common IPFS multihash is SHA2-256 (32 bytes) with code 0x12.
        bytes32 ipfsDigest;
        uint256 timestamp;
    }

    // Mapping from a document ID (as bytes32) to the data record.
    // The client should use keccak256(docIdString) to get the ID.
    mapping(bytes32 => Record) private registry;

    event DataAnchored(
        bytes32 indexed docId,
        bytes32 indexed ipfsDigest,
        uint256 timestamp
    );

    /**
     * @dev Anchors a data record on the blockchain.
     * Throws if a record for the given docId already exists.
     * @param _docId The keccak256 hash of the unique document identifier.
     * @param _ipfsDigest The 32-byte digest part of the IPFS multihash.
     */
    function anchorData(bytes32 _docId, bytes32 _ipfsDigest) public {
        // Prevent overwriting existing records to ensure immutability.
        require(registry[_docId].timestamp == 0, "Record already exists");
        // Ensure the provided IPFS digest is not empty.
        require(_ipfsDigest != bytes32(0), "IPFS digest cannot be empty");

        registry[_docId] = Record(_ipfsDigest, block.timestamp);

        emit DataAnchored(_docId, _ipfsDigest, block.timestamp);
    }

    /**
     * @dev Retrieves the anchored data record for a given document ID.
     * @param _docId The keccak256 hash of the unique document identifier.
     * @return ipfsDigest The 32-byte digest of the IPFS multihash.
     * @return timestamp The Unix timestamp when the data was anchored.
     */
    function getRecord(bytes32 _docId) public view returns (bytes32 ipfsDigest, uint256 timestamp) {
        Record memory rec = registry[_docId];
        // A non-existent record will return (0x0, 0). The client should check the timestamp.
        return (rec.ipfsDigest, rec.timestamp);
    }
}