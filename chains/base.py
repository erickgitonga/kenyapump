"""
chains/base.py

Base chain adapter. Base is an OP-Stack L2 with EVM semantics identical
to Ethereum — so we inherit everything from EthereumAdapter and only
override chain identity and chain-specific constants.

Primary DEX: Aerodrome Slipstream (Uniswap V3 fork).
PoolCreated signature: (address,address,bool,address,uint256)
  — note `bool stable`, NOT `uint24 fee` like mainnet Uniswap V3.
"""

from __future__ import annotations

import logging

from chains.ethereum import EthereumAdapter
from config.settings import BaseConfig
from core.models import ChainId

log = logging.getLogger("kenyapump.chains.base")


class BaseAdapter(EthereumAdapter):
    """
    Base chain adapter. All EVM read/write logic is inherited from
    EthereumAdapter — only chain identity and config differ.
    """

    chain_id = ChainId.BASE

    # Aerodrome Slipstream pool factory on Base
    AERODROME_SLIPSTREAM_FACTORY = "0x420DD381b31aEf6683db6B902084cB0FFECe40Da"

    def __init__(self, config: BaseConfig):
        super().__init__(config)
        log.info("BaseAdapter initialized (chain_id=%s)", self.chain_id.value)
