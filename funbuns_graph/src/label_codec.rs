//! BitSerializer/BitDeserializer for u8 edge labels (m values).
//!
//! This lets us carry m values through webgraph's SortPairs external sort.
//! Each label is gamma-coded (m values are small, typically 1-37).

use dsi_bitstream::codes::{GammaRead, GammaWrite};
use dsi_bitstream::prelude::*;
use webgraph::traits::{BitDeserializer, BitSerializer};
use webgraph::prelude::grouped_gaps::GroupedGapsCodec;

/// Serializer: writes u8 as gamma code.
#[derive(Clone, Debug, Default)]
pub struct U8Ser;

/// Deserializer: reads gamma code as u8.
#[derive(Clone, Debug, Default)]
pub struct U8Deser;

impl<E: Endianness, BW: BitWrite<E> + GammaWrite<E>> BitSerializer<E, BW> for U8Ser {
    type SerType = u8;
    #[inline]
    fn serialize(&self, value: &u8, bitstream: &mut BW) -> Result<usize, BW::Error> {
        bitstream.write_gamma(*value as u64)
    }
}

impl<E: Endianness, BR: BitRead<E> + GammaRead<E>> BitDeserializer<E, BR> for U8Deser {
    type DeserType = u8;
    #[inline]
    fn deserialize(&self, bitstream: &mut BR) -> Result<u8, BR::Error> {
        Ok(bitstream.read_gamma()? as u8)
    }
}

/// Batch codec type alias for SortPairs with u8 labels.
pub type U8BatchCodec = GroupedGapsCodec<NE, U8Ser, U8Deser>;

/// Create a U8BatchCodec instance.
pub fn u8_codec() -> U8BatchCodec {
    GroupedGapsCodec::new(U8Ser, U8Deser)
}
