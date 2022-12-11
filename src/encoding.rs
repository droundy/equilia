const U16_CODE: u8 = 253;
const U32_CODE: u8 = 254;
const U64_CODE: u8 = 255;

/// Write a `u64` in as few bytes as possible.
pub fn write_u64(buf: &mut Vec<u8>, v: u64) {
    if v < U16_CODE as u64 {
        buf.push(v as u8);
    } else if v < u16::MAX as u64 {
        buf.push(U16_CODE);
        buf.extend((v as u16).to_be_bytes());
    } else if v < u32::MAX as u64 {
        buf.push(U32_CODE);
        buf.extend((v as u32).to_be_bytes());
    } else {
        buf.push(U64_CODE);
        buf.extend(v.to_be_bytes());
    }
}

/// Read a `u64` that was written with [`write_u64`].
pub fn read_u64(mut buf: &[u8]) -> Result<(u64, &[u8]), ()> {
    let b = buf.first().copied().ok_or(())?;
    buf = &buf[1..];
    match b {
        U16_CODE => {
            let (array, rest) = try_split::<2>(buf).ok_or(())?;
            Ok((u16::from_be_bytes(array) as u64, rest))
        }
        U32_CODE => {
            let (array, rest) = try_split::<4>(buf).ok_or(())?;
            Ok((u32::from_be_bytes(array) as u64, rest))
        }
        U64_CODE => {
            let (array, rest) = try_split::<8>(buf).ok_or(())?;
            Ok((u64::from_be_bytes(array), rest))
        }
        _ => Ok((b as u64, buf)),
    }
}

fn try_split<const N: usize>(buf: &[u8]) -> Option<([u8; N], &[u8])> {
    if buf.len() < N {
        None
    } else {
        Some((buf[..N].try_into().unwrap(), &buf[N..]))
    }
}
