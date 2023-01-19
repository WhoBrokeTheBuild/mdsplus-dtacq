
import MDSplus
import numpy as np

_SECONDS_TO_NANOSECONDS = 1_000_000_000
"""One second in nanoseconds"""

# NOTE: Update when this value is changed globally
_TAI_TO_UTC_OFFSET_NANOSECONDS = 37 * _SECONDS_TO_NANOSECONDS
"""The offset needed to convert TAI timestamps to UTC and back"""

def _tai_to_utc(tai_timestamp):
    """Convert nanosecond TAI timestamp to UTC"""
    return tai_timestamp - _TAI_TO_UTC_OFFSET_NANOSECONDS

def _utc_to_tai(utc_timestamp):
    """Convert nanosecond UTC timestamp to TAI"""
    return utc_timestamp + _TAI_TO_UTC_OFFSET_NANOSECONDS

def _parse_spad_tai_timestamp(spad1, spad2, ns_per_tick):
    """Parse the nanosecond TAI timestamp stored in SPAD[1] and SPAD[2]"""

    tai_seconds = spad1
    tai_ticks = spad2 & 0x0FFFFFFF # The vernier
    tai_nanoseconds = (tai_ticks * ns_per_tick)

    # Calculate the TAI time in nanoseconds
    tai_timestamp = (tai_seconds * _SECONDS_TO_NANOSECONDS) + tai_nanoseconds
    return int(tai_timestamp)
    
def acq2106_parse_spad_timestamps(spad1, spad2, ns_per_tick):
    """Parse an array of nanosecond TAI timestamps stored in SPAD[1] and SPAD[2]"""
    
    spad1 = np.uint32(spad1.data())
    spad2 = np.uint32(spad2.data())
    if len(spad1) != len(spad2):
        return None
        
    ns_per_tick = ns_per_tick.data()
    
    timestamp_list = np.empty(len(spad1))
    for i in range(len(spad1)):
        timestamp_list[i] = _tai_to_utc(_parse_spad_tai_timestamp(spad1[i], spad2[i], ns_per_tick)) / _SECONDS_TO_NANOSECONDS
        
    return timestamp_list
