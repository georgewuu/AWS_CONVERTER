import logging
from struct import unpack, error
from . import packet_map, analytics_map
from io import BytesIO

logger = logging.getLogger(__name__)


def get_device_and_segment(val):
    return (int(val) >> 8, int(val) & 0xFF)


def decode_packet(stream):
    string = stream.read(2)
    if len(string) < 1:
        return 0, None, None, None, None, (None, None), string

    packet_id = unpack("<H", string)[0]  # decode the packet id
    packet_def = packet_map[packet_id]

    sn = None
    tm = None
    device = None
    segment = None
    content = None
    packet_string = string

    if packet_def["size"] > 0:  # this packet has a fixed size
        string = stream.read(packet_def["size"] - 2)
        packet_string += string
        if "struct" in packet_def.keys():
            # this packets content can be decoded
            try:
                content = unpack(packet_def["struct"], string)
            except error as e:
                logger.exception(f"error decoding: {packet_id}")
                raise e
            device, segment = get_device_and_segment(content[0])
            sn = content[1]
            tm = content[2] if packet_id == 4 else None

    elif packet_id == 42:  # log packet
        string = stream.read(packet_def["header"]["size"])
        packet_string += string
        header = unpack(packet_def["header"]["struct"], string)
        device, segment = get_device_and_segment(header[1])
        tm = header[2]
        string = stream.read(header[4])
        packet_string += string
        data = string.decode("utf-8").strip()
        content = (header, data)

    elif packet_id == 257:  # analytics data
        string = stream.read(2)
        packet_string += string
        if len(string) < 1:
            logger.error("Packet type 257: too short to read total packet size")
            return 0, None, None, None, (None, None), string
        packet_size = unpack("<H", string)[0]
        if packet_size < 20:
            logger.error(
                "Packet type 257: given packet size == {} too short".format(packet_size)
            )
            string = stream.read(packet_size - 4)
            packet_string += string
            return (
                257,
                None,
                None,
                None,
                None,
                ((packet_size, None, None, None, None), None),
                string,
            )
        string = stream.read(packet_def["header"]["size"])
        packet_string += string
        header = unpack(packet_def["header"]["struct"], string)
        device, segment = get_device_and_segment(header[0])
        data_size = packet_size - packet_def["header"]["size"] - 4
        tm = header[3]
        data = None
        if header[2] in analytics_map.keys():
            log_def = analytics_map[header[2]]
            if data_size == log_def["size"]:
                string = stream.read(log_def["size"])
                packet_string += string
                data = unpack(log_def["struct"], string)
        if data is None:
            pass
        tmp = [packet_size]
        tmp.extend(header)
        content = (tuple(tmp), data)

    elif "waveform" in packet_def.keys() and packet_def["waveform"]["version"] == 2:
        string = stream.read(13)
        packet_string += string
        header = unpack("<H2IHB", string)
        device, segment = get_device_and_segment(header[1])
        sn = header[2]
        string = stream.read(header[0] - 15)
        packet_string += string
        structdef = packet_def["struct"].format(numsamp=header[4])
        try:
            data = unpack(structdef, string)
        except:  # noqa E722
            logger.exception(
                "Error unpacking waveform data for {}".format(packet_def["array"])
            )
            data = []
        content = (header, data)

    elif packet_id == 184:  # ALARM_STATE_DATA2_TYPE (variable length packet)
        string = stream.read(packet_def["header"]["size"])
        packet_string += string
        header = unpack(packet_def["header"]["struct"], string)
        device, segment = get_device_and_segment(header[1])
        sn = header[2]
        elements = []
        for k in range(header[6]):
            string = stream.read(8)
            packet_string += string
            elements.append(unpack("<H2BI", string))
        content = (header, elements)

    elif packet_id == 186:  # ALARM_LIMITS2_TYPE (variable length packet)
        string = stream.read(packet_def["header"]["size"])
        packet_string += string
        header = unpack(packet_def["header"]["struct"], string)
        device, segment = get_device_and_segment(header[1])
        sn = header[2]
        elements = []
        for k in range(header[3]):
            string = stream.read(4)
            packet_string += string
            elements.append(unpack("<HH", string))
        content = (header, elements)

    return (
        packet_id,
        sn,
        tm,
        device,
        segment if packet_id < 3000 else None,
        content,
        packet_string,
    )


def decode_packet_from_raw(raw):
    with BytesIO(raw) as stream_:
        vals = decode_packet(stream_)
    return vals


def spool_packets(stream):
    packet_id = 1
    while packet_id:
        packet_id, sn, tm, device, segment, content, packet_string = decode_packet(
            stream
        )
        if packet_id > 0:
            yield packet_id, sn, tm, device, segment, content, packet_string
