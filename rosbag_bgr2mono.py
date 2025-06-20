import argparse
from pathlib import Path

import numpy as np

from rosbags.rosbag1 import Reader, Writer
from rosbags.typesys import Stores, get_typestore, get_types_from_msg

def bgr8_to_mono8(src: Path, dst: Path):
    typestore = get_typestore(Stores.ROS1_NOETIC)

    with Reader(src) as reader, Writer(dst) as writer:
        types = {}
        typecount = 0
        for conn in reader.connections:
            typecount += 1
            types.update(get_types_from_msg(conn.msgdef.data, conn.msgtype))
        typestore.register(types)
        print(f"Enumerated {len(types)} types from {typecount} connections")
        conn_map = {
            conn.id: writer.add_connection(
                conn.topic,
                conn.msgtype,
                typestore=typestore,
                md5sum=conn.digest,
                msgdef=conn.msgdef.data,
                callerid=conn.ext.callerid,
                latching=conn.ext.latching,
            )
            for conn in reader.connections
        }

        msgcount = 0
        for conn, timestamp, rawdata in reader.messages():
            if msgcount % 1000 == 0:
                print(f"Processed {msgcount} messages")
            if conn.msgtype != 'sensor_msgs/msg/Image':
                writer.write(conn_map[conn.id], timestamp, rawdata)
                msgcount += 1
                continue

            msg = typestore.deserialize_ros1(rawdata, conn.msgtype)

            bgr_image = msg.data.reshape((msg.height, msg.width, 3))

            mono_image = (
                bgr_image[:, :, 0] * 0.299 +
                bgr_image[:, :, 1] * 0.587 +
                bgr_image[:, :, 2] * 0.114
            ).astype(np.uint8)

            msg.encoding = 'mono8'
            msg.step = msg.width
            msg.data = mono_image.flatten()

            writer.write(
                conn_map[conn.id], 
                timestamp, 
                typestore.serialize_ros1(msg, conn.msgtype)
            )
            msgcount += 1


def main():
    parser = argparse.ArgumentParser(description="Convert BGR8 images in a ROS1 bag to MONO8.")
    parser.add_argument('src', type=Path, help="Source ROS1 bag file")
    parser.add_argument('dst', type=Path, help="Destination ROS1 bag file")
    args = parser.parse_args()

    if not args.src.exists():
        print(f"Source file {args.src} does not exist.")
        return

    if args.dst.exists():
        print(f"Destination file {args.dst} already exists.")
        return

    bgr8_to_mono8(args.src, args.dst)
    print(f"Converted {args.src} to {args.dst} with MONO8 images.")

if __name__ == "__main__":
    main()