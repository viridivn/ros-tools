import argparse
import json
import sys
from pathlib import Path

import cv2
import numpy as np
from rosbags.rosbag1 import Writer
from rosbags.typesys import Stores, get_typestore

YELLOW = '\033[93m'
RED = '\033[91m'
CYAN = '\033[96m'
RESET = '\033[0m'

def spectacular_to_rosbag(video_path: Path, metadata_path: Path, output_path: Path) -> None:
    if not video_path.exists() or not metadata_path.exists():
        print(f"{RED}One or both input files do not exist.{RESET}", file=sys.stderr)
        sys.exit(1)

    if output_path.exists():
        print(f"{RED}Output file already exists.{RESET}", file=sys.stderr)
        sys.exit(1)

    print("Initialize ROS1 typestore (Noetic)")
    typestore = get_typestore(Stores.ROS1_NOETIC)

    Image = typestore.types['sensor_msgs/msg/Image']
    Imu = typestore.types['sensor_msgs/msg/Imu']
    MagneticField = typestore.types['sensor_msgs/msg/MagneticField']
    Header = typestore.types['std_msgs/msg/Header']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']
    Quaternion = typestore.types['geometry_msgs/msg/Quaternion']
    Time = typestore.types['builtin_interfaces/msg/Time']

    print("Load input data")
    with metadata_path.open('r') as f:
        metadata = f.readlines()

    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        print(f"{RED}Error opening video file.{RESET}", file=sys.stderr)
        sys.exit(1)

    try:
        with Writer(output_path) as writer:
            conn_img = writer.add_connection('/camera/image_raw', Image.__msgtype__, typestore=typestore)
            conn_imu = writer.add_connection('/imu/data', Imu.__msgtype__, typestore=typestore)
            conn_mag = writer.add_connection('/imu/mag', MagneticField.__msgtype__, typestore=typestore)

            seq_counters = {
                'image': 0,
                'imu': 0,
                'mag': 0,
                'total': 0
            }
            accel_cache = None

            for i, line in enumerate(metadata):
                if seq_counters['total'] % 1000 == 0 and seq_counters['total'] > 0:
                    print(f"Processed {seq_counters['total']} messages")
                try:
                    data = json.loads(line.strip())
                except json.JSONDecodeError:
                    print(f"{YELLOW}Skipping invalid JSON line {i+1}: {line.strip()}{RESET}", file=sys.stderr)
                    continue
                if "version" in data:
                    continue

                timestamp_float = data["time"]
                timestamp_ns = int(timestamp_float * 1e9)
                secs = int(timestamp_float)
                nsecs = int((timestamp_float - secs) * 1e9)
                ros_time = Time(sec=secs, nanosec=nsecs)

                if "frames" in data:
                    frame_idx = data["number"]

                    cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
                    ret, frame = cap.read()
                    if not ret:
                        print(f"{YELLOW}Skipping invalid frame {frame_idx} at line {i+1}{RESET}", file=sys.stderr)
                        continue

                    header = Header(
                        seq=seq_counters['image'],
                        stamp=ros_time,
                        frame_id='camera'
                    )
                    img_msg = Image(
                        header=header,
                        height=frame.shape[0],
                        width=frame.shape[1],
                        encoding='bgr8',
                        is_bigendian=0,
                        step=frame.strides[0],
                        data=frame.flatten()
                    )

                    raw = typestore.serialize_ros1(img_msg, Image.__msgtype__)
                    writer.write(conn_img, timestamp_ns, raw)
                    seq_counters['image'] += 1
                    seq_counters['total'] += 1

                elif "sensor" in data:
                    sensor = data["sensor"]
                    values= sensor["values"]

                    if sensor["type"] == "accelerometer":
                        accel_cache = {"time": timestamp_float, "values": values}
                    
                    elif sensor["type"] == "gyroscope":
                        if accel_cache and abs(accel_cache["time"] - timestamp_float) < 1e-9:
                            header = Header(
                                seq=seq_counters['imu'],
                                stamp=ros_time,
                                frame_id='imu_link'
                            )
                            accel = accel_cache["values"]
                            covariance = np.array([-1.0] + [0.0] * 8, dtype=np.float64)

                            imu_msg = Imu(
                                header=header,
                                orientation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
                                angular_velocity=Vector3(x=values[0], y=values[1], z=values[2]),
                                linear_acceleration=Vector3(x=accel[0], y=accel[1], z=accel[2]),
                                orientation_covariance=covariance,
                                angular_velocity_covariance=covariance,
                                linear_acceleration_covariance=covariance
                            )
                            raw = typestore.serialize_ros1(imu_msg, Imu.__msgtype__)
                            writer.write(conn_imu, timestamp_ns, raw)
                            seq_counters['imu'] += 1
                            seq_counters['total'] += 1
                            accel_cache = None 

                        else:
                            print(f"{YELLOW}Got gyro data without matching accel at line {i+1}{RESET}", file=sys.stderr)

                    elif sensor["type"] == "magnetometer":
                        header = Header(
                            seq=seq_counters['mag'],
                            stamp=ros_time,
                            frame_id='imu_link'
                        )
                        mag_msg = MagneticField(
                            header=header,
                            magnetic_field=Vector3(x=values[0], y=values[1], z=values[2]),
                            magnetic_field_covariance=np.array([-1.0] + [0.0] * 8, dtype=np.float64)
                        )

                        raw = typestore.serialize_ros1(mag_msg, MagneticField.__msgtype__)
                        writer.write(conn_mag, timestamp_ns, raw)
                        seq_counters['mag'] += 1
                        seq_counters['total'] += 1

    finally:
        cap.release()

        print(f"{CYAN}Finished processing {seq_counters['total']} messages{RESET}")
        print(f"{CYAN}Created output bag file: {output_path}{RESET}")

def main():
    parser = argparse.ArgumentParser(description="Convert Spectacular dataset to ROS1 bag format")
    parser.add_argument('video_path', type=Path, help="Path to the video file")
    parser.add_argument('metadata_path', type=Path, help="Path to the metadata JSONL file")
    parser.add_argument('output_path', type=Path, help="Path to save the output ROS1 bag file")

    args = parser.parse_args()

    spectacular_to_rosbag(args.video_path, args.metadata_path, args.output_path)

if __name__ == "__main__":
    main()