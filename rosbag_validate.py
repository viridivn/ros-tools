from pathlib import Path
from rosbags.highlevel import AnyReader

def validate_rosbag(bag_path: Path) -> bool:
    msg_count = 0
    msg_types = set()
    try:
        with AnyReader([bag_path]) as reader:
            for connection, timestamp, rawdata in reader.messages():
                msg = reader.deserialize(rawdata, connection.msgtype)
                msg_count += 1
                msg_types.add(msg.__class__.__name__)
                if msg_count % 1000 == 0:
                    print(f"Read {msg_count} messages")
            print(f"Bag file {bag_path} is valid.")
            print(f"Total messages: {msg_count}")
            print(f"Message types: {', '.join(msg_types)}")
            return True
    except Exception as e:
        print(f"Bag file {bag_path} is invalid: {e}")
        return False

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python rosbag_validate.py <path_to_bag_file>")
        sys.exit(1)

    bag_path = Path(sys.argv[1])
    if not bag_path.exists():
        print(f"Bag file {bag_path} does not exist.")
        sys.exit(1)

    validate_rosbag(bag_path)