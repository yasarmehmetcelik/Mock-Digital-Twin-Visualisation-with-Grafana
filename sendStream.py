import argparse
import csv
import json
import sys
import time
from dateutil.parser import parse
from confluent_kafka import Producer
import socket

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('filename', type=str,
                        help='Time series csv file.')
    parser.add_argument('topic', type=str,
                        help='Name of the Kafka topic to stream.')
    parser.add_argument('--speed', type=float, default=1, required=False,
                        help='Speed up time series by a given multiplicative factor.')
    args = parser.parse_args()

    topic = args.topic
    p_key = args.filename

    conf = {'bootstrap.servers': "localhost:19091",
            'client.id': socket.gethostname(),
            'default.topic.config': {'api.version.request': True}}
    
    producer = Producer(conf)

    rdr = csv.reader(open(args.filename))
    next(rdr)  # Skip header
    firstline = True
    previous_timestamp = None  # Initialize previous_timestamp variable

    while True:
        try:
            line = next(rdr, None)
            if line is None:  # If there are no more lines
                break

            # Convert timestamp to UNIX format
            timestamp_unix = int(parse(line[0]).timestamp() * 1000)

            # The rest of your line processing code...
            x1, y1, nn_pred_xr, nn_pred_yr, svr_pred_xr, svr_pred_yr, zero = float(line[1]), float(line[2]), float(line[3]), float(line[4]), float(line[5]), float(line[6]), int(line[7])
            
            result = {
                "timestamp": timestamp_unix,
                "x1": x1,
                "y1": y1,
                "nn_pred_xr": nn_pred_xr,
                "nn_pred_yr": nn_pred_yr,
                "svr_pred_xr": svr_pred_xr,
                "svr_pred_yr": svr_pred_yr,
                "zero1":zero
            }

            jresult = json.dumps(result)

            # For the first line, there's no need to wait; for subsequent lines, calculate the wait time based on the timestamp difference
            if not firstline:
                # Now using UNIX timestamp, need to convert previous_timestamp back to a datetime for comparison
                d1 = previous_timestamp_unix
                d2 = timestamp_unix
                diff = (d2 - d1) / args.speed
                time.sleep(diff)

            previous_timestamp_unix = timestamp_unix  # Update previous_timestamp for the next iteration
            firstline = False

            producer.produce(topic, key=p_key, value=jresult, callback=acked)
            producer.flush()

        except StopIteration:  # Catch the StopIteration if next(rdr) returns None
            break
        except Exception as e:
            print(f"An error occurred: {e}")
            break

if __name__ == "__main__":
    main()


