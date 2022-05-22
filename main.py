import argparse
from kafka_process import KafkaStreamingProcess
from persist_formatted import FormatLoadProcess


def build_arg_parser():
    parser = argparse.ArgumentParser(description='Run format zone processes')
    parser.add_argument('--format-process', dest='format_process', action='store_true',
                        help='Run Format Zone pipeline process')
    parser.add_argument('--kafka-process', dest='kafka_process', action='store_true',
                        help='Run Kafka streaming process')
    args = parser.parse_args()
    return args

def main():
    args = build_arg_parser()
    if args.format_process:
        FormatLoadProcess().run_process()
    if args.kafka_process:
        KafkaStreamingProcess().run_process()
        

if __name__ == "__main__":
    main()