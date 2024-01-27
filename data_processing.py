import argparse
import multiprocessing
import pickle
import queue
from datetime import datetime
from time import sleep

from dateutil.relativedelta import relativedelta
from faker import Faker
from faker.providers import profile

Faker.seed(42)

class GeneratorData():
    def __init__(self, data_queue, count_generated_data):
        self.queue = data_queue
        self.count_data = count_generated_data

    def start_genertion(self, generation_is_ending, sleep_time=1):
        fake = Faker()
        for i in range(self.count_data):
            data = fake.profile()
            self.queue.put(data)
            print(f'genrator: genrate {data["name"]}')
            sleep(sleep_time)

        generation_is_ending.set()
        print(f'genrator: done')


class ProcessorData():
    def __init__(self, data_queue, storage_file):
        self.queue = data_queue
        self.storage = storage_file

    @staticmethod
    def validate_data(data):
        birthdate = data["birthdate"]
        current_age = relativedelta(datetime.now(), birthdate)
        if 30 <= current_age.years < 41:
            return True
        else:
            return False

    def run_processing(self, lock, generation_is_ending, sleep_time=0.3):
        while True:
            try:
                data = self.queue.get(timeout=30.0)
                print(f'processor: {data["name"]}')
                is_valid_data = self.validate_data(data)

                if is_valid_data:
                    lock.acquire()
                    with open(self.storage, "rb") as file:
                        file_data = pickle.load(file)

                    file_data.append(data)

                    with open(self.storage, "wb") as file:
                        pickle.dump(file_data, file)
                    lock.release()

                sleep(sleep_time)
            except queue.Empty as e:
                print(f'Queue if empty for too long.')

            if generation_is_ending.is_set():
                print(f'processor Done')
                break


class SenderData():
    def __init__(self, storage_file, server_address):
        self.storage = storage_file
        self.address = server_address

    def send_to_imaginary_server(self, data):
        self.address
        sleep(1)
        pass

    def run_sending(self, lock, generation_is_ending, sleep_time=3):
        batch_num = 0
        while True:
            sleep(sleep_time)
            lock.acquire()
            with open(self.storage, "rb") as file:
                file_data = pickle.load(file)
            if file_data:
                data_to_send = file_data.copy()
                file_data.clear()
                with open(self.storage, "wb") as file:
                    pickle.dump(file_data, file)
                lock.release()

                for data in data_to_send:
                    print(f"sending {data["name"]}...")
                    self.send_to_imaginary_server(data)
                    print(f"\n{data["name"]} sended!\n")

                print(f'data batch num {batch_num} sended')
                batch_num += 1
            else:
                lock.release()

            if generation_is_ending.is_set():
                print("Sender done")
                break


def parse_args() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Script that simulates parallel '
        'computing, processing, and sending data'
        )
    parser.add_argument(
        '-c', '--count',
        type=int,
        required=True,
        help='how much data needs to be generated'
        )
    parser.add_argument(
        '-s', '--storage',
        type=str,
        default="./data.pickle",
        help='path to file where the data is stored'
        )
    parser.add_argument(
        '-a', '--address',
        type=str,
        default="img.serv.com",
        help='address of the server to send'
        )
    # parser.add_argument(
    #   '--set_seed',
    #   action='store_true',
    #   help='Specify this argument if you want '
    #   'that the experiment was be repeatable'
    #   )
    return parser.parse_args()


def main():
    # добавить доку
    args = parse_args()

    try:
        inital_lsit=[]
        with open(args.storage, "wb") as file:
            pickle.dump(inital_lsit, file)
    except IOError as error:
        raise IOError(
            'Storage file was not found, please check path to storage.'
            )

    lock = multiprocessing.Lock()
    data_queue = multiprocessing.Queue()
    generation_is_ending = multiprocessing.Event()

    d_generator = GeneratorData(data_queue, args.count)
    d_processor = ProcessorData(data_queue, args.storage)
    d_sender = SenderData(args.storage, args.address)

    p_generator = multiprocessing.Process(
        target=d_generator.start_genertion,
        args=(generation_is_ending,)
        )
    p_processor = multiprocessing.Process(
        target=d_processor.run_processing,
        args=(lock, generation_is_ending,)
        )
    p_sender = multiprocessing.Process(
        target=d_sender.run_sending,
        args=(lock, generation_is_ending,)
        )

    p_generator.start()
    p_processor.start()
    p_sender.start()

    p_generator.join()
    p_processor.join()
    p_sender.join()


def test_data():
    with open("data.pickle", "rb") as file:
        data = pickle.load(file)
        print(data)


if __name__ == '__main__':
    # test_data()
    main()
