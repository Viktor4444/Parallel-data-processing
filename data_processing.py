import multiprocessing
import pickle
import queue
from time import sleep


class GeneratorData():
    def __init__(self, count_generated_data, data_queue):
        self.count_data = count_generated_data
        self.queue = data_queue

    def start_genertion(self, sleep_time=1):
        for i in range(self.count_data):
            sleep(sleep_time)
            self.queue.put(i)
            print(f'genrator: genrate {i}')
        print(f'genrator: done')

class ProcessorData():
    def __init__(self, data_queue, storage_file):
        self.queue = data_queue
        self.storage = storage_file

    def validate_data(self, data):
        if type(data) is int:
            return True
        else:
            return False

    def run_processing(self, lock):
        while True:
            try:
                data = self.queue.get(timeout=10.0)
                print(f'processor: {data}')
                is_valid_data = self.validate_data(data)

                if is_valid_data:
                    lock.acquire()
                    with open(self.storage, "rb") as file:
                        file_data = pickle.load(file)

                    file_data.append(data)

                    with open(self.storage, "wb") as file:
                        pickle.dump(file_data, file)
                    lock.release()

                sleep(0.3)
            except queue.Empty as e:
                print(f'processor Done')
                break


class SenderData():
    def __init__(self, storage_file):
        self.storage = storage_file

    def send_to_imaginary_server(self, data):
        pass

    def run_sending(self, lock):
        batch_num = 0
        count_cycles_without_data = 0
        while True:
            sleep(3)
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
                    print(f"sending {data}...")
                    self.send_to_imaginary_server(data)
                    sleep(1)
                    print(f"{data} sended!")

                print(f'data batch num {batch_num} sended')
                batch_num += 1
                count_cycles_without_data = 0
            else:
                lock.release()
                count_cycles_without_data += 1

            if count_cycles_without_data == 5:
                break
        print("Sender done")


def main():
    # отправить коммит с классами
    # добавить аргументы и их валидацию
    # добавить фэйкер
    # добавить доку

    storage_path = "./data.pickle"
    inital_lsit=[]
    with open(storage_path, "wb") as file:
        pickle.dump(inital_lsit, file)

    lock = multiprocessing.Lock()
    data_queue = multiprocessing.Queue()

    d_generator = GeneratorData(5, data_queue)
    d_processor = ProcessorData(data_queue, storage_path)
    d_sender = SenderData(storage_path)

    p1 = multiprocessing.Process(target=d_generator.start_genertion, args=(1,))
    p2 = multiprocessing.Process(target=d_processor.run_processing, args=(lock,))
    p3 = multiprocessing.Process(target=d_sender.run_sending, args=(lock,))

    p1.start()
    p2.start()
    p3.start()

    p1.join()
    p2.join()
    p3.join()


def test_data():
    with open("data.pickle", "rb") as file:
        data = pickle.load(file)
        print(data)


if __name__ == '__main__':
    # test_data()
    main()
